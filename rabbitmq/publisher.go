// Copyright 2023 Kapeta Inc.
// SPDX-License-Identifier: MIT
package rabbitmq

import (
	"encoding/json"
	"fmt"
	"github.com/kapetacom/sdk-go-config/providers"
	rmq "github.com/wagslane/go-rabbitmq"
	"time"
)

type PublishOptions struct {
	// Mandatory fails to publish if there are no queues
	// bound to the routing key
	Mandatory bool
	// Immediate fails to publish if there are no consumers
	// that can ack bound to the queue on the routing key
	Immediate bool
	// Transient (0 or 1) or Persistent (2)
	DeliveryMode uint8
	// Expiration time in ms that a message will expire from a queue.
	// See https://www.rabbitmq.com/ttl.html#per-message-ttl-in-publishers
	Expiration string
	// 0 to 9
	Priority uint8
	// correlation identifier
	CorrelationID string
	// message identifier
	MessageID string
	// message timestamp
	Timestamp time.Time
	// message type name
	Type string
	// creating user id - ex: "guest"
	UserID string
}

type PublisherPayload[DataType any, Headers map[string]any, RoutingKey string] struct {
	Data       DataType        `json:"data"`
	Headers    Headers         `json:"headers"`
	RoutingKey RoutingKey      `json:"routingKey"`
	Options    *PublishOptions `json:"options"`
}

type PublisherOptions struct {
	Confirm bool
}

func CreatePublisher[DataType any, Headers map[string]any, RoutingKey string](config providers.ConfigProvider, resourceName string) (*Publisher[DataType, Headers, RoutingKey], error) {
	return CreatePublisherWithOptions[DataType, Headers, RoutingKey](config, resourceName, PublisherOptions{})
}

func CreatePublisherWithOptions[DataType any, Headers map[string]any, RoutingKey string](
	config providers.ConfigProvider,
	resourceName string, publishOptions PublisherOptions) (*Publisher[DataType, Headers, RoutingKey], error) {

	instances, err := config.GetInstancesForProvider(resourceName)
	if err != nil {
		return nil, fmt.Errorf("error getting instances for provider: %v", err)
	}

	if len(instances) == 0 {
		return nil, fmt.Errorf("no instances found for provider: %s", resourceName)
	}

	connections := map[string]*rmq.Conn{}
	publishers := make([]rmq.Publisher, 0)

	for _, instance := range instances {
		blockSpec, err := toBlockSpec(instance)
		if err != nil {
			return nil, fmt.Errorf("error decoding block spec: %v", err)
		}

		if connections[instance.InstanceId] == nil {
			conn, err := ConnectToInstance(config, instance.InstanceId)
			if err != nil {
				return nil, fmt.Errorf("error connecting to instance: %v", err)
			}
			connections[instance.InstanceId] = conn
		}
		conn := connections[instance.InstanceId]

		exchangeDefinitions := make([]ExchangeResource, 0)

		for _, connection := range instance.Connections {
			for _, exchange := range blockSpec.Consumers {
				if exchange.Metadata.Name == connection.Consumer.ResourceName &&
					connection.Provider.ResourceName == resourceName {
					exchangeDefinitions = append(exchangeDefinitions, exchange)
					break
				}
			}
		}

		if len(exchangeDefinitions) == 0 {
			return nil, fmt.Errorf("no exchange definitions found for provider %s", resourceName)
		}

		for _, exchangeDefinition := range exchangeDefinitions {
			exchange := asExchange(&exchangeDefinition)

			bindings, exchanges, err := resolveBindings(blockSpec, rmq.BindingTypeExchange, exchange.Name)

			if err != nil {
				return nil, fmt.Errorf("error resolving bindings: %v", err)
			}

			exchanges = append(exchanges, &exchange)

			exchangeName := exchange.Name

			publisher, err := rmq.NewPublisher(
				conn,
				rmq.WithPublisherOptionsLogging,
				rmq.WithPublisherOptionsConfirmMode(publishOptions.Confirm),
				rmq.WithPublisherOptionsExchangeName(exchangeName),
				rmq.WithPublisherExchanges(dereferenceSlice(exchanges)),
				rmq.WithPublisherBindings(dereferenceSlice(bindings)),
			)
			if err != nil {
				return nil, fmt.Errorf("error creating publisher: %v", err)
			}

			publishers = append(publishers, *publisher)
		}
	}

	return &Publisher[DataType, Headers, RoutingKey]{
		appId:      config.GetInstanceId() + "_" + resourceName,
		publishers: publishers,
	}, nil
}

type Publisher[DataType any, Headers map[string]any, RoutingKey string] struct {
	appId      string
	publishers []rmq.Publisher
}

func (p *Publisher[DataType, Headers, RoutingKey]) Publish(payload PublisherPayload[DataType, Headers, RoutingKey]) error {
	jsonPayload, err := json.Marshal(payload.Data)
	if err != nil {
		return err
	}
	routingKey := []string{string(payload.RoutingKey)}
	for _, publisher := range p.publishers {
		err := publisher.Publish(
			jsonPayload,
			routingKey,
			rmq.WithPublishOptionsAppID(p.appId),
			rmq.WithPublishOptionsContentType("application/json"),
			rmq.WithPublishOptionsContentEncoding("utf-8"),
			rmq.WithPublishOptionsHeaders(rmq.Table(payload.Headers)),
			func(options *rmq.PublishOptions) {
				if payload.Options == nil {
					return
				}
				options.DeliveryMode = payload.Options.DeliveryMode
				options.Expiration = payload.Options.Expiration
				options.Priority = payload.Options.Priority
				options.CorrelationID = payload.Options.CorrelationID
				options.MessageID = payload.Options.MessageID
				options.Timestamp = payload.Options.Timestamp
				options.Type = payload.Options.Type
				options.UserID = payload.Options.UserID
			},
		)
		if err != nil {
			return err
		}
	}

	return nil
}

func (p *Publisher[DataType, Headers, RoutingKey]) Close() error {
	for _, publisher := range p.publishers {
		publisher.Close()
	}
	return nil
}
