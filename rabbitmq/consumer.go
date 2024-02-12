// Copyright 2023 Kapeta Inc.
// SPDX-License-Identifier: MIT
package rabbitmq

import (
	"encoding/json"
	"fmt"
	"github.com/kapetacom/sdk-go-config/providers"
	amqp "github.com/rabbitmq/amqp091-go"
	rmq "github.com/wagslane/go-rabbitmq"
	"log"
)

type Action = rmq.Action

const (
	// Ack default ack this msg after you have successfully processed this delivery.
	Ack = rmq.Ack
	// NackDiscard the message will be dropped or delivered to a server configured dead-letter queue.
	NackDiscard = rmq.NackDiscard
	// NackRequeue deliver this message to a different consumer.
	NackRequeue = rmq.NackRequeue
	// Message acknowledgement is left to the user using the msg.Ack() method
	Manual = rmq.Manual
)

type MessageHandler[T any] func(message T, delivery amqp.Delivery) (Action, error)

type Consumer = rmq.Consumer

func CreateConsumer[T any](config providers.ConfigProvider, resourceName string, callback MessageHandler[T]) (*rmq.Consumer, error) {
	instance, err := config.GetInstanceForConsumer(resourceName)
	if err != nil {
		return nil, err
	}

	blockSpec, err := toBlockSpec(instance)
	if err != nil {
		return nil, fmt.Errorf("error decoding block spec: %v", err)
	}

	conn, err := ConnectToInstance(config, instance.InstanceId)
	if err != nil {
		return nil, err
	}

	queueDefinitions := make([]QueueResource, 0)

	for _, connection := range instance.Connections {
		for _, queue := range blockSpec.Providers {
			if queue.Metadata.Name == connection.Provider.ResourceName &&
				connection.Consumer.ResourceName == resourceName {
				queueDefinitions = append(queueDefinitions, queue)
				break
			}
		}
	}

	if len(queueDefinitions) == 0 {
		return nil, fmt.Errorf("no queues found for provider: %s", resourceName)
	}

	if len(queueDefinitions) > 1 {
		return nil, fmt.Errorf("multiple defined queues found. Only 1 expected for provider: %s", resourceName)
	}

	queue := queueDefinitions[0]
	queueName := queue.Metadata.Name
	queueOptions := asQueue(queue)

	bindings, exchanges, err := resolveBindings(blockSpec, rmq.BindingTypeQueue, queueName)

	if queueOptions.Name == "" {
		// Exclusive queues need to have the destination name set to an empty string
		for _, binding := range bindings {
			binding.DestinationName = ""
		}
	}

	if err != nil {
		return nil, fmt.Errorf("error resolving bindings: %v", err)
	}

	consumerTag := config.GetInstanceId() + "_" + resourceName

	return rmq.NewConsumer(
		conn,
		createHandler(callback),
		queueOptions.Name,
		rmq.WithConsumerOptionsLogging,
		rmq.WithConsumerOptionsConsumerName(consumerTag),
		rmq.WithConsumerQueue(queueOptions),
		rmq.WithConsumerBindings(dereferenceSlice(bindings)),
		rmq.WithConsumerExchanges(dereferenceSlice(exchanges)),
	)
}

func createHandler[T any](callback MessageHandler[T]) func(message rmq.Delivery) (action rmq.Action) {
	return func(message rmq.Delivery) (action rmq.Action) {
		if message.Delivery.ContentType != "application/json" {
			log.Printf("message was not in json format")
			return rmq.NackDiscard
		}
		var payload T
		err := json.Unmarshal(message.Delivery.Body, &payload)
		if err != nil {
			log.Printf("Failed to parse message from %s: %s", message.Delivery.AppId, err)
			return rmq.NackDiscard
		}
		action, err = callback(payload, message.Delivery)
		if err != nil {
			return rmq.NackRequeue
		}
		return action
	}
}
