// Copyright 2023 Kapeta Inc.
// SPDX-License-Identifier: MIT

package rabbitmq

import (
	"fmt"
	"github.com/mitchellh/mapstructure"
	rmq "github.com/wagslane/go-rabbitmq"
	"strings"
)

func asExchange(exchange *ExchangeResource) rmq.ExchangeOptions {
	return rmq.ExchangeOptions{
		Name:       exchange.Metadata.Name,
		Durable:    exchange.Spec.Durable,
		AutoDelete: exchange.Spec.AutoDelete,
		Kind:       exchange.Spec.ExchangeType,
		Declare:    true,
	}
}

func asQueue(queue QueueResource) rmq.QueueOptions {
	queueRequestName := queue.Metadata.Name
	if queue.Spec.Exclusive {
		queueRequestName = ""
	}
	return rmq.QueueOptions{
		Name:       queueRequestName,
		Durable:    queue.Spec.Durable,
		AutoDelete: queue.Spec.AutoDelete,
		Exclusive:  queue.Spec.Exclusive,
		Declare:    true,
	}
}

func getBindingHeaders(binding ExchangeBindingSchema) (rmq.Table, error) {
	rawHeader, ok := binding.Routing.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("invalid headers binding")
	}

	var headerBindings HeaderBindings
	err := mapstructure.Decode(rawHeader, &headerBindings)
	if err != nil {
		return nil, fmt.Errorf("error decoding headers binding: %v", err)
	}

	headers := rmq.Table{}
	for key, value := range headerBindings.Headers {
		headers[key] = value
	}

	if headerBindings.MatchAll {
		headers["x-match"] = "all"
	} else {
		headers["x-match"] = "any"
	}
	return headers, nil
}

func resolveBindings(blockSpec *BlockSpec, destinationType rmq.BindingDestinationType, destinationName string) ([]*rmq.Binding, []*rmq.ExchangeOptions, error) {
	exchanges := make([]*rmq.ExchangeOptions, 0)
	bindings := make([]*rmq.Binding, 0)

	for _, exchangeBindings := range blockSpec.Bindings.Exchanges {
		var exchange *ExchangeResource
		for _, exchangeDefinition := range blockSpec.Consumers {
			if exchangeDefinition.Metadata.Name == exchangeBindings.Exchange {
				exchange = &exchangeDefinition
				break
			}
		}
		if exchange == nil {
			return nil, nil, fmt.Errorf("exchange not found for binding")
		}

		if len(exchangeBindings.Bindings) == 0 {
			// no bindings for this exchange
			continue
		}

		foundAnyBinding := false

		for _, binding := range exchangeBindings.Bindings {
			if !strings.EqualFold(binding.Type, string(destinationType)) ||
				binding.Name != destinationName {
				// not a binding for this destination
				continue
			}

			foundAnyBinding = true
			routingKey, ok := binding.Routing.(string)
			if ok {
				// routing key binding
				bindings = append(bindings, &rmq.Binding{
					DestinationName: destinationName,
					DestinationType: destinationType,
					ExchangeName:    exchange.Metadata.Name,
					RoutingKey:      routingKey,
					BindingOptions: rmq.BindingOptions{
						Declare: true,
					},
				})
			} else {
				// headers binding
				headers, err := getBindingHeaders(binding)
				if err != nil {
					return nil, nil, err
				}

				bindings = append(bindings, &rmq.Binding{
					DestinationName: destinationName,
					DestinationType: destinationType,
					ExchangeName:    exchange.Metadata.Name,
					BindingOptions: rmq.BindingOptions{
						Declare: true,
						Args:    headers,
					},
				})
			}
		}

		if !foundAnyBinding {
			continue
		}

		// We only want the exchanges that we have bindings for
		exchangeOptions := asExchange(exchange)
		exchanges = append(exchanges, &exchangeOptions)
	}

	return bindings, exchanges, nil
}
