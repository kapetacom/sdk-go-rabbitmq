// Copyright 2023 Kapeta Inc.
// SPDX-License-Identifier: MIT
package rabbitmq

import "github.com/kapetacom/schemas/packages/go/model"

type PayloadType struct {
	Type      string       `json:"type"`
	Structure model.Entity `json:"structure"`
}

type BaseSpec struct {
	Port struct {
		Type string `json:"type"`
	} `json:"port"`
	PayloadType PayloadType `json:"payloadType"`
}

type SubscriberSpec BaseSpec

type HeaderRoute struct {
	Name   string   `json:"name"`
	Values []string `json:"values"`
}

type PublisherSpec struct {
	BaseSpec
	RouteKeys *struct {
		Text string   `json:"text"`
		Data []string `json:"data"`
	} `json:"routeKeys,omitempty"`
	Headers *struct {
		Text string        `json:"text"`
		Data []HeaderRoute `json:"data"`
	} `json:"headers,omitempty"`
}

type ExchangeSpec struct {
	BaseSpec
	ExchangeType string `json:"exchangeType"`
	Durable      bool   `json:"durable,omitempty"`
	AutoDelete   bool   `json:"autoDelete,omitempty"`
}

type QueueSpec struct {
	BaseSpec
	Durable    bool `json:"durable,omitempty"`
	Exclusive  bool `json:"exclusive,omitempty"`
	AutoDelete bool `json:"autoDelete,omitempty"`
}

type HeaderBindings struct {
	MatchAll bool              `json:"matchAll"`
	Headers  map[string]string `json:"headers"`
}

type ExchangeRouting interface{}

type ExchangeBindingSchema struct {
	Name    string          `json:"name"`
	Type    string          `json:"type"`
	Routing ExchangeRouting `json:"routing,omitempty"`
}

type ExchangeBindingsSchema struct {
	Exchange string                  `json:"exchange"`
	Bindings []ExchangeBindingSchema `json:"bindings,omitempty"`
}

type BindingsSchema struct {
	Exchanges []ExchangeBindingsSchema `json:"exchanges,omitempty"`
}

type BlockSpec struct {
	Entities  *model.EntityList  `json:"entities,omitempty"`
	Consumers []ExchangeResource `json:"consumers,omitempty"`
	Providers []QueueResource    `json:"providers,omitempty"`
	Bindings  *BindingsSchema    `json:"bindings,omitempty"`
}

type BlockDefinition struct {
	Kind     string         `json:"kind"`
	Metadata model.Metadata `json:"metadata"`
	Spec     BlockSpec      `json:"spec"`
}

type SubscriberResource struct {
	ResourceWithSpec[SubscriberSpec] `json:",inline"`
}

type PublisherResource struct {
	ResourceWithSpec[PublisherSpec] `json:",inline"`
}

type ExchangeResource struct {
	ResourceWithSpec[ExchangeSpec] `json:",inline"`
}

type QueueResource struct {
	ResourceWithSpec[QueueSpec] `json:",inline"`
}

type ResourceWithSpec[T any] struct {
	Kind     string                 `json:"kind"`
	Metadata model.ResourceMetadata `json:"metadata"`
	Spec     T                      `json:"spec"`
}

type OperatorOptions struct {
	Vhost string `json:"vhost"`
}
