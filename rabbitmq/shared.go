// Copyright 2023 Kapeta Inc.
// SPDX-License-Identifier: MIT
package rabbitmq

import (
	"encoding/json"
	"fmt"
	"github.com/kapetacom/sdk-go-config/providers"
	rmq "github.com/wagslane/go-rabbitmq"
	"log"
	"net/http"
)

func ConnectToInstance(config providers.ConfigProvider, instanceId string) (*rmq.Conn, error) {
	operator, err := config.GetInstanceOperator(instanceId)
	if err != nil {
		return nil, fmt.Errorf("error getting instance operator: %v", err)
	}
	vhost, err := ensureVHost(operator, instanceId)
	if err != nil {
		return nil, fmt.Errorf("error ensuring vhost: %v", err)
	}
	return connect(operator, vhost)
}

func connect(operator *providers.InstanceOperator, vhost string) (*rmq.Conn, error) {
	if operator.Ports["amqp"].Port == 0 {
		return nil, fmt.Errorf("amqp port not found")
	}
	credentials := getCredentials(operator)
	amqpURL := fmt.Sprintf("amqp://%s:%s@%s:%d",
		credentials.Username,
		credentials.Password,
		operator.Hostname,
		operator.Ports["amqp"].Port,
	)
	return rmq.NewConn(
		amqpURL,
		rmq.WithConnectionOptionsLogging,
		rmq.WithConnectionOptionsConfig(rmq.Config{Vhost: vhost}),
	)
}

func ensureVHost(operator *providers.InstanceOperator, instanceId string) (string, error) {
	vhostName := instanceId

	client := NewRabbitRESTClient(operator)

	log.Printf("Checking RabbitMQ vhost: %s @ %s\n", vhostName, client.baseURL)

	// Check if vhost exists
	resp, err := client.GetQueues(vhostName)
	if err != nil {
		return "", fmt.Errorf("error checking vhost: %v", err)
	}

	if resp.StatusCode == http.StatusOK {
		log.Printf("Found RabbitMQ vhost: %s @ %s\n", vhostName, client.baseURL)
		return vhostName, nil
	}

	if resp.StatusCode != http.StatusNotFound {
		return "", fmt.Errorf("failed to check for existing vhost: %s @ %s. Error: %d : %s", vhostName, client.baseURL, resp.StatusCode, resp.Status)
	}

	// Create vhost
	resp, err = client.CreateVHost(vhostName)
	if err != nil {
		return "", fmt.Errorf("error creating vhost: %v", err)
	}

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("failed to create vhost: %s @ %s. Error: %d : %s", vhostName, client.baseURL, resp.StatusCode, resp.Status)
	}

	log.Printf("Created RabbitMQ vhost: %s @ %s\n", vhostName, client.baseURL)
	return vhostName, nil
}

func getCredentials(operator *providers.InstanceOperator) *providers.DefaultCredentials {
	username := operator.Credentials["username"].(string)
	password := operator.Credentials["password"].(string)

	return &providers.DefaultCredentials{
		Username: username,
		Password: password,
	}
}

func toBlockSpec(instance *providers.BlockInstanceDetails) (*BlockSpec, error) {
	bytes, err := json.Marshal(instance.Block.Spec)
	if err != nil {
		return nil, err
	}
	blockSpec := &BlockSpec{}
	err = json.Unmarshal(bytes, blockSpec)
	if err != nil {
		return nil, fmt.Errorf("error decoding block spec: %v", err)
	}

	if len(blockSpec.Consumers) == 0 ||
		len(blockSpec.Providers) == 0 ||
		blockSpec.Bindings == nil ||
		len(blockSpec.Bindings.Exchanges) == 0 {
		return nil, fmt.Errorf(
			"invalid rabbitmq block definition. Missing consumers, providers and/or bindings",
		)
	}

	return blockSpec, err
}

func dereferenceSlice[T any](slice []*T) []T {
	out := make([]T, len(slice))
	for i, v := range slice {
		out[i] = *v
	}
	return out
}
