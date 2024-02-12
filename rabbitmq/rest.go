// Copyright 2023 Kapeta Inc.
// SPDX-License-Identifier: MIT

package rabbitmq

import (
	"encoding/base64"
	"fmt"
	"github.com/hashicorp/go-retryablehttp"
	"github.com/kapetacom/sdk-go-config/providers"
	"net/http"
	"net/url"
)

type RabbitRESTClient struct {
	operator *providers.InstanceOperator
	client   *retryablehttp.Client
	baseURL  string
}

func NewRabbitRESTClient(operator *providers.InstanceOperator) *RabbitRESTClient {
	port := 15672
	if p, ok := operator.Ports["management"]; ok && p.Port != 0 {
		port = p.Port
	}

	client := retryablehttp.NewClient()
	client.RetryMax = 100
	return &RabbitRESTClient{
		client:   client,
		operator: operator,
		baseURL:  fmt.Sprintf("http://%s:%d/api", operator.Hostname, port),
	}
}

func (c *RabbitRESTClient) GetQueues(vhostName string) (*http.Response, error) {
	requestUrl := fmt.Sprintf("%s/queues/%s", c.baseURL, url.PathEscape(vhostName))
	return c.doRequest("GET", requestUrl)
}

func (c *RabbitRESTClient) CreateVHost(vhostName string) (*http.Response, error) {
	requestUrl := fmt.Sprintf("%s/vhosts/%s", c.baseURL, url.PathEscape(vhostName))
	return c.doRequest("PUT", requestUrl)
}

func (c *RabbitRESTClient) doRequest(method, url string) (*http.Response, error) {
	req, err := c.createRequest(method, url)

	if err != nil {
		return nil, err
	}
	resp, err := c.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	return resp, nil
}

func (c *RabbitRESTClient) createHeaders() *http.Header {
	credential := getCredentials(c.operator)

	auth := base64.StdEncoding.EncodeToString([]byte(credential.Username + ":" + credential.Password))
	return &http.Header{
		"Authorization":      []string{"Basic " + auth},
		"Content-SourceType": []string{"application/json"},
	}
}

func (c *RabbitRESTClient) createRequest(method, url string) (*retryablehttp.Request, error) {
	req, err := retryablehttp.NewRequest(method, url, nil)
	if err != nil {
		return nil, fmt.Errorf("error checking vhost: %v", err)
	}
	headers := c.createHeaders()
	for k, v := range *headers {
		req.Header[k] = v
	}
	return req, nil
}
