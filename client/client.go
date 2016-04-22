package client

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/pagarme/teleport/config"
	// "mime/multipart"
	"net/http"
	// "io"
	"os"
)

type Client struct {
	config.Target
}

func New(target config.Target) *Client {
	return &Client{
		target,
	}
}

func (c *Client) urlForRequest(path string) string {
	return fmt.Sprintf(
		"http://%s:%d%v",
		c.Endpoint.Hostname,
		c.Endpoint.Port,
		path,
	)
}

func (c *Client) SendRequest(path string, obj interface{}) (*http.Response, error) {
	data := new(bytes.Buffer)
	json.NewEncoder(data).Encode(obj)

	return http.Post(
		c.urlForRequest(path),
		"application/json",
		data,
	)
}

func (c *Client) SendFile(path, formField string, file *os.File) (*http.Response, error) {
	return http.Post(
		c.urlForRequest(path),
		"application/json",
		file,
	)
}
