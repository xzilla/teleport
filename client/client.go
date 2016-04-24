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
	"io/ioutil"
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

func (c *Client) handleResponse(res *http.Response, err error) error {
	if err != nil {
		return err
	}

	if res.StatusCode != 200 {
		body, _ := ioutil.ReadAll(res.Body)
		return fmt.Errorf(string(body))
	}

	return nil
}

func (c *Client) SendRequest(path string, obj interface{}) error {
	data := new(bytes.Buffer)
	json.NewEncoder(data).Encode(obj)

	res, err := http.Post(
		c.urlForRequest(path),
		"application/json",
		data,
	)

	defer res.Body.Close()
	return c.handleResponse(res, err)
}

func (c *Client) SendFile(path, formField string, file *os.File) error {
	res, err := http.Post(
		c.urlForRequest(path),
		"application/json",
		file,
	)

	defer res.Body.Close()
	return c.handleResponse(res, err)
}
