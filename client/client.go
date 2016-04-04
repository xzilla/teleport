package client

import (
	"github.com/pagarme/teleport/config"
)

type Client struct {
	config.Target
}

func New(target config.Target) *Client {
	return &Client{
		target,
	}
}
