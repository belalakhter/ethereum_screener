package token

import (
	"log"

	"os"

	"context"

	"github.com/defiweb/go-eth/rpc"
	"github.com/defiweb/go-eth/rpc/transport"
	"github.com/ethereum/go-ethereum/ethclient"
)

var http_url = os.Getenv("HTTP_URL")
var ws_url = os.Getenv("WS_URL")

func NewEthClient() *ethclient.Client {

	client, err := ethclient.Dial(http_url)
	if err != nil {
		log.Fatalf("Failed to connect to Ethereum: %v", err)
	}
	return client
}
func NewEthClientWS() *rpc.Client {
	ctx := context.Background()
	transport, err := transport.NewWebsocket(transport.WebsocketOptions{
		Context: ctx,
		URL:     ws_url,
	})
	if err != nil {
		panic(err)
	}

	client, err := rpc.NewClient(rpc.WithTransport(transport))
	if err != nil {
		panic(err)
	}
	return client
}
