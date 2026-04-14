package main

import (
	"log"

	server "github.com/belalakhter/ethereum_screener/internals/server"
	token "github.com/belalakhter/ethereum_screener/internals/token"
	"github.com/joho/godotenv"
)

func main() {
	err := godotenv.Load()
	if err != nil {
		log.Println("Error loading .env file, assuming env vars are set")
	}
	http_eth_client := token.NewEthClient()
	ws_eth_client := token.NewEthClientWS()
	NewServer := server.NewServer(http_eth_client, ws_eth_client)
	NewServer.Start(":8080")
}
