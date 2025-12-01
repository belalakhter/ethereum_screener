package token

import (
	"context"
	"math/big"
	"time"

	"github.com/defiweb/go-eth/abi"
	"github.com/defiweb/go-eth/rpc"
	"github.com/defiweb/go-eth/types"
)

type Tx struct {
	Hash     string
	Src      string
	Dst      string
	Wad      string
	TimeUnix int64
}

func GetTransactions(ctx context.Context, client *rpc.Client, tokenHash string, txChan chan Tx) {
	tokenAddress := types.MustAddressFromHex(tokenHash)

	transfer := abi.MustParseEvent("event Transfer(address indexed src, address indexed dst, uint256 wad)")

	query := types.NewFilterLogsQuery().
		SetAddresses(tokenAddress).
		SetTopics([]types.Hash{transfer.Topic0()})

	logs, err := client.SubscribeLogs(ctx, query)
	if err != nil {
		panic(err)
	}

	for {
		select {
		case <-ctx.Done():
			return
		case log, ok := <-logs:
			if !ok {
				return
			}

			var (
				src types.Address
				dst types.Address
				wad *big.Int
			)
			transfer.MustDecodeValues(log.Topics, log.Data, &src, &dst, &wad)
			tx := Tx{
				Hash:     log.TransactionHash.String(),
				Src:      src.String(),
				Dst:      dst.String(),
				Wad:      wad.String(),
				TimeUnix: time.Now().Unix(),
			}

			select {
			case txChan <- tx:
			case <-ctx.Done():
				return
			}
		}
	}
}
