package token

import (
	"context"
	"fmt"
	"log"
	"math/big"
	"strings"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/ethclient"
)

type TokenMetadata struct {
	Name         string
	Symbol       string
	Price        *big.Float
	BaseAddress  string
	TotalSupply  *big.Int
	MarketCap    *big.Float
	Volume24h    *big.Float
	PooledWETH   *big.Float
	PooledToken  *big.Int
	TotalTxCount uint64
}

const UniswapV2PairABI = `[
	{"constant":true,"inputs":[],"name":"getReserves","outputs":[{"name":"reserve0","type":"uint112"},{"name":"reserve1","type":"uint112"},{"name":"blockTimestampLast","type":"uint32"}],"type":"function"},
	{"constant":true,"inputs":[],"name":"token0","outputs":[{"name":"","type":"address"}],"type":"function"},
	{"constant":true,"inputs":[],"name":"token1","outputs":[{"name":"","type":"address"}],"type":"function"},
	{"anonymous":false,"inputs":[{"indexed":true,"name":"sender","type":"address"},{"indexed":false,"name":"amount0In","type":"uint256"},{"indexed":false,"name":"amount1In","type":"uint256"},{"indexed":false,"name":"amount0Out","type":"uint256"},{"indexed":false,"name":"amount1Out","type":"uint256"},{"indexed":true,"name":"to","type":"address"}],"name":"Swap","type":"event"}
]`

const erc20ABI = `[
	{"constant":true,"name":"name","outputs":[{"name":"","type":"string"}],"inputs":[],"type":"function"},
	{"constant":true,"name":"symbol","outputs":[{"name":"","type":"string"}],"inputs":[],"type":"function"},
	{"constant":true,"name":"totalSupply","outputs":[{"name":"","type":"uint256"}],"inputs":[],"type":"function"},
	{"constant":true,"name":"decimals","outputs":[{"name":"","type":"uint8"}],"inputs":[],"type":"function"}
]`

var WETH_ADDRESS = common.HexToAddress("0xC02aaA39b223FE8d0A0e5C4F27eAD9083C756Cc2")

func fetchTokenInfo(client *ethclient.Client, ctx context.Context, tokenAddress common.Address) (string, string, uint8) {
	ercAbi, err := abi.JSON(strings.NewReader(erc20ABI))
	if err != nil {
		fmt.Printf("Failed to parse ERC20 ABI: %v", err)
	}

	nameData, _ := ercAbi.Pack("name")
	nameRes, err := client.CallContract(ctx, ethereum.CallMsg{To: &tokenAddress, Data: nameData}, nil)
	if err != nil || len(nameRes) == 0 {
		log.Printf("Failed to fetch token name: %v", err)
		return "Unknown", "UNK", 18
	}
	name, _ := ercAbi.Unpack("name", nameRes)

	symbolData, _ := ercAbi.Pack("symbol")
	symbolRes, err := client.CallContract(ctx, ethereum.CallMsg{To: &tokenAddress, Data: symbolData}, nil)
	if err != nil || len(symbolRes) == 0 {
		log.Printf("Failed to fetch token symbol: %v", err)
		return name[0].(string), "UNK", 18
	}
	symbol, _ := ercAbi.Unpack("symbol", symbolRes)

	decimalsData, _ := ercAbi.Pack("decimals")
	decimalsRes, err := client.CallContract(ctx, ethereum.CallMsg{To: &tokenAddress, Data: decimalsData}, nil)
	if err != nil || len(decimalsRes) == 0 {
		log.Printf("Failed to fetch token decimals: %v", err)
		return name[0].(string), symbol[0].(string), 18
	}
	decimals := new(big.Int).SetBytes(decimalsRes).Uint64()

	return name[0].(string), symbol[0].(string), uint8(decimals)
}

func getTotalSupply(client *ethclient.Client, ctx context.Context, tokenAddress common.Address) *big.Int {
	tokenAbi, err := abi.JSON(strings.NewReader(erc20ABI))
	if err != nil {
		log.Printf("ERC20 ABI parse failed: %v", err)
		return big.NewInt(0)
	}

	dataTotalSupply, err := tokenAbi.Pack("totalSupply")
	if err != nil {
		log.Printf("Pack totalSupply failed: %v", err)
		return big.NewInt(0)
	}

	supplyRaw, err := client.CallContract(ctx, ethereum.CallMsg{
		To:   &tokenAddress,
		Data: dataTotalSupply,
	}, nil)
	if err != nil {
		log.Printf("Call totalSupply failed: %v", err)
		return big.NewInt(0)
	}

	totalSupply := new(big.Int)
	totalSupply.SetBytes(supplyRaw)
	return totalSupply
}

func calculateVolume24h(client *ethclient.Client, ctx context.Context, pairAddress common.Address) *big.Float {
	currentBlock, err := client.BlockNumber(ctx)
	if err != nil {
		log.Printf("Failed to get current block: %v", err)
		return big.NewFloat(0)
	}

	blocksIn24h := uint64(7200)
	fromBlock := currentBlock - blocksIn24h
	if fromBlock < 0 {
		fromBlock = 0
	}

	contractAbi, err := abi.JSON(strings.NewReader(UniswapV2PairABI))
	if err != nil {
		log.Printf("Failed to parse ABI: %v", err)
		return big.NewFloat(0)
	}

	swapEventID := contractAbi.Events["Swap"].ID
	query := ethereum.FilterQuery{
		FromBlock: big.NewInt(int64(fromBlock)),
		ToBlock:   big.NewInt(int64(currentBlock)),
		Addresses: []common.Address{pairAddress},
		Topics:    [][]common.Hash{{swapEventID}},
	}

	logs, err := client.FilterLogs(ctx, query)
	if err != nil {
		log.Printf("Failed to filter logs: %v", err)
		return big.NewFloat(0)
	}

	totalVolume := big.NewFloat(0)
	for _, vLog := range logs {
		if len(vLog.Data) >= 128 {
			amount0In := new(big.Int).SetBytes(vLog.Data[0:32])
			amount1In := new(big.Int).SetBytes(vLog.Data[32:64])
			amount0Out := new(big.Int).SetBytes(vLog.Data[64:96])
			amount1Out := new(big.Int).SetBytes(vLog.Data[96:128])
			swapVolume := new(big.Int)
			swapVolume.Add(swapVolume, amount0In)
			swapVolume.Add(swapVolume, amount1In)
			swapVolume.Add(swapVolume, amount0Out)
			swapVolume.Add(swapVolume, amount1Out)

			totalVolume.Add(totalVolume, new(big.Float).SetInt(swapVolume))
		}
	}

	return totalVolume
}

func getTransactionCount(client *ethclient.Client, ctx context.Context, pairAddress common.Address) uint64 {
	currentBlock, err := client.BlockNumber(ctx)
	if err != nil {
		log.Printf("Failed to get current block: %v", err)
		return 0
	}

	fromBlock := big.NewInt(0)
	if currentBlock > 10000 {
		fromBlock = big.NewInt(int64(currentBlock - 10000))
	}

	contractAbi, err := abi.JSON(strings.NewReader(UniswapV2PairABI))
	if err != nil {
		log.Printf("Failed to parse ABI: %v", err)
		return 0
	}

	swapEventID := contractAbi.Events["Swap"].ID
	query := ethereum.FilterQuery{
		FromBlock: fromBlock,
		ToBlock:   big.NewInt(int64(currentBlock)),
		Addresses: []common.Address{pairAddress},
		Topics:    [][]common.Hash{{swapEventID}},
	}

	logs, err := client.FilterLogs(ctx, query)
	if err != nil {
		log.Printf("Failed to filter logs: %v", err)
		return 0
	}

	return uint64(len(logs))
}

func GetMetadata(ctx context.Context, client *ethclient.Client, pairAddress common.Address) *TokenMetadata {

	contractAbi, err := abi.JSON(strings.NewReader(UniswapV2PairABI))
	if err != nil {
		fmt.Printf("ABI parse failed: %v", err)
	}

	data0, _ := contractAbi.Pack("token0")
	res0, err := client.CallContract(ctx, ethereum.CallMsg{To: &pairAddress, Data: data0}, nil)
	if err != nil {
		fmt.Printf("CallContract failed %v:", err)
	}
	token0 := common.BytesToAddress(res0)

	data1, _ := contractAbi.Pack("token1")
	res1, err := client.CallContract(ctx, ethereum.CallMsg{To: &pairAddress, Data: data1}, nil)
	if err != nil {
		fmt.Printf("CallContract failed: %v", err)
	}
	token1 := common.BytesToAddress(res1)

	reserveData, _ := contractAbi.Pack("getReserves")
	reservesRes, err := client.CallContract(ctx, ethereum.CallMsg{To: &pairAddress, Data: reserveData}, nil)
	if err != nil {
		fmt.Printf("reservesRes failed: %v", err)
	}
	reserves, err := contractAbi.Unpack("getReserves", reservesRes)
	if err != nil {
		fmt.Printf("Unpack failed: %v", err)
	}

	reserve0 := reserves[0].(*big.Int)
	reserve1 := reserves[1].(*big.Int)

	var targetToken common.Address
	var wethReserve, tokenReserve *big.Int

	if token0 == WETH_ADDRESS {
		targetToken = token1
		wethReserve = reserve0
		tokenReserve = reserve1
	} else if token1 == WETH_ADDRESS {
		targetToken = token0
		wethReserve = reserve1
		tokenReserve = reserve0
	} else {
		targetToken = token0
		wethReserve = reserve1
		tokenReserve = reserve0
	}

	name, symbol, decimals := fetchTokenInfo(client, ctx, targetToken)

	var price *big.Float
	if tokenReserve.Cmp(big.NewInt(0)) > 0 {
		price = new(big.Float).Quo(
			new(big.Float).SetInt(wethReserve),
			new(big.Float).SetInt(tokenReserve),
		)
	} else {
		price = big.NewFloat(0)
	}

	totalSupply := getTotalSupply(client, ctx, targetToken)

	marketCap := new(big.Float).Mul(price, new(big.Float).SetInt(totalSupply))

	volume24h := calculateVolume24h(client, ctx, pairAddress)

	txCount := getTransactionCount(client, ctx, pairAddress)

	decimalsAdjustment := new(big.Float).SetInt(new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(decimals)), nil))
	adjustedMarketCap := new(big.Float).Quo(marketCap, decimalsAdjustment)

	metadata := &TokenMetadata{
		Name:         name,
		Symbol:       symbol,
		Price:        price,
		BaseAddress:  targetToken.String(),
		TotalSupply:  totalSupply,
		MarketCap:    adjustedMarketCap,
		Volume24h:    volume24h,
		PooledWETH:   FormatEther(wethReserve),
		PooledToken:  tokenReserve,
		TotalTxCount: txCount,
	}

	return metadata
}

func FormatBigFloat(f *big.Float, precision int) string {
	return f.Text('f', precision)
}

func FormatBigInt(i *big.Int) string {
	return i.String()
}
func FormatEther(wei *big.Int) *big.Float {
	ether := new(big.Float).Quo(new(big.Float).SetInt(wei), big.NewFloat(1e18))
	return ether
}
