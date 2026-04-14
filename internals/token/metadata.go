package token

import (
	"context"
	"errors"
	"fmt"
	"log"
	"math/big"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/ethclient"
)

type TokenMetadata struct {
	Name               string
	Symbol             string
	Price              *big.Float
	PriceDisplay       string
	BaseAddress        string
	PairAddress        string
	TotalSupply        *big.Int
	TotalSupplyDisplay string
	MarketCap          string
	Liquidity          string
	Volume24h          string
	PooledWETH         *big.Float
	PooledToken        *big.Int
	TotalTxCount       uint64
	IsToken0           bool
	PriceUSD           string
	Error              string
	CachedAt           time.Time `json:"-"`
}

type TransactionStreamConfig struct {
	TokenAddressHex         string
	PairAddressHex          string
	WrappedNativeAddressHex string
	WrappedIsToken0         bool
	TokenDecimals           uint8
	WrappedNativeUSDPrice   *big.Float
}

type wrappedNativeUSDQuoteSource struct {
	pairAddress   string
	stableAddress string
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

const wrappedNativeUSDPriceCacheTTL = 5 * time.Minute

var (
	defaultWETHAddress       = common.HexToAddress("0xC02aaA39b223FE8d0A0e5C4F27eAD9083C756Cc2")
	errEmptyContractResponse = errors.New("contract returned empty response")
	errNoContractCode        = errors.New("no contract code at address")
	errTokenAddressInput     = errors.New("address looks like an ERC20 token, not a pair")
	errNoWrappedNativePair   = errors.New("no wrapped native pair found for token")
	errPairMissingWrapped    = errors.New("pair does not include the wrapped native token")
	errUnsupportedPair       = errors.New("address does not expose Uniswap V2 pair methods")
	wethAddressesByChainID   = map[string]string{
		"1":     "0xC02aaA39b223FE8d0A0e5C4F27eAD9083C756Cc2",
		"10":    "0x4200000000000000000000000000000000000006",
		"8453":  "0x4200000000000000000000000000000000000006",
		"42161": "0x82aF49447D8a07e3bd95BD0d56f35241523FbAb1",
	}
	networkNamesByChainID = map[string]string{
		"1":     "Ethereum Mainnet",
		"10":    "Optimism",
		"8453":  "Base",
		"42161": "Arbitrum One",
	}
	stablecoinAddressesByChainID = map[string][]string{
		"1": {
			"0xA0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",
			"0xdAC17F958D2ee523a2206206994597C13D831ec7",
			"0x6B175474E89094C44Da98b954EedeAC495271d0F",
		},
		"10": {
			"0x0b2C639c533813f4Aa9D7837CAf62653d097FF85",
			"0x94b008aA00579c1307B0EF2c499aD98a8ce58e58",
			"0xDA10009cBd5D07dd0CeCc66161FC93D7c9000da1",
		},
		"8453": {
			"0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913",
		},
		"42161": {
			"0xaf88d065e77c8cC2239327C5EDb3A432268e5831",
			"0xFF970A61A04b1Ca14834A43f5dE4533eBDDB5CC8",
			"0xFd086bC7CD5C481DCC9C85ebe478A1C0b69FCbb9",
			"0xDA10009cBd5D07dd0CeCc66161FC93D7c9000da1",
		},
	}
	wrappedNativeUSDQuoteSourcesByChainID = map[string]wrappedNativeUSDQuoteSource{
		"1": {
			pairAddress:   "0xB4e16d0168e52d35CaCD2c6185b44281Ec28C9Dc",
			stableAddress: "0xA0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",
		},
	}
)

func NewMetadataMessage(message string) *TokenMetadata {
	return &TokenMetadata{
		Name:         "Unavailable",
		Symbol:       "N/A",
		Price:        big.NewFloat(0),
		PriceDisplay: "0.00",
		TotalSupply:  big.NewInt(0),
		MarketCap:    "0.00",
		Volume24h:    "0.00",
		PooledWETH:   big.NewFloat(0),
		PooledToken:  big.NewInt(0),
		TotalTxCount: 0,
		Error:        message,
	}
}

func ResolveWrappedNativeAddress(ctx context.Context, client *ethclient.Client) common.Address {
	addressFromEnv := strings.TrimSpace(os.Getenv("WETH_ADDRESS"))
	if common.IsHexAddress(addressFromEnv) {
		return common.HexToAddress(addressFromEnv)
	}

	chainID, err := client.ChainID(ctx)
	if err != nil {
		log.Printf("Failed to resolve chain ID, falling back to Ethereum WETH: %v", err)
		return defaultWETHAddress
	}

	if wrappedNative, ok := wethAddressesByChainID[chainID.String()]; ok {
		return common.HexToAddress(wrappedNative)
	}

	log.Printf("Unsupported chain ID %s for wrapped native token auto-detection, falling back to Ethereum WETH", chainID.String())
	return defaultWETHAddress
}

func ConnectedNetworkLabel(ctx context.Context, client *ethclient.Client) string {
	chainID, err := client.ChainID(ctx)
	if err != nil {
		log.Printf("Failed to resolve chain ID for network label: %v", err)
		return "the connected network"
	}

	return networkLabelForChainID(chainID.String())
}

func MetadataLookupMessage(address common.Address, network string, err error) string {
	if strings.TrimSpace(network) == "" {
		network = "the connected network"
	}

	switch {
	case errors.Is(err, errNoContractCode):
		return fmt.Sprintf("No contract code was found at %s on %s. If this address is on another chain, point HTTP_URL and WS_URL at that network first.", address.Hex(), network)
	case errors.Is(err, errTokenAddressInput):
		return fmt.Sprintf("%s on %s looks like a token contract, not a Uniswap V2-style pair. Enter the pair address instead.", address.Hex(), network)
	case errors.Is(err, errNoWrappedNativePair):
		return fmt.Sprintf("I couldn't find a Uniswap V2-style pool for %s against the chain wrapped native token on %s. This app currently prices tokens from wrapped-native V2 pools; Uniswap V3, Curve, and Balancer need separate adapters.", address.Hex(), network)
	case errors.Is(err, errPairMissingWrapped):
		return fmt.Sprintf("%s on %s is a pool, but it is not paired against the chain wrapped native token. This app currently prices tokens from wrapped-native V2 pools only.", address.Hex(), network)
	case errors.Is(err, errUnsupportedPair):
		return fmt.Sprintf("%s on %s is a contract, but it does not expose the Uniswap V2 pair methods this app expects (`token0`, `token1`, `getReserves`).", address.Hex(), network)
	default:
		return fmt.Sprintf("Couldn't load metadata for %s on %s.", address.Hex(), network)
	}
}

func fetchTokenInfo(client *ethclient.Client, ctx context.Context, tokenAddress common.Address) (string, string, uint8) {
	ercAbi, err := abi.JSON(strings.NewReader(erc20ABI))
	if err != nil {
		log.Printf("Failed to parse ERC20 ABI: %v", err)
		return "Unknown", "UNK", 18
	}

	name := "Unknown"
	nameData, err := ercAbi.Pack("name")
	if err == nil {
		nameRes, callErr := callContract(ctx, client, tokenAddress, nameData)
		if callErr != nil {
			log.Printf("Failed to fetch token name: %v", callErr)
		} else if unpackedName, unpackErr := unpackStringResult(ercAbi, "name", nameRes); unpackErr != nil {
			log.Printf("Failed to decode token name: %v", unpackErr)
		} else {
			name = unpackedName
		}
	} else {
		log.Printf("Failed to pack token name call: %v", err)
	}

	symbol := "UNK"
	symbolData, err := ercAbi.Pack("symbol")
	if err == nil {
		symbolRes, callErr := callContract(ctx, client, tokenAddress, symbolData)
		if callErr != nil {
			log.Printf("Failed to fetch token symbol: %v", callErr)
		} else if unpackedSymbol, unpackErr := unpackStringResult(ercAbi, "symbol", symbolRes); unpackErr != nil {
			log.Printf("Failed to decode token symbol: %v", unpackErr)
		} else {
			symbol = unpackedSymbol
		}
	} else {
		log.Printf("Failed to pack token symbol call: %v", err)
	}

	decimals := uint8(18)
	decimalsData, err := ercAbi.Pack("decimals")
	if err == nil {
		decimalsRes, callErr := callContract(ctx, client, tokenAddress, decimalsData)
		if callErr != nil {
			log.Printf("Failed to fetch token decimals: %v", callErr)
		} else if unpackedDecimals, unpackErr := unpackUint8Result(ercAbi, "decimals", decimalsRes); unpackErr != nil {
			log.Printf("Failed to decode token decimals: %v", unpackErr)
		} else {
			decimals = unpackedDecimals
		}
	} else {
		log.Printf("Failed to pack token decimals call: %v", err)
	}

	return name, symbol, decimals
}

func fetchTokenDecimals(client *ethclient.Client, ctx context.Context, tokenAddress common.Address, fallback uint8) uint8 {
	tokenDecimalsCache.mu.RLock()
	cached, ok := tokenDecimalsCache.entries[tokenAddress]
	tokenDecimalsCache.mu.RUnlock()
	if ok && time.Now().Before(cached.expiresAt) {
		return cached.decimals
	}

	ercAbi, err := abi.JSON(strings.NewReader(erc20ABI))
	if err != nil {
		log.Printf("Failed to parse ERC20 ABI for decimals on %s: %v", tokenAddress.Hex(), err)
		return fallback
	}

	decimalsData, err := ercAbi.Pack("decimals")
	if err != nil {
		log.Printf("Failed to pack token decimals call for %s: %v", tokenAddress.Hex(), err)
		return fallback
	}

	decimalsRes, err := callContract(ctx, client, tokenAddress, decimalsData)
	if err != nil {
		log.Printf("Failed to fetch token decimals for %s: %v", tokenAddress.Hex(), err)
		return fallback
	}

	decimals, err := unpackUint8Result(ercAbi, "decimals", decimalsRes)
	if err != nil {
		log.Printf("Failed to decode token decimals for %s: %v", tokenAddress.Hex(), err)
		return fallback
	}

	tokenDecimalsCache.mu.Lock()
	tokenDecimalsCache.entries[tokenAddress] = cachedTokenDecimals{
		decimals:  decimals,
		expiresAt: time.Now().Add(24 * time.Hour),
	}
	tokenDecimalsCache.mu.Unlock()

	return decimals
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

func calculateVolume24h(client *ethclient.Client, ctx context.Context, pairAddress, token0, token1, wrappedNativeAddress common.Address) *big.Float {
	currentBlock, err := client.BlockNumber(ctx)
	if err != nil {
		log.Printf("Failed to get current block: %v", err)
		return big.NewFloat(0)
	}

	blocksIn24h := uint64(7200)
	var fromBlock uint64
	if currentBlock > blocksIn24h {
		fromBlock = currentBlock - blocksIn24h
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

	wrappedTokenIsToken0 := token0 == wrappedNativeAddress
	wrappedTokenIsToken1 := token1 == wrappedNativeAddress
	if !wrappedTokenIsToken0 && !wrappedTokenIsToken1 {
		return big.NewFloat(0)
	}

	totalWrappedVolumeRaw := big.NewInt(0)
	for _, vLog := range logs {
		if len(vLog.Data) >= 128 {
			amount0In := new(big.Int).SetBytes(vLog.Data[0:32])
			amount1In := new(big.Int).SetBytes(vLog.Data[32:64])
			amount0Out := new(big.Int).SetBytes(vLog.Data[64:96])
			amount1Out := new(big.Int).SetBytes(vLog.Data[96:128])

			wrappedVolumeRaw := new(big.Int)
			if wrappedTokenIsToken0 {
				wrappedVolumeRaw.Add(amount0In, amount0Out)
			} else {
				wrappedVolumeRaw.Add(amount1In, amount1Out)
			}

			totalWrappedVolumeRaw.Add(totalWrappedVolumeRaw, wrappedVolumeRaw)
		}
	}

	totalWrappedVolume := formatTokenAmount(totalWrappedVolumeRaw, 18)
	return totalWrappedVolume
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

func GetTrackedToken(ctx context.Context, client *ethclient.Client, pairAddress, wrappedNativeAddress common.Address) (common.Address, error) {
	contractAbi, err := abi.JSON(strings.NewReader(UniswapV2PairABI))
	if err != nil {
		return common.Address{}, fmt.Errorf("parse pair ABI: %w", err)
	}

	token0, token1, err := getPairTokens(ctx, client, contractAbi, pairAddress)
	if err != nil {
		return common.Address{}, diagnosePairAddress(ctx, client, pairAddress, err)
	}
	if !pairUsesWrappedNative(token0, token1, wrappedNativeAddress) {
		return common.Address{}, fmt.Errorf("%w: %s", errPairMissingWrapped, pairAddress.Hex())
	}

	targetToken, _, _ := selectTargetToken(token0, token1, big.NewInt(0), big.NewInt(0), wrappedNativeAddress)
	return targetToken, nil
}

func BuildTransactionStreamConfig(ctx context.Context, client *ethclient.Client, pairAddress, wrappedNativeAddress common.Address) (*TransactionStreamConfig, error) {
	contractAbi, err := abi.JSON(strings.NewReader(UniswapV2PairABI))
	if err != nil {
		return nil, fmt.Errorf("parse pair ABI: %w", err)
	}

	token0, token1, err := getPairTokens(ctx, client, contractAbi, pairAddress)
	if err != nil {
		return nil, diagnosePairAddress(ctx, client, pairAddress, err)
	}
	if !pairUsesWrappedNative(token0, token1, wrappedNativeAddress) {
		return nil, fmt.Errorf("%w: %s", errPairMissingWrapped, pairAddress.Hex())
	}

	targetToken, _, _ := selectTargetToken(token0, token1, big.NewInt(0), big.NewInt(0), wrappedNativeAddress)
	decimals := fetchTokenDecimals(client, ctx, targetToken, 18)
	wrappedNativeUSDPrice := resolveWrappedNativeUSDPrice(ctx, client, wrappedNativeAddress)

	return &TransactionStreamConfig{
		TokenAddressHex:         targetToken.Hex(),
		PairAddressHex:          pairAddress.Hex(),
		WrappedNativeAddressHex: wrappedNativeAddress.Hex(),
		WrappedIsToken0:         token0 == wrappedNativeAddress,
		TokenDecimals:           decimals,
		WrappedNativeUSDPrice:   wrappedNativeUSDPrice,
	}, nil
}

func ResolvePairAddress(ctx context.Context, client *ethclient.Client, inputAddress, wrappedNativeAddress common.Address) (common.Address, error) {
	contractAbi, err := abi.JSON(strings.NewReader(UniswapV2PairABI))
	if err != nil {
		return common.Address{}, fmt.Errorf("parse pair ABI: %w", err)
	}

	if _, _, err := getPairTokens(ctx, client, contractAbi, inputAddress); err == nil {
		return inputAddress, nil
	} else if !errors.Is(err, errEmptyContractResponse) {
		return common.Address{}, diagnosePairAddress(ctx, client, inputAddress, err)
	}

	hasCode, err := contractHasCode(ctx, client, inputAddress)
	if err != nil {
		return common.Address{}, fmt.Errorf("inspect contract code for %s: %w", inputAddress.Hex(), err)
	}
	if !hasCode {
		network := ConnectedNetworkLabel(ctx, client)
		return common.Address{}, fmt.Errorf("%w %s on network %q", errNoContractCode, inputAddress.Hex(), network)
	}
	if !looksLikeERC20(ctx, client, inputAddress) {
		return common.Address{}, fmt.Errorf("%w: %s", errUnsupportedPair, inputAddress.Hex())
	}

	pairAddresses, err := findWrappedNativePairs(ctx, client, inputAddress, wrappedNativeAddress)
	if err != nil {
		return common.Address{}, err
	}
	if len(pairAddresses) == 0 {
		return common.Address{}, fmt.Errorf("%w: %s", errNoWrappedNativePair, inputAddress.Hex())
	}

	return chooseBestWrappedNativePair(ctx, client, pairAddresses, wrappedNativeAddress)
}

func GetMetadata(ctx context.Context, client *ethclient.Client, pairAddress, wrappedNativeAddress common.Address) (*TokenMetadata, error) {
	pairMetadataCache.mu.RLock()
	cachedMetadata, ok := pairMetadataCache.entries[pairAddress]
	pairMetadataCache.mu.RUnlock()
	if ok && time.Since(cachedMetadata.CachedAt) < 1*time.Minute {
		log.Printf("[metadata] record hit pair=%s symbol=%s", pairAddress.Hex(), cachedMetadata.Symbol)
		return cachedMetadata, nil
	}

	startedAt := time.Now()

	contractAbi, err := abi.JSON(strings.NewReader(UniswapV2PairABI))
	if err != nil {
		return nil, fmt.Errorf("parse pair ABI: %w", err)
	}

	token0, token1, err := getPairTokens(ctx, client, contractAbi, pairAddress)
	if err != nil {
		return nil, diagnosePairAddress(ctx, client, pairAddress, err)
	}
	if !pairUsesWrappedNative(token0, token1, wrappedNativeAddress) {
		return nil, fmt.Errorf("%w: %s", errPairMissingWrapped, pairAddress.Hex())
	}

	reserveData, err := contractAbi.Pack("getReserves")
	if err != nil {
		return nil, fmt.Errorf("pack getReserves call: %w", err)
	}

	reservesRes, err := callContract(ctx, client, pairAddress, reserveData)
	if err != nil {
		if errors.Is(err, errEmptyContractResponse) {
			return nil, fmt.Errorf("%w: %s", errUnsupportedPair, pairAddress.Hex())
		}
		return nil, fmt.Errorf("call getReserves on %s: %w", pairAddress.Hex(), err)
	}

	reserve0, reserve1, err := unpackReservesResult(contractAbi, reservesRes)
	if err != nil {
		return nil, fmt.Errorf("%w: %s", errUnsupportedPair, pairAddress.Hex())
	}

	targetToken, wethReserve, tokenReserve := selectTargetToken(token0, token1, reserve0, reserve1, wrappedNativeAddress)

	var (
		name                  string
		symbol                string
		decimals              uint8
		totalSupply           *big.Int
		wrappedNativeUSDPrice *big.Float
		wrappedVolume24h      *big.Float
		txCount               uint64
	)

	var wg sync.WaitGroup
	wg.Add(5)

	go func() {
		defer wg.Done()
		name, symbol, decimals = fetchTokenInfo(client, ctx, targetToken)
	}()

	go func() {
		defer wg.Done()
		totalSupply = getTotalSupply(client, ctx, targetToken)
	}()

	go func() {
		defer wg.Done()
		wrappedNativeUSDPrice = resolveWrappedNativeUSDPrice(ctx, client, wrappedNativeAddress)
	}()

	go func() {
		defer wg.Done()
		wrappedVolume24h = calculateVolume24h(client, ctx, pairAddress, token0, token1, wrappedNativeAddress)
	}()

	go func() {
		defer wg.Done()
		txCount = getTransactionCount(client, ctx, pairAddress)
	}()

	wg.Wait()

	price := calculateTokenPriceInWrapped(wethReserve, tokenReserve, decimals)
	normalizedTotalSupply := formatTokenAmount(totalSupply, decimals)
	priceUSD := new(big.Float).Mul(price, wrappedNativeUSDPrice)
	marketCapUSD := new(big.Float).Mul(priceUSD, normalizedTotalSupply)
	volume24h := new(big.Float).Mul(wrappedVolume24h, wrappedNativeUSDPrice)

	liquidityUSD := new(big.Float).Mul(FormatEther(wethReserve), new(big.Float).Mul(wrappedNativeUSDPrice, big.NewFloat(2)))
	metadata := &TokenMetadata{
		Name:               name,
		Symbol:             symbol,
		Price:              price,
		PriceDisplay:       FormatPriceDisplay(price),
		PriceUSD:           FormatPriceDisplay(priceUSD),
		BaseAddress:        targetToken.String(),
		PairAddress:        pairAddress.String(),
		TotalSupply:        totalSupply,
		TotalSupplyDisplay: FriendlyFormat(normalizedTotalSupply),
		MarketCap:          FriendlyFormat(marketCapUSD),
		Liquidity:          FriendlyFormat(liquidityUSD),
		Volume24h:          FriendlyFormat(volume24h),
		PooledWETH:         FormatEther(wethReserve),
		PooledToken:        tokenReserve,
		TotalTxCount:       txCount,
		IsToken0:           targetToken == token0,
		CachedAt:           time.Now(),
	}

	pairMetadataCache.mu.Lock()
	pairMetadataCache.entries[pairAddress] = metadata
	pairMetadataCache.mu.Unlock()

	log.Printf("Loaded metadata pair=%s token=%s symbol=%s in %s", pairAddress.Hex(), targetToken.Hex(), symbol, time.Since(startedAt))
	return metadata, nil
}

func FriendlyFormat(f *big.Float) string {
	if f == nil {
		return "0.00"
	}

	absF := new(big.Float).Abs(f)
	oneT := big.NewFloat(1e12)
	oneB := big.NewFloat(1e9)
	oneM := big.NewFloat(1e6)
	oneK := big.NewFloat(1000)

	switch {
	case absF.Cmp(oneT) >= 0:
		v, _ := new(big.Float).Quo(f, oneT).Float64()
		return fmt.Sprintf("%.2f T", v)
	case absF.Cmp(oneB) >= 0:
		v, _ := new(big.Float).Quo(f, oneB).Float64()
		return fmt.Sprintf("%.2f B", v)
	case absF.Cmp(oneM) >= 0:
		v, _ := new(big.Float).Quo(f, oneM).Float64()
		return fmt.Sprintf("%.2f M", v)
	case absF.Cmp(oneK) >= 0:
		v, _ := new(big.Float).Quo(f, oneK).Float64()
		return fmt.Sprintf("%.2f K", v)
	default:
		v, _ := f.Float64()
		if v < 0.000001 && v > 0 {
			return f.Text('f', 10)
		}
		return fmt.Sprintf("%.6f", v)
	}
}

func FormatPriceDisplay(price *big.Float) string {
	if price == nil || price.Sign() == 0 {
		return "0.00"
	}

	sign := ""
	if price.Sign() < 0 {
		sign = "-"
	}

	absPrice := new(big.Float).Abs(price)
	if absPrice.Cmp(big.NewFloat(1)) >= 0 {
		return sign + trimTrailingZeroes(absPrice.Text('f', 4))
	}
	if absPrice.Cmp(big.NewFloat(0.0001)) >= 0 {
		return sign + trimTrailingZeroes(absPrice.Text('f', 8))
	}

	decimal := trimTrailingZeroes(absPrice.Text('f', 24))
	if !strings.HasPrefix(decimal, "0.") {
		return sign + decimal
	}

	fraction := strings.TrimPrefix(decimal, "0.")
	leadingZeroes := 0
	for leadingZeroes < len(fraction) && fraction[leadingZeroes] == '0' {
		leadingZeroes++
	}
	if leadingZeroes >= len(fraction) {
		return "0.00"
	}

	significantDigits := strings.TrimRight(fraction[leadingZeroes:min(leadingZeroes+6, len(fraction))], "0")
	if len(significantDigits) == 0 {
		return "0.00"
	}

	return sign + "0.0" + toSubscriptNumber(leadingZeroes) + significantDigits
}

func FormatBigFloat(f *big.Float, precision int) string {
	return f.Text('f', precision)
}

func FormatBigInt(i *big.Int) string {
	return i.String()
}
func FormatEther(wei *big.Int) *big.Float {
	if wei == nil {
		return big.NewFloat(0)
	}
	ether := new(big.Float).Quo(new(big.Float).SetInt(wei), big.NewFloat(1e18))
	return ether
}

func formatTokenAmount(amount *big.Int, decimals uint8) *big.Float {
	if amount == nil {
		return big.NewFloat(0)
	}

	scale := new(big.Float).SetInt(new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(decimals)), nil))
	return new(big.Float).Quo(new(big.Float).SetInt(amount), scale)
}

func calculateTokenPriceInWrapped(wrappedReserve, tokenReserve *big.Int, tokenDecimals uint8) *big.Float {
	if wrappedReserve == nil || tokenReserve == nil || tokenReserve.Sign() <= 0 {
		return big.NewFloat(0)
	}

	normalizedWrappedReserve := formatTokenAmount(wrappedReserve, 18)
	normalizedTokenReserve := formatTokenAmount(tokenReserve, tokenDecimals)
	if normalizedTokenReserve.Sign() <= 0 {
		return big.NewFloat(0)
	}

	return new(big.Float).Quo(normalizedWrappedReserve, normalizedTokenReserve)
}

func resolveWrappedNativeUSDPrice(ctx context.Context, client *ethclient.Client, wrappedNativeAddress common.Address) *big.Float {
	envValue := strings.TrimSpace(os.Getenv("WRAPPED_NATIVE_USD_PRICE"))
	if envValue != "" {
		if parsedValue, _, err := big.ParseFloat(envValue, 10, 256, big.ToNearestEven); err == nil && parsedValue.Sign() > 0 {
			return parsedValue
		}
		log.Printf("Ignoring invalid WRAPPED_NATIVE_USD_PRICE value %q", envValue)
	}

	chainID, err := client.ChainID(ctx)
	if err != nil {
		log.Printf("Failed to resolve chain ID for wrapped native USD price: %v", err)
		return big.NewFloat(0)
	}

	cacheKey := chainID.String() + ":" + strings.ToLower(wrappedNativeAddress.Hex())
	if cachedPrice, ok := loadCachedWrappedNativeUSDPrice(cacheKey); ok {
		return cachedPrice
	}

	if pairAddress, stableAddress, ok := wrappedNativeUSDDirectQuoteSource(chainID.String()); ok {
		price, err := wrappedNativeUSDPriceFromPair(ctx, client, pairAddress, stableAddress, wrappedNativeAddress)
		if err != nil {
			log.Printf("Failed to price wrapped native from direct pair %s: %v", pairAddress.Hex(), err)
		} else if price.Sign() > 0 {
			storeCachedWrappedNativeUSDPrice(cacheKey, price)
			return price
		}
	}

	for _, stableAddress := range stablecoinCandidates(chainID.String()) {
		pairAddresses, err := findWrappedNativePairs(ctx, client, stableAddress, wrappedNativeAddress)
		if err != nil {
			log.Printf("Failed to find wrapped-native USD pairs for %s: %v", stableAddress.Hex(), err)
			continue
		}
		if len(pairAddresses) == 0 {
			continue
		}

		pairAddress, err := chooseBestWrappedNativePair(ctx, client, pairAddresses, wrappedNativeAddress)
		if err != nil {
			log.Printf("Failed to choose wrapped-native USD pair for %s: %v", stableAddress.Hex(), err)
			continue
		}

		price, err := wrappedNativeUSDPriceFromPair(ctx, client, pairAddress, stableAddress, wrappedNativeAddress)
		if err != nil {
			log.Printf("Failed to price wrapped native from pair %s: %v", pairAddress.Hex(), err)
			continue
		}
		if price.Sign() > 0 {
			storeCachedWrappedNativeUSDPrice(cacheKey, price)
			return price
		}
	}

	return big.NewFloat(0)
}

func WarmWrappedNativeUSDPriceCache(ctx context.Context, client *ethclient.Client, wrappedNativeAddress common.Address) *big.Float {
	return resolveWrappedNativeUSDPrice(ctx, client, wrappedNativeAddress)
}

func wrappedNativeUSDDirectQuoteSource(chainID string) (common.Address, common.Address, bool) {
	pairFromEnv := strings.TrimSpace(os.Getenv("WRAPPED_NATIVE_USD_PAIR_ADDRESS"))
	stableFromEnv := strings.TrimSpace(os.Getenv("WRAPPED_NATIVE_USD_STABLE_ADDRESS"))
	if common.IsHexAddress(pairFromEnv) && common.IsHexAddress(stableFromEnv) {
		return common.HexToAddress(pairFromEnv), common.HexToAddress(stableFromEnv), true
	}
	if pairFromEnv != "" || stableFromEnv != "" {
		log.Printf("Ignoring invalid direct wrapped native USD quote source pair=%q stable=%q", pairFromEnv, stableFromEnv)
	}

	source, ok := wrappedNativeUSDQuoteSourcesByChainID[chainID]
	if !ok {
		return common.Address{}, common.Address{}, false
	}
	if !common.IsHexAddress(source.pairAddress) || !common.IsHexAddress(source.stableAddress) {
		return common.Address{}, common.Address{}, false
	}

	return common.HexToAddress(source.pairAddress), common.HexToAddress(source.stableAddress), true
}

func stablecoinCandidates(chainID string) []common.Address {
	envValue := strings.TrimSpace(os.Getenv("USD_STABLE_ADDRESS"))
	if envValue != "" {
		candidates := make([]common.Address, 0)
		for _, rawAddress := range strings.Split(envValue, ",") {
			trimmed := strings.TrimSpace(rawAddress)
			if !common.IsHexAddress(trimmed) {
				log.Printf("Ignoring invalid USD_STABLE_ADDRESS entry %q", trimmed)
				continue
			}
			candidates = append(candidates, common.HexToAddress(trimmed))
		}
		if len(candidates) > 0 {
			return candidates
		}
	}

	rawCandidates := stablecoinAddressesByChainID[chainID]
	candidates := make([]common.Address, 0, len(rawCandidates))
	for _, rawAddress := range rawCandidates {
		candidates = append(candidates, common.HexToAddress(rawAddress))
	}

	return candidates
}

func wrappedNativeUSDPriceFromPair(ctx context.Context, client *ethclient.Client, pairAddress, stableAddress, wrappedNativeAddress common.Address) (*big.Float, error) {
	contractAbi, err := abi.JSON(strings.NewReader(UniswapV2PairABI))
	if err != nil {
		return nil, fmt.Errorf("parse pair ABI: %w", err)
	}

	token0, token1, err := getPairTokens(ctx, client, contractAbi, pairAddress)
	if err != nil {
		return nil, err
	}

	reserveData, err := contractAbi.Pack("getReserves")
	if err != nil {
		return nil, fmt.Errorf("pack getReserves call: %w", err)
	}

	reservesRes, err := callContract(ctx, client, pairAddress, reserveData)
	if err != nil {
		return nil, err
	}

	reserve0, reserve1, err := unpackReservesResult(contractAbi, reservesRes)
	if err != nil {
		return nil, err
	}

	var (
		stableReserve  *big.Int
		wrappedReserve *big.Int
	)
	switch {
	case token0 == stableAddress && token1 == wrappedNativeAddress:
		stableReserve = reserve0
		wrappedReserve = reserve1
	case token0 == wrappedNativeAddress && token1 == stableAddress:
		wrappedReserve = reserve0
		stableReserve = reserve1
	default:
		return nil, fmt.Errorf("pair %s does not match stable %s and wrapped native %s", pairAddress.Hex(), stableAddress.Hex(), wrappedNativeAddress.Hex())
	}

	stableDecimals := fetchTokenDecimals(client, ctx, stableAddress, 6)
	normalizedStableReserve := formatTokenAmount(stableReserve, stableDecimals)
	normalizedWrappedReserve := formatTokenAmount(wrappedReserve, 18)
	if normalizedWrappedReserve.Sign() <= 0 {
		return big.NewFloat(0), nil
	}

	return new(big.Float).Quo(normalizedStableReserve, normalizedWrappedReserve), nil
}

func loadCachedWrappedNativeUSDPrice(cacheKey string) (*big.Float, bool) {
	wrappedNativeUSDPriceCache.mu.RLock()
	entry, ok := wrappedNativeUSDPriceCache.entries[cacheKey]
	wrappedNativeUSDPriceCache.mu.RUnlock()
	if !ok || time.Now().After(entry.expiresAt) || entry.price == nil || entry.price.Sign() <= 0 {
		return nil, false
	}

	return new(big.Float).Set(entry.price), true
}

func storeCachedWrappedNativeUSDPrice(cacheKey string, price *big.Float) {
	if price == nil || price.Sign() <= 0 {
		return
	}

	wrappedNativeUSDPriceCache.mu.Lock()
	wrappedNativeUSDPriceCache.entries[cacheKey] = cachedWrappedNativeUSDPrice{
		price:     new(big.Float).Set(price),
		expiresAt: time.Now().Add(wrappedNativeUSDPriceCacheTTL),
	}
	wrappedNativeUSDPriceCache.mu.Unlock()
}

func trimTrailingZeroes(value string) string {
	value = strings.TrimRight(value, "0")
	value = strings.TrimRight(value, ".")
	if value == "" || value == "-" {
		return "0"
	}
	return value
}

func toSubscriptNumber(value int) string {
	if value == 0 {
		return "₀"
	}

	digits := []rune("₀₁₂₃₄₅₆₇₈₉")
	normal := fmt.Sprintf("%d", value)
	var builder strings.Builder
	builder.Grow(len(normal) * 3)
	for _, char := range normal {
		if char < '0' || char > '9' {
			continue
		}
		builder.WriteRune(digits[char-'0'])
	}

	return builder.String()
}

func callContract(ctx context.Context, client *ethclient.Client, contractAddress common.Address, data []byte) ([]byte, error) {
	res, err := client.CallContract(ctx, ethereum.CallMsg{
		To:   &contractAddress,
		Data: data,
	}, nil)
	if err != nil {
		return nil, err
	}
	if len(res) == 0 {
		return nil, errEmptyContractResponse
	}

	return res, nil
}

func diagnosePairAddress(ctx context.Context, client *ethclient.Client, address common.Address, pairErr error) error {
	hasCode, err := contractHasCode(ctx, client, address)
	if err != nil {
		return fmt.Errorf("inspect contract code for %s: %w", address.Hex(), err)
	}
	if !hasCode {
		network := ConnectedNetworkLabel(ctx, client)
		return fmt.Errorf("%w %s on network %q", errNoContractCode, address.Hex(), network)
	}

	if errors.Is(pairErr, errEmptyContractResponse) {
		if looksLikeERC20(ctx, client, address) {
			return fmt.Errorf("%w: %s", errTokenAddressInput, address.Hex())
		}

		return fmt.Errorf("%w: %s", errUnsupportedPair, address.Hex())
	}

	return fmt.Errorf("load pair metadata for %s: %w", address.Hex(), pairErr)
}

func findWrappedNativePairs(ctx context.Context, client *ethclient.Client, tokenAddress, wrappedNativeAddress common.Address) ([]common.Address, error) {
	if tokenAddress == wrappedNativeAddress {
		return nil, nil
	}

	pairCreatedTopic := crypto.Keccak256Hash([]byte("PairCreated(address,address,address,uint256)"))
	forwardLogs, err := filterLogsWithChunkFallback(ctx, client, ethereum.FilterQuery{
		Topics: [][]common.Hash{
			{pairCreatedTopic},
			{indexedAddressTopic(tokenAddress)},
			{indexedAddressTopic(wrappedNativeAddress)},
		},
	})
	if err != nil {
		return nil, fmt.Errorf("find wrapped-native pools for %s: %w", tokenAddress.Hex(), err)
	}

	reverseLogs, err := filterLogsWithChunkFallback(ctx, client, ethereum.FilterQuery{
		Topics: [][]common.Hash{
			{pairCreatedTopic},
			{indexedAddressTopic(wrappedNativeAddress)},
			{indexedAddressTopic(tokenAddress)},
		},
	})
	if err != nil {
		return nil, fmt.Errorf("find wrapped-native pools for %s: %w", tokenAddress.Hex(), err)
	}

	seen := make(map[common.Address]struct{})
	pairAddresses := make([]common.Address, 0, len(forwardLogs)+len(reverseLogs))
	for _, eventLog := range append(forwardLogs, reverseLogs...) {
		pairAddress, err := pairAddressFromPairCreatedLog(eventLog)
		if err != nil {
			log.Printf("Skipping malformed PairCreated log for %s: %v", tokenAddress.Hex(), err)
			continue
		}
		if _, ok := seen[pairAddress]; ok {
			continue
		}
		seen[pairAddress] = struct{}{}
		pairAddresses = append(pairAddresses, pairAddress)
	}

	return pairAddresses, nil
}

func filterLogsWithChunkFallback(ctx context.Context, client *ethclient.Client, query ethereum.FilterQuery) ([]types.Log, error) {
	currentBlock, err := client.BlockNumber(ctx)
	if err != nil {
		return nil, fmt.Errorf("get current block: %w", err)
	}

	fullRangeQuery := query
	fullRangeQuery.FromBlock = big.NewInt(0)
	fullRangeQuery.ToBlock = big.NewInt(int64(currentBlock))

	logs, err := client.FilterLogs(ctx, fullRangeQuery)
	if err == nil {
		return logs, nil
	}

	const chunkSize uint64 = 1_000_000
	allLogs := make([]types.Log, 0)
	for start := uint64(0); start <= currentBlock; start += chunkSize {
		end := start + chunkSize - 1
		if end > currentBlock {
			end = currentBlock
		}

		chunkQuery := query
		chunkQuery.FromBlock = big.NewInt(int64(start))
		chunkQuery.ToBlock = big.NewInt(int64(end))

		chunkLogs, chunkErr := client.FilterLogs(ctx, chunkQuery)
		if chunkErr != nil {
			return nil, fmt.Errorf("filter logs %d-%d: %w", start, end, chunkErr)
		}
		allLogs = append(allLogs, chunkLogs...)
	}

	return allLogs, nil
}

func chooseBestWrappedNativePair(ctx context.Context, client *ethclient.Client, pairAddresses []common.Address, wrappedNativeAddress common.Address) (common.Address, error) {
	if len(pairAddresses) == 1 {
		return pairAddresses[0], nil
	}

	contractAbi, err := abi.JSON(strings.NewReader(UniswapV2PairABI))
	if err != nil {
		return common.Address{}, fmt.Errorf("parse pair ABI: %w", err)
	}

	reserveData, err := contractAbi.Pack("getReserves")
	if err != nil {
		return common.Address{}, fmt.Errorf("pack getReserves call: %w", err)
	}

	var (
		bestPair           common.Address
		bestWrappedReserve = big.NewInt(-1)
	)

	for _, pairAddress := range pairAddresses {
		token0, token1, err := getPairTokens(ctx, client, contractAbi, pairAddress)
		if err != nil || !pairUsesWrappedNative(token0, token1, wrappedNativeAddress) {
			continue
		}

		reservesRes, err := callContract(ctx, client, pairAddress, reserveData)
		if err != nil {
			continue
		}

		reserve0, reserve1, err := unpackReservesResult(contractAbi, reservesRes)
		if err != nil {
			continue
		}

		_, wrappedReserve, _ := selectTargetToken(token0, token1, reserve0, reserve1, wrappedNativeAddress)
		if bestPair == (common.Address{}) || wrappedReserve.Cmp(bestWrappedReserve) > 0 {
			bestPair = pairAddress
			bestWrappedReserve = new(big.Int).Set(wrappedReserve)
		}
	}

	if bestPair != (common.Address{}) {
		return bestPair, nil
	}

	return pairAddresses[0], nil
}

func indexedAddressTopic(address common.Address) common.Hash {
	return common.BytesToHash(common.LeftPadBytes(address.Bytes(), common.HashLength))
}

func pairAddressFromPairCreatedLog(eventLog types.Log) (common.Address, error) {
	if len(eventLog.Data) < 32 {
		return common.Address{}, fmt.Errorf("expected at least 32 bytes of event data, got %d", len(eventLog.Data))
	}

	return common.BytesToAddress(eventLog.Data[12:32]), nil
}

func contractHasCode(ctx context.Context, client *ethclient.Client, address common.Address) (bool, error) {
	code, err := client.CodeAt(ctx, address, nil)
	if err != nil {
		return false, err
	}

	return len(code) > 0, nil
}

func looksLikeERC20(ctx context.Context, client *ethclient.Client, address common.Address) bool {
	tokenABI, err := abi.JSON(strings.NewReader(erc20ABI))
	if err != nil {
		log.Printf("ERC20 ABI parse failed while classifying %s: %v", address.Hex(), err)
		return false
	}

	methods := []string{"decimals", "totalSupply"}
	for _, method := range methods {
		data, err := tokenABI.Pack(method)
		if err != nil {
			continue
		}

		if _, err := callContract(ctx, client, address, data); err == nil {
			return true
		}
	}

	return false
}

func networkLabelForChainID(chainID string) string {
	if name, ok := networkNamesByChainID[chainID]; ok {
		return name
	}
	if chainID == "" {
		return "the connected network"
	}

	return fmt.Sprintf("chain ID %s", chainID)
}

func pairUsesWrappedNative(token0, token1, wrappedNativeAddress common.Address) bool {
	return token0 == wrappedNativeAddress || token1 == wrappedNativeAddress
}

func getPairTokens(ctx context.Context, client *ethclient.Client, contractAbi abi.ABI, pairAddress common.Address) (common.Address, common.Address, error) {
	pairTokensCache.mu.RLock()
	cached, ok := pairTokensCache.entries[pairAddress]
	pairTokensCache.mu.RUnlock()
	if ok && time.Now().Before(cached.expiresAt) {
		return cached.token0, cached.token1, nil
	}

	data0, err := contractAbi.Pack("token0")
	if err != nil {
		return common.Address{}, common.Address{}, fmt.Errorf("pack token0 call: %w", err)
	}

	res0, err := callContract(ctx, client, pairAddress, data0)
	if err != nil {
		return common.Address{}, common.Address{}, fmt.Errorf("call token0 on %s: %w", pairAddress.Hex(), err)
	}

	token0, err := unpackAddressResult(contractAbi, "token0", res0)
	if err != nil {
		return common.Address{}, common.Address{}, fmt.Errorf("decode token0 for %s: %w", pairAddress.Hex(), err)
	}

	data1, err := contractAbi.Pack("token1")
	if err != nil {
		return common.Address{}, common.Address{}, fmt.Errorf("pack token1 call: %w", err)
	}

	res1, err := callContract(ctx, client, pairAddress, data1)
	if err != nil {
		return common.Address{}, common.Address{}, fmt.Errorf("call token1 on %s: %w", pairAddress.Hex(), err)
	}

	token1, err := unpackAddressResult(contractAbi, "token1", res1)
	if err != nil {
		return common.Address{}, common.Address{}, fmt.Errorf("decode token1 for %s: %w", pairAddress.Hex(), err)
	}

	pairTokensCache.mu.Lock()
	pairTokensCache.entries[pairAddress] = cachedPairTokens{
		token0:    token0,
		token1:    token1,
		expiresAt: time.Now().Add(24 * time.Hour),
	}
	pairTokensCache.mu.Unlock()

	return token0, token1, nil
}

func selectTargetToken(token0, token1 common.Address, reserve0, reserve1 *big.Int, wrappedNativeAddress common.Address) (common.Address, *big.Int, *big.Int) {
	if token0 == wrappedNativeAddress {
		return token1, reserve0, reserve1
	}
	if token1 == wrappedNativeAddress {
		return token0, reserve1, reserve0
	}

	return token0, reserve1, reserve0
}

func unpackAddressResult(contractAbi abi.ABI, method string, raw []byte) (common.Address, error) {
	values, err := contractAbi.Unpack(method, raw)
	if err != nil {
		return common.Address{}, err
	}
	if len(values) != 1 {
		return common.Address{}, fmt.Errorf("expected 1 result, got %d", len(values))
	}

	address, ok := values[0].(common.Address)
	if !ok {
		return common.Address{}, fmt.Errorf("unexpected %s result type %T", method, values[0])
	}

	return address, nil
}

func unpackStringResult(contractAbi abi.ABI, method string, raw []byte) (string, error) {
	values, err := contractAbi.Unpack(method, raw)
	if err != nil {
		return "", err
	}
	if len(values) != 1 {
		return "", fmt.Errorf("expected 1 result, got %d", len(values))
	}

	value, ok := values[0].(string)
	if !ok {
		return "", fmt.Errorf("unexpected %s result type %T", method, values[0])
	}

	return value, nil
}

func unpackUint8Result(contractAbi abi.ABI, method string, raw []byte) (uint8, error) {
	values, err := contractAbi.Unpack(method, raw)
	if err != nil {
		return 0, err
	}
	if len(values) != 1 {
		return 0, fmt.Errorf("expected 1 result, got %d", len(values))
	}

	value, ok := values[0].(uint8)
	if !ok {
		return 0, fmt.Errorf("unexpected %s result type %T", method, values[0])
	}

	return value, nil
}

func unpackReservesResult(contractAbi abi.ABI, raw []byte) (*big.Int, *big.Int, error) {
	values, err := contractAbi.Unpack("getReserves", raw)
	if err != nil {
		return nil, nil, err
	}
	if len(values) < 2 {
		return nil, nil, fmt.Errorf("expected at least 2 reserve values, got %d", len(values))
	}

	reserve0, ok := values[0].(*big.Int)
	if !ok {
		return nil, nil, fmt.Errorf("unexpected reserve0 type %T", values[0])
	}

	reserve1, ok := values[1].(*big.Int)
	if !ok {
		return nil, nil, fmt.Errorf("unexpected reserve1 type %T", values[1])
	}

	return reserve0, reserve1, nil
}
