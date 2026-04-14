package token

import (
	"math/big"
	"strings"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/common"
)

const maxCachedTransactions = 1000

type cachedWrappedNativeUSDPrice struct {
	price     *big.Float
	expiresAt time.Time
}

type cachedPairTokens struct {
	token0    common.Address
	token1    common.Address
	expiresAt time.Time
}

type cachedTokenDecimals struct {
	decimals  uint8
	expiresAt time.Time
}

var (
	wrappedNativeUSDPriceCache = struct {
		mu      sync.RWMutex
		entries map[string]cachedWrappedNativeUSDPrice
	}{
		entries: make(map[string]cachedWrappedNativeUSDPrice),
	}

	pairTokensCache = struct {
		mu      sync.RWMutex
		entries map[common.Address]cachedPairTokens
	}{
		entries: make(map[common.Address]cachedPairTokens),
	}

	tokenDecimalsCache = struct {
		mu      sync.RWMutex
		entries map[common.Address]cachedTokenDecimals
	}{
		entries: make(map[common.Address]cachedTokenDecimals),
	}

	pairMetadataCache = struct {
		mu      sync.RWMutex
		entries map[common.Address]*TokenMetadata
	}{
		entries: make(map[common.Address]*TokenMetadata),
	}

	blockTimestampCache = struct {
		mu      sync.RWMutex
		entries map[uint64]int64
	}{
		entries: make(map[uint64]int64),
	}

	transactionCache = struct {
		mu      sync.RWMutex
		entries map[string][]Tx
	}{
		entries: make(map[string][]Tx),
	}

	candleCache = struct {
		mu      sync.RWMutex
		entries map[string][]ChartCandle
	}{
		entries: make(map[string][]ChartCandle),
	}
)

func AddToTransactionCache(pairAddress string, tx Tx) {
	transactionCache.mu.Lock()
	defer transactionCache.mu.Unlock()

	history := transactionCache.entries[pairAddress]
	history = append([]Tx{tx}, history...)
	if len(history) > maxCachedTransactions {
		history = history[:maxCachedTransactions]
	}
	transactionCache.entries[pairAddress] = history
}

func SeedTransactionCache(pairAddress string, txs []Tx) {
	transactionCache.mu.Lock()
	defer transactionCache.mu.Unlock()

	history := append([]Tx(nil), txs...)
	if len(history) > maxCachedTransactions {
		history = history[:maxCachedTransactions]
	}
	transactionCache.entries[pairAddress] = history
}

func UpdateCandleCache(pairAddress string, tx Tx) {
	candleCache.mu.Lock()
	defer candleCache.mu.Unlock()

	price, ok := big.NewFloat(0).SetString(tx.Price)
	if !ok {
		return
	}
	priceF, _ := price.Float64()
	if !isValidChartPrice(priceF) {
		return
	}

	candles := candleCache.entries[pairAddress]
	bucketTime := chartBucketStart(tx.TimeUnix, chartCandleIntervalSeconds)
	candles = upsertChartCandle(candles, bucketTime, priceF, tx.Volume, chartCandleIntervalSeconds)

	if len(candles) > maxCachedChartCandles {
		candles = candles[len(candles)-maxCachedChartCandles:]
	}
	candleCache.entries[pairAddress] = candles
}

func GetCachedTransactions(pairAddress string) []Tx {
	transactionCache.mu.RLock()
	defer transactionCache.mu.RUnlock()
	return append([]Tx(nil), transactionCache.entries[pairAddress]...)
}

func GetCachedCandles(pairAddress string) []ChartCandle {
	candleCache.mu.RLock()
	defer candleCache.mu.RUnlock()
	return append([]ChartCandle(nil), candleCache.entries[pairAddress]...)
}

func SeedCandleCache(pairAddress string, candles []ChartCandle) {
	candleCache.mu.Lock()
	defer candleCache.mu.Unlock()

	cached := append([]ChartCandle(nil), candles...)
	if len(cached) > maxCachedChartCandles {
		cached = cached[len(cached)-maxCachedChartCandles:]
	}
	candleCache.entries[pairAddress] = cached
}

func UpdateMetadataPrice(pairAddress string, tx Tx) {
	pairMetadataCache.mu.Lock()
	defer pairMetadataCache.mu.Unlock()

	metadata, ok := pairMetadataCache.entries[common.HexToAddress(pairAddress)]
	if !ok {
		return
	}

	price, okFloat := big.NewFloat(0).SetString(tx.Price)
	if !okFloat {
		return
	}

	metadata.Price = price
	metadata.PriceDisplay = tx.PriceDisplay

	cacheKey := "1:" + strings.ToLower(defaultWETHAddress.Hex())
	nativePrice, exists := loadCachedWrappedNativeUSDPrice(cacheKey)
	if !exists {
		nativePrice, exists = loadCachedWrappedNativeUSDPriceForAddress(defaultWETHAddress)
	}
	if exists {
		priceUSD := new(big.Float).Mul(price, nativePrice)
		metadata.PriceUSD = FormatPriceDisplay(priceUSD)
	}

	metadata.CachedAt = time.Now()
}
