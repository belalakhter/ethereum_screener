package token

import (
	"context"
	"fmt"
	"math"
	"math/big"
	"sort"
	"strings"
	"time"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
	gethtypes "github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethclient"
)

type ChartCandle struct {
	Time   int64   `json:"time"`
	Open   float64 `json:"open"`
	High   float64 `json:"high"`
	Low    float64 `json:"low"`
	Close  float64 `json:"close"`
	Volume float64 `json:"volume"`
}

type chartSwapPoint struct {
	Time   int64
	Price  float64
	Volume float64
}

const (
	defaultChartWindowSeconds  int64  = 48 * 60 * 60
	chartCandleIntervalSeconds int64  = 10 * 60
	defaultChartLookbackBlocks uint64 = 15000
	maxChartHistoryScanBlocks  uint64 = 300000
	maxChartHistoryLogs               = 1200
	maxCachedChartCandles             = 1000
)

func GetRecentChartCandles(ctx context.Context, client *ethclient.Client, config *TransactionStreamConfig, windowSeconds int64, lookbackBlocks uint64) ([]ChartCandle, error) {
	if windowSeconds <= 0 {
		windowSeconds = defaultChartWindowSeconds
	}
	if lookbackBlocks == 0 {
		lookbackBlocks = defaultChartLookbackBlocks
	}

	currentBlock, err := client.BlockNumber(ctx)
	if err != nil {
		return nil, fmt.Errorf("get current block for chart candles: %w", err)
	}

	contractABI, err := abi.JSON(strings.NewReader(UniswapV2PairABI))
	if err != nil {
		return nil, fmt.Errorf("parse pair ABI for chart candles: %w", err)
	}

	swapEventID := contractABI.Events["Swap"].ID
	logs, err := loadChartSwapLogs(ctx, client, common.HexToAddress(config.PairAddressHex), swapEventID, currentBlock, lookbackBlocks)
	if err != nil {
		return nil, err
	}
	if len(logs) == 0 {
		return []ChartCandle{}, nil
	}

	sortChartLogs(logs)

	nowUnix := time.Now().Unix()
	blockTimestamps := estimateChartBlockTimestamps(logs, currentBlock, nowUnix)

	currentWindowStart, currentWindowEnd := chartWindowBounds(windowSeconds, chartCandleIntervalSeconds, nowUnix)
	allPoints := make([]chartSwapPoint, 0, len(logs))
	for _, logItem := range logs {
		price, volume, ok := chartPointFromSwapLogData(config, logItem.Data)
		if !ok {
			continue
		}

		timestamp, ok := blockTimestamps[logItem.BlockNumber]
		if !ok {
			continue
		}

		allPoints = append(allPoints, chartSwapPoint{
			Time:   timestamp,
			Price:  price,
			Volume: volume,
		})
	}
	if len(allPoints) == 0 {
		return []ChartCandle{}, nil
	}

	windowStart, windowEnd := activeChartWindow(allPoints, currentWindowStart, currentWindowEnd, windowSeconds, chartCandleIntervalSeconds)
	points, seedClose, hasSeedClose := pointsInChartWindow(allPoints, windowStart, windowEnd, chartCandleIntervalSeconds)

	candles := buildChartCandlesFromPoints(points, windowStart, windowEnd, chartCandleIntervalSeconds, seedClose, hasSeedClose)

	if len(candles) > 0 {
		SeedCandleCache(config.PairAddressHex, candles)
	}

	return candles, nil
}

func ChartCandlesFromTransactions(txs []Tx, windowSeconds int64, nowUnix int64) []ChartCandle {
	if len(txs) == 0 {
		return []ChartCandle{}
	}
	if windowSeconds <= 0 {
		windowSeconds = defaultChartWindowSeconds
	}
	if nowUnix <= 0 {
		nowUnix = time.Now().Unix()
	}

	points := make([]chartSwapPoint, 0, len(txs))
	for _, tx := range txs {
		point, ok := chartPointFromTx(tx)
		if !ok {
			continue
		}
		points = append(points, point)
	}
	if len(points) == 0 {
		return []ChartCandle{}
	}

	currentWindowStart, currentWindowEnd := chartWindowBounds(windowSeconds, chartCandleIntervalSeconds, nowUnix)
	windowStart, windowEnd := activeChartWindow(points, currentWindowStart, currentWindowEnd, windowSeconds, chartCandleIntervalSeconds)
	windowPoints, seedClose, hasSeedClose := pointsInChartWindow(points, windowStart, windowEnd, chartCandleIntervalSeconds)
	candles := buildChartCandlesFromPoints(windowPoints, windowStart, windowEnd, chartCandleIntervalSeconds, seedClose, hasSeedClose)

	return trimChartCandles(candles)
}

func SeedCandleCacheFromTransactions(pairAddress string, txs []Tx, windowSeconds int64, nowUnix int64) []ChartCandle {
	candles := ChartCandlesFromTransactions(txs, windowSeconds, nowUnix)
	if len(candles) > 0 {
		SeedCandleCache(pairAddress, candles)
	}

	return candles
}

func GetCachedChartCandlesThrough(pairAddress string, targetTime int64) []ChartCandle {
	candles := FillChartCandlesThrough(GetCachedCandles(pairAddress), targetTime)
	if len(candles) > 0 {
		SeedCandleCache(pairAddress, candles)
	}

	return candles
}

func FillChartCandlesThrough(candles []ChartCandle, targetTime int64) []ChartCandle {
	if len(candles) == 0 {
		return []ChartCandle{}
	}
	if targetTime <= 0 {
		targetTime = time.Now().Unix()
	}

	filled := append([]ChartCandle(nil), candles...)
	sort.SliceStable(filled, func(i, j int) bool {
		return filled[i].Time < filled[j].Time
	})

	targetBucket := chartBucketStart(targetTime, chartCandleIntervalSeconds)
	lastTime := filled[len(filled)-1].Time
	if targetBucket <= lastTime {
		return trimChartCandles(filled)
	}

	maxTargetBucket := lastTime + int64(maxCachedChartCandles)*chartCandleIntervalSeconds
	if targetBucket > maxTargetBucket {
		targetBucket = maxTargetBucket
	}

	filled = fillFlatCandlesThrough(filled, targetBucket, chartCandleIntervalSeconds)
	return trimChartCandles(filled)
}

func loadChartSwapLogs(ctx context.Context, client *ethclient.Client, pairAddress common.Address, swapEventID common.Hash, currentBlock, chunkBlocks uint64) ([]gethtypes.Log, error) {
	if chunkBlocks == 0 {
		chunkBlocks = defaultChartLookbackBlocks
	}

	initialFromBlock := chartFromBlock(currentBlock, chunkBlocks)
	logs, err := filterChartSwapLogs(ctx, client, pairAddress, swapEventID, initialFromBlock, currentBlock)
	if err != nil {
		return nil, err
	}
	if len(logs) > 0 || initialFromBlock == 0 {
		return logs, nil
	}

	maxScanBlocks := maxChartHistoryScanBlocks
	if chunkBlocks > maxScanBlocks {
		maxScanBlocks = chunkBlocks
	}

	scannedBlocks := currentBlock - initialFromBlock + 1
	toBlock := initialFromBlock - 1
	for scannedBlocks < maxScanBlocks {
		remainingBlocks := maxScanBlocks - scannedBlocks
		rangeBlocks := chunkBlocks
		if rangeBlocks > remainingBlocks {
			rangeBlocks = remainingBlocks
		}

		fromBlock := chartFromBlock(toBlock, rangeBlocks)
		chunkLogs, err := filterChartSwapLogs(ctx, client, pairAddress, swapEventID, fromBlock, toBlock)
		if err != nil {
			return nil, err
		}
		if len(chunkLogs) > 0 {
			logs = append(logs, chunkLogs...)
			break
		}
		if fromBlock == 0 {
			break
		}

		scannedBlocks += toBlock - fromBlock + 1
		toBlock = fromBlock - 1
	}

	sortChartLogs(logs)
	if len(logs) > maxChartHistoryLogs {
		logs = logs[len(logs)-maxChartHistoryLogs:]
	}

	return logs, nil
}

func filterChartSwapLogs(ctx context.Context, client *ethclient.Client, pairAddress common.Address, swapEventID common.Hash, fromBlock, toBlock uint64) ([]gethtypes.Log, error) {
	query := ethereum.FilterQuery{
		FromBlock: big.NewInt(int64(fromBlock)),
		ToBlock:   big.NewInt(int64(toBlock)),
		Addresses: []common.Address{pairAddress},
		Topics:    [][]common.Hash{{swapEventID}},
	}

	logs, err := client.FilterLogs(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("filter swap logs for chart candles blocks %d-%d: %w", fromBlock, toBlock, err)
	}

	return logs, nil
}

func chartFromBlock(toBlock, blockCount uint64) uint64 {
	if blockCount == 0 {
		return toBlock
	}
	if toBlock+1 > blockCount {
		return toBlock - blockCount + 1
	}
	return 0
}

func sortChartLogs(logs []gethtypes.Log) {
	sort.Slice(logs, func(i, j int) bool {
		if logs[i].BlockNumber != logs[j].BlockNumber {
			return logs[i].BlockNumber < logs[j].BlockNumber
		}
		if logs[i].TxIndex != logs[j].TxIndex {
			return logs[i].TxIndex < logs[j].TxIndex
		}
		return logs[i].Index < logs[j].Index
	})
}

func GetRecentTransactions(ctx context.Context, client *ethclient.Client, config *TransactionStreamConfig, lookbackBlocks uint64) ([]Tx, error) {
	currentBlock, err := client.BlockNumber(ctx)
	if err != nil {
		return nil, fmt.Errorf("get current block: %w", err)
	}

	fromBlock := uint64(0)
	if currentBlock > lookbackBlocks {
		fromBlock = currentBlock - lookbackBlocks
	}

	contractABI, err := abi.JSON(strings.NewReader(UniswapV2PairABI))
	if err != nil {
		return nil, fmt.Errorf("parse pair ABI: %w", err)
	}

	swapEventID := contractABI.Events["Swap"].ID
	query := ethereum.FilterQuery{
		FromBlock: big.NewInt(int64(fromBlock)),
		ToBlock:   big.NewInt(int64(currentBlock)),
		Addresses: []common.Address{common.HexToAddress(config.PairAddressHex)},
		Topics:    [][]common.Hash{{swapEventID}},
	}

	logs, err := client.FilterLogs(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("filter swap logs: %w", err)
	}
	sortChartLogs(logs)
	if len(logs) > 100 {
		logs = logs[len(logs)-100:]
	}

	blockTimestamps := estimateChartBlockTimestamps(logs, currentBlock, time.Now().Unix())
	makerByHash := loadTransactionMakers(ctx, client, logs)

	txs := make([]Tx, 0, len(logs))
	for i := len(logs) - 1; i >= 0; i-- {
		logItem := logs[i]
		timestamp, ok := blockTimestamps[logItem.BlockNumber]
		if !ok {
			timestamp = time.Now().Unix()
		}

		amount0In, amount1In, amount0Out, amount1Out, ok := decodeSwapAmounts(logItem.Data)
		if !ok {
			continue
		}

		src, dst := swapLogAddresses(logItem)
		tx, ok := buildSwapTx(config, logItem.TxHash.String(), src, dst, makerByHash[logItem.TxHash], timestamp, amount0In, amount1In, amount0Out, amount1Out)
		if !ok {
			continue
		}
		txs = append(txs, tx)
	}

	return txs, nil
}

func swapLogAddresses(logItem gethtypes.Log) (string, string) {
	if len(logItem.Topics) < 3 {
		return "", ""
	}

	return common.BytesToAddress(logItem.Topics[1].Bytes()[12:]).Hex(),
		common.BytesToAddress(logItem.Topics[2].Bytes()[12:]).Hex()
}

func estimateChartBlockTimestamps(logs []gethtypes.Log, currentBlock uint64, nowUnix int64) map[uint64]int64 {
	timestamps := make(map[uint64]int64, len(logs))
	for _, logItem := range logs {
		blockNum := logItem.BlockNumber
		if _, ok := timestamps[blockNum]; ok {
			continue
		}

		blocksAgo := uint64(0)
		if currentBlock > blockNum {
			blocksAgo = currentBlock - blockNum
		}

		timestamps[blockNum] = nowUnix - int64(blocksAgo*12)
	}

	return timestamps
}

func chartWindowBounds(windowSeconds, intervalSeconds, now int64) (int64, int64) {
	if windowSeconds <= 0 {
		windowSeconds = defaultChartWindowSeconds
	}
	if intervalSeconds <= 0 {
		intervalSeconds = chartCandleIntervalSeconds
	}
	if now < 0 {
		now = 0
	}

	windowEnd := chartBucketStart(now, intervalSeconds)
	bucketCount := windowSeconds / intervalSeconds
	if windowSeconds%intervalSeconds != 0 {
		bucketCount++
	}
	if bucketCount < 1 {
		bucketCount = 1
	}

	windowStart := windowEnd - ((bucketCount - 1) * intervalSeconds)
	if windowStart < 0 {
		windowStart = 0
	}

	return windowStart, windowEnd
}

func activeChartWindow(points []chartSwapPoint, currentWindowStart, currentWindowEnd, windowSeconds, intervalSeconds int64) (int64, int64) {
	if len(points) == 0 {
		return currentWindowStart, currentWindowEnd
	}

	latestPointTime := points[0].Time
	for _, point := range points {
		if point.Time > latestPointTime {
			latestPointTime = point.Time
		}
		bucketTime := chartBucketStart(point.Time, intervalSeconds)
		if bucketTime >= currentWindowStart && bucketTime <= currentWindowEnd {
			return currentWindowStart, currentWindowEnd
		}
	}

	return chartWindowBounds(windowSeconds, intervalSeconds, latestPointTime)
}

func pointsInChartWindow(points []chartSwapPoint, windowStart, windowEnd, intervalSeconds int64) ([]chartSwapPoint, float64, bool) {
	windowPoints := make([]chartSwapPoint, 0, len(points))
	var (
		seedClose    float64
		hasSeedClose bool
	)

	sort.SliceStable(points, func(i, j int) bool {
		return points[i].Time < points[j].Time
	})

	for _, point := range points {
		bucketTime := chartBucketStart(point.Time, intervalSeconds)
		if bucketTime < windowStart {
			seedClose = point.Price
			hasSeedClose = true
			continue
		}
		if bucketTime > windowEnd {
			continue
		}

		windowPoints = append(windowPoints, point)
	}

	return windowPoints, seedClose, hasSeedClose
}

func chartBucketStart(timestamp, intervalSeconds int64) int64 {
	if intervalSeconds <= 0 {
		intervalSeconds = chartCandleIntervalSeconds
	}
	if timestamp <= 0 {
		return 0
	}

	return timestamp - (timestamp % intervalSeconds)
}

func buildChartCandlesFromPoints(points []chartSwapPoint, windowStart, windowEnd, intervalSeconds int64, seedClose float64, hasSeedClose bool) []ChartCandle {
	if intervalSeconds <= 0 {
		intervalSeconds = chartCandleIntervalSeconds
	}
	if windowEnd < windowStart {
		return nil
	}

	sort.SliceStable(points, func(i, j int) bool {
		return points[i].Time < points[j].Time
	})

	candles := make([]ChartCandle, 0, int((windowEnd-windowStart)/intervalSeconds)+1)
	if hasSeedClose && isValidChartPrice(seedClose) {
		candles = append(candles, flatChartCandle(windowStart, seedClose))
	}

	for _, point := range points {
		if !isValidChartPrice(point.Price) {
			continue
		}
		bucketTime := chartBucketStart(point.Time, intervalSeconds)
		if bucketTime < windowStart || bucketTime > windowEnd {
			continue
		}

		if bucketTime < windowStart {
			bucketTime = windowStart
		}

		candles = upsertChartCandle(candles, bucketTime, point.Price, point.Volume, intervalSeconds)
	}

	if len(candles) > 0 {
		candles = fillFlatCandlesThrough(candles, windowEnd, intervalSeconds)
	}

	return candles
}

func chartPointFromTx(tx Tx) (chartSwapPoint, bool) {
	if tx.TimeUnix <= 0 {
		return chartSwapPoint{}, false
	}

	price, ok := big.NewFloat(0).SetString(tx.Price)
	if !ok {
		return chartSwapPoint{}, false
	}
	priceFloat, _ := price.Float64()
	if !isValidChartPrice(priceFloat) {
		return chartSwapPoint{}, false
	}

	volume := tx.Volume
	if volume < 0 || math.IsNaN(volume) || math.IsInf(volume, 0) {
		volume = 0
	}

	return chartSwapPoint{
		Time:   tx.TimeUnix,
		Price:  priceFloat,
		Volume: volume,
	}, true
}

func upsertChartCandle(candles []ChartCandle, bucketTime int64, price, volume float64, intervalSeconds int64) []ChartCandle {
	if !isValidChartPrice(price) {
		return candles
	}
	if intervalSeconds <= 0 {
		intervalSeconds = chartCandleIntervalSeconds
	}
	if volume < 0 || math.IsNaN(volume) || math.IsInf(volume, 0) {
		volume = 0
	}

	if len(candles) == 0 {
		return append(candles, ChartCandle{
			Time:   bucketTime,
			Open:   price,
			High:   price,
			Low:    price,
			Close:  price,
			Volume: volume,
		})
	}

	lastTime := candles[len(candles)-1].Time
	if bucketTime < lastTime {
		idx := sort.Search(len(candles), func(i int) bool {
			return candles[i].Time >= bucketTime
		})
		if idx < len(candles) && candles[idx].Time == bucketTime {
			applyTradeToCandle(&candles[idx], price, volume)
		}
		return candles
	}

	if bucketTime == lastTime {
		applyTradeToCandle(&candles[len(candles)-1], price, volume)
		return candles
	}

	candles = fillFlatCandlesThrough(candles, bucketTime-intervalSeconds, intervalSeconds)
	open := candles[len(candles)-1].Close
	return append(candles, ChartCandle{
		Time:   bucketTime,
		Open:   open,
		High:   math.Max(open, price),
		Low:    math.Min(open, price),
		Close:  price,
		Volume: volume,
	})
}

func fillFlatCandlesThrough(candles []ChartCandle, targetTime int64, intervalSeconds int64) []ChartCandle {
	if len(candles) == 0 {
		return candles
	}
	if intervalSeconds <= 0 {
		intervalSeconds = chartCandleIntervalSeconds
	}

	targetBucket := chartBucketStart(targetTime, intervalSeconds)
	for {
		last := candles[len(candles)-1]
		nextTime := last.Time + intervalSeconds
		if nextTime > targetBucket {
			break
		}

		candles = append(candles, flatChartCandle(nextTime, last.Close))
	}

	return candles
}

func flatChartCandle(timestamp int64, price float64) ChartCandle {
	return ChartCandle{
		Time:  timestamp,
		Open:  price,
		High:  price,
		Low:   price,
		Close: price,
	}
}

func applyTradeToCandle(candle *ChartCandle, price, volume float64) {
	candle.High = math.Max(candle.High, price)
	candle.Low = math.Min(candle.Low, price)
	candle.Close = price
	candle.Volume += volume
}

func chartPointFromSwapLogData(config *TransactionStreamConfig, data []byte) (float64, float64, bool) {
	amount0In, amount1In, amount0Out, amount1Out, ok := decodeSwapAmounts(data)
	if !ok {
		return 0, 0, false
	}

	_, tokenAmountRaw, wrappedAmountRaw, ok := classifySwapTrade(config, amount0In, amount1In, amount0Out, amount1Out)
	if !ok {
		return 0, 0, false
	}

	price := calculateTokenPriceInWrapped(wrappedAmountRaw, tokenAmountRaw, config.TokenDecimals)
	if price == nil || price.Sign() <= 0 {
		return 0, 0, false
	}

	priceFloat, _ := price.Float64()
	if !isValidChartPrice(priceFloat) {
		return 0, 0, false
	}

	return priceFloat, chartWrappedVolume(config, amount0In, amount1In, amount0Out, amount1Out), true
}

func chartWrappedVolume(config *TransactionStreamConfig, amount0In, amount1In, amount0Out, amount1Out *big.Int) float64 {
	var wrappedIn, wrappedOut *big.Int
	if config.WrappedIsToken0 {
		wrappedIn = amount0In
		wrappedOut = amount0Out
	} else {
		wrappedIn = amount1In
		wrappedOut = amount1Out
	}

	inValue, _ := formatTokenAmount(wrappedIn, 18).Float64()
	outValue, _ := formatTokenAmount(wrappedOut, 18).Float64()
	volume := math.Abs(inValue) + math.Abs(outValue)
	if math.IsNaN(volume) || math.IsInf(volume, 0) || volume < 0 {
		return 0
	}

	return volume
}

func isValidChartPrice(price float64) bool {
	return !math.IsNaN(price) && !math.IsInf(price, 0) && price > 0
}

func trimChartCandles(candles []ChartCandle) []ChartCandle {
	if len(candles) <= maxCachedChartCandles {
		return candles
	}

	return candles[len(candles)-maxCachedChartCandles:]
}
