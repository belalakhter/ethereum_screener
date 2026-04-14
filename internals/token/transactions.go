package token

import (
	"context"
	"log"
	"math/big"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/defiweb/go-eth/abi"
	"github.com/defiweb/go-eth/rpc"
	"github.com/defiweb/go-eth/types"
	gethcommon "github.com/ethereum/go-ethereum/common"
	gethtypes "github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethclient"
)

type Tx struct {
	Type          string
	Hash          string
	Src           string
	Dst           string
	Maker         string
	Wad           string
	Amount        string
	WrappedAmount string
	ValueUSD      string
	Price         string
	PriceDisplay  string
	PriceUSD      string
	TimeUnix      int64
	Volume        float64
}

const (
	transactionMakerLoadWorkers = 2
	transactionSignerCacheTTL   = time.Hour
)

var transactionSignerCache = struct {
	mu        sync.RWMutex
	signer    gethtypes.Signer
	expiresAt time.Time
}{}

func GetTransactions(ctx context.Context, client *rpc.Client, httpClient *ethclient.Client, config *TransactionStreamConfig, txChan chan Tx) {
	defer close(txChan)

	pairAddress := types.MustAddressFromHex(config.PairAddressHex)
	swap := abi.MustParseEvent("event Swap(address indexed sender, uint256 amount0In, uint256 amount1In, uint256 amount0Out, uint256 amount1Out, address indexed to)")

	query := types.NewFilterLogsQuery().
		SetAddresses(pairAddress).
		SetTopics([]types.Hash{swap.Topic0()})

	for {
		if ctx.Err() != nil {
			log.Printf("[tx] listener stopped token=%s pair=%s error=%v", config.TokenAddressHex, config.PairAddressHex, ctx.Err())
			return
		}

		log.Printf("[tx] subscribing token=%s pair=%s", config.TokenAddressHex, config.PairAddressHex)
		logs, err := client.SubscribeLogs(ctx, query)
		if err != nil {
			log.Printf("[tx] subscribe failed token=%s pair=%s error=%v", config.TokenAddressHex, config.PairAddressHex, err)
			if !waitForTransactionRetry(ctx, 3*time.Second) {
				return
			}
			continue
		}
		log.Printf("[tx] subscription active token=%s pair=%s", config.TokenAddressHex, config.PairAddressHex)

		subscriptionClosed := false
		for !subscriptionClosed {
			select {
			case <-ctx.Done():
				log.Printf("[tx] listener stopped token=%s pair=%s error=%v", config.TokenAddressHex, config.PairAddressHex, ctx.Err())
				return
			case logItem, ok := <-logs:
				if !ok {
					log.Printf("[tx] subscription channel closed token=%s pair=%s, retrying", config.TokenAddressHex, config.PairAddressHex)
					subscriptionClosed = true
					break
				}

				var (
					src        types.Address
					dst        types.Address
					amount0In  *big.Int
					amount1In  *big.Int
					amount0Out *big.Int
					amount1Out *big.Int
				)
				if err := swap.DecodeValues(logItem.Topics, logItem.Data, &src, &amount0In, &amount1In, &amount0Out, &amount1Out, &dst); err != nil {
					log.Printf("[tx] decode failed token=%s pair=%s hash=%s error=%v", config.TokenAddressHex, config.PairAddressHex, logItem.TransactionHash.String(), err)
					continue
				}

				hash := logItem.TransactionHash.String()
				maker := loadTransactionMaker(ctx, httpClient, gethcommon.HexToHash(hash))
				tx, ok := buildSwapTx(config, hash, src.String(), dst.String(), maker, time.Now().Unix(), amount0In, amount1In, amount0Out, amount1Out)
				if !ok {
					log.Printf("[tx] skipping swap without priced leg token=%s pair=%s hash=%s", config.TokenAddressHex, config.PairAddressHex, logItem.TransactionHash.String())
					continue
				}
				log.Printf("[tx] event hash=%s type=%s token=%s pair=%s src=%s dst=%s amount=%s price=%s", tx.Hash, tx.Type, config.TokenAddressHex, config.PairAddressHex, tx.Src, tx.Dst, tx.Amount, tx.PriceDisplay)

				AddToTransactionCache(config.PairAddressHex, tx)
				UpdateCandleCache(config.PairAddressHex, tx)
				UpdateMetadataPrice(config.PairAddressHex, tx)

				select {
				case txChan <- tx:
					log.Printf("[tx] queued hash=%s type=%s token=%s pair=%s", tx.Hash, tx.Type, config.TokenAddressHex, config.PairAddressHex)
				case <-ctx.Done():
					log.Printf("[tx] context closed before queue hash=%s token=%s pair=%s error=%v", tx.Hash, config.TokenAddressHex, config.PairAddressHex, ctx.Err())
					return
				}
			}
		}

		if !waitForTransactionRetry(ctx, 2*time.Second) {
			return
		}
	}
}

func buildSwapTx(config *TransactionStreamConfig, hash, src, dst, maker string, timestamp int64, amount0In, amount1In, amount0Out, amount1Out *big.Int) (Tx, bool) {
	tradeType, tokenAmountRaw, wrappedAmountRaw, ok := classifySwapTrade(config, amount0In, amount1In, amount0Out, amount1Out)
	if !ok {
		return Tx{}, false
	}

	price := calculateTokenPriceInWrapped(wrappedAmountRaw, tokenAmountRaw, config.TokenDecimals)
	if price == nil || price.Sign() <= 0 {
		return Tx{}, false
	}

	amount := formatTransactionTokenAmount(formatTokenAmount(tokenAmountRaw, config.TokenDecimals))
	wrappedAmount := formatTokenAmount(wrappedAmountRaw, 18)
	volume, _ := wrappedAmount.Float64()
	priceUSD, valueUSD := transactionUSDDisplays(config, price, wrappedAmount)
	if strings.TrimSpace(maker) == "" {
		maker = firstNonEmpty(src, dst)
	}

	return Tx{
		Type:          tradeType,
		Hash:          hash,
		Src:           src,
		Dst:           dst,
		Maker:         maker,
		Wad:           tokenAmountRaw.String(),
		Amount:        amount,
		WrappedAmount: formatTransactionWrappedAmount(wrappedAmount),
		ValueUSD:      valueUSD,
		Price:         price.Text('f', 24),
		PriceDisplay:  FormatPriceDisplay(price),
		PriceUSD:      priceUSD,
		TimeUnix:      timestamp,
		Volume:        volume,
	}, true
}

func transactionUSDDisplays(config *TransactionStreamConfig, priceInWrapped, wrappedAmount *big.Float) (string, string) {
	nativePrice := config.WrappedNativeUSDPrice
	if nativePrice == nil || nativePrice.Sign() <= 0 {
		cacheKey := "1:" + strings.ToLower(defaultWETHAddress.Hex())
		if gethcommon.IsHexAddress(config.WrappedNativeAddressHex) {
			cacheKey = "1:" + strings.ToLower(gethcommon.HexToAddress(config.WrappedNativeAddressHex).Hex())
		}

		cachedNativePrice, ok := loadCachedWrappedNativeUSDPrice(cacheKey)
		if !ok && gethcommon.IsHexAddress(config.WrappedNativeAddressHex) {
			cachedNativePrice, ok = loadCachedWrappedNativeUSDPriceForAddress(gethcommon.HexToAddress(config.WrappedNativeAddressHex))
		}
		if !ok || cachedNativePrice == nil || cachedNativePrice.Sign() <= 0 {
			return "", ""
		}
		nativePrice = cachedNativePrice
	}

	priceUSD := new(big.Float).Mul(priceInWrapped, nativePrice)
	valueUSD := new(big.Float).Mul(wrappedAmount, nativePrice)
	return "$" + FormatPriceDisplay(priceUSD), "$" + formatTransactionUSDValue(valueUSD)
}

func formatTransactionUSDValue(value *big.Float) string {
	if value == nil {
		return "0.00"
	}
	if new(big.Float).Abs(value).Cmp(big.NewFloat(1000)) >= 0 {
		return FriendlyFormat(value)
	}
	return FormatPriceDisplay(value)
}

func formatTransactionTokenAmount(value *big.Float) string {
	if value == nil {
		return "0"
	}

	absValue := new(big.Float).Abs(value)
	if absValue.Cmp(big.NewFloat(1)) >= 0 {
		return addCommasToInteger(value.Text('f', 0))
	}

	return FormatPriceDisplay(value)
}

func formatTransactionWrappedAmount(value *big.Float) string {
	if value == nil {
		return "0"
	}
	if new(big.Float).Abs(value).Cmp(big.NewFloat(0.000001)) < 0 {
		return FormatPriceDisplay(value)
	}
	return trimTrailingZeroes(value.Text('f', 6))
}

func addCommasToInteger(value string) string {
	sign := ""
	if strings.HasPrefix(value, "-") {
		sign = "-"
		value = strings.TrimPrefix(value, "-")
	}
	if len(value) <= 3 {
		return sign + value
	}

	var builder strings.Builder
	builder.WriteString(sign)
	firstGroupLen := len(value) % 3
	if firstGroupLen == 0 {
		firstGroupLen = 3
	}
	builder.WriteString(value[:firstGroupLen])
	for i := firstGroupLen; i < len(value); i += 3 {
		builder.WriteString(",")
		builder.WriteString(value[i : i+3])
	}

	return builder.String()
}

func loadTransactionMakers(ctx context.Context, client *ethclient.Client, logs []gethtypes.Log) map[gethcommon.Hash]string {
	makers := make(map[gethcommon.Hash]string)
	if client == nil || len(logs) == 0 || !transactionMakerLookupEnabled() {
		return makers
	}

	signer, ok := cachedTransactionSigner(ctx, client)
	if !ok {
		return makers
	}

	seen := make(map[gethcommon.Hash]struct{}, len(logs))
	hashes := make([]gethcommon.Hash, 0, len(logs))
	for _, logItem := range logs {
		if _, ok := seen[logItem.TxHash]; ok {
			continue
		}
		seen[logItem.TxHash] = struct{}{}
		hashes = append(hashes, logItem.TxHash)
	}

	jobs := make(chan gethcommon.Hash, len(hashes))
	results := make(chan transactionMakerResult, len(hashes))
	for _, hash := range hashes {
		jobs <- hash
	}
	close(jobs)

	workerCount := transactionMakerLoadWorkers
	if len(hashes) < workerCount {
		workerCount = len(hashes)
	}

	var wg sync.WaitGroup
	wg.Add(workerCount)
	for i := 0; i < workerCount; i++ {
		go func() {
			defer wg.Done()
			for hash := range jobs {
				maker, err := transactionMakerFromHash(ctx, client, signer, hash)
				if err != nil {
					log.Printf("[tx] failed maker lookup hash=%s error=%v", hash.Hex(), err)
					continue
				}
				results <- transactionMakerResult{Hash: hash, Maker: maker}
			}
		}()
	}

	go func() {
		wg.Wait()
		close(results)
	}()

	for result := range results {
		makers[result.Hash] = result.Maker
	}

	return makers
}

func loadTransactionMaker(ctx context.Context, client *ethclient.Client, hash gethcommon.Hash) string {
	if client == nil || !transactionMakerLookupEnabled() {
		return ""
	}

	signer, ok := cachedTransactionSigner(ctx, client)
	if !ok {
		return ""
	}

	maker, err := transactionMakerFromHash(ctx, client, signer, hash)
	if err != nil {
		log.Printf("[tx] failed maker lookup hash=%s error=%v", hash.Hex(), err)
		return ""
	}

	return maker
}

func transactionMakerLookupEnabled() bool {
	value := strings.ToLower(strings.TrimSpace(os.Getenv("ENABLE_TRANSACTION_MAKER_LOOKUP")))
	return value == "1" || value == "true" || value == "yes" || value == "on"
}

func cachedTransactionSigner(ctx context.Context, client *ethclient.Client) (gethtypes.Signer, bool) {
	now := time.Now()
	transactionSignerCache.mu.RLock()
	signer := transactionSignerCache.signer
	expiresAt := transactionSignerCache.expiresAt
	transactionSignerCache.mu.RUnlock()
	if signer != nil && now.Before(expiresAt) {
		return signer, true
	}

	chainID, err := client.ChainID(ctx)
	if err != nil {
		log.Printf("[tx] failed to load chain id for maker lookup: %v", err)
		return nil, false
	}

	signer = gethtypes.LatestSignerForChainID(chainID)
	transactionSignerCache.mu.Lock()
	transactionSignerCache.signer = signer
	transactionSignerCache.expiresAt = now.Add(transactionSignerCacheTTL)
	transactionSignerCache.mu.Unlock()

	return signer, true
}

func transactionMakerFromHash(ctx context.Context, client *ethclient.Client, signer gethtypes.Signer, hash gethcommon.Hash) (string, error) {
	tx, _, err := client.TransactionByHash(ctx, hash)
	if err != nil {
		return "", err
	}

	maker, err := gethtypes.Sender(signer, tx)
	if err != nil {
		return "", err
	}

	return maker.Hex(), nil
}

type transactionMakerResult struct {
	Hash  gethcommon.Hash
	Maker string
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}
	return ""
}

func waitForTransactionRetry(ctx context.Context, delay time.Duration) bool {
	timer := time.NewTimer(delay)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
