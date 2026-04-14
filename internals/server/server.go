package server

import (
	"context"
	"encoding/json"
	"fmt"
	"html/template"
	"log"
	"math/big"
	"net/http"
	"os"
	"strings"
	"time"

	token "github.com/belalakhter/ethereum_screener/internals/token"
	"github.com/defiweb/go-eth/rpc"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/ethclient"
)

type Server struct {
	mux                *http.ServeMux
	templates          *template.Template
	http_eth_client    *ethclient.Client
	ws_eth_client      *rpc.Client
	networkLabel       string
	wrappedNativeToken common.Address
	defaultPairAddress string
}

type IndexPageData struct {
	DefaultPair string
	Network     string
	Metadata    *token.TokenMetadata
}

type chartTickPayload struct {
	Time         int64   `json:"time"`
	Price        string  `json:"price"`
	PriceDisplay string  `json:"priceDisplay"`
	Hash         string  `json:"hash"`
	Type         string  `json:"type"`
	Volume       float64 `json:"volume"`
	Historical   bool    `json:"historical,omitempty"`
}

type streamStatsPayload struct {
	TransactionCount int `json:"transactionCount"`
	CandleCount      int `json:"candleCount"`
}

const (
	chartHistoryWindowSeconds int64  = 48 * 60 * 60
	chartHistoryLookbackBlocks uint64 = 15000
)

func NewServer(http_eth_client *ethclient.Client, ws_eth_client *rpc.Client) *Server {
	mux := http.NewServeMux()

	tmpl := template.Must(template.ParseGlob("internals/views/*.html"))
	defaultPairAddress := strings.TrimSpace(os.Getenv("DEFAULT_PAIR_ADDRESS"))
	if defaultPairAddress != "" && !common.IsHexAddress(defaultPairAddress) {
		log.Printf("Ignoring invalid DEFAULT_PAIR_ADDRESS %q", defaultPairAddress)
		defaultPairAddress = ""
	}

	s := &Server{
		mux:                mux,
		templates:          tmpl,
		http_eth_client:    http_eth_client,
		ws_eth_client:      ws_eth_client,
		networkLabel:       token.ConnectedNetworkLabel(context.Background(), http_eth_client),
		wrappedNativeToken: token.ResolveWrappedNativeAddress(context.Background(), http_eth_client),
		defaultPairAddress: defaultPairAddress,
	}
	go func() {
		price := token.WarmWrappedNativeUSDPriceCache(context.Background(), http_eth_client, s.wrappedNativeToken)
		if price != nil && price.Sign() > 0 {
			log.Printf("[metadata] warmed wrapped native USD price=%s network=%s", price.Text('f', 6), s.networkLabel)
		}
	}()
	mux.HandleFunc("/", s.handleIndex)
	fs := http.FileServer(http.Dir("internals/views"))
	mux.HandleFunc("/metadata", s.handleMetadata)
	mux.HandleFunc("/events", s.handleSSE)
	mux.HandleFunc("/chart-data", s.handleChartData)

	mux.Handle("/static/", http.StripPrefix("/static/", fs))

	mux.HandleFunc("/ping", s.ping)

	return s
}

func (s *Server) Start(addr string) {
	log.Printf("Starting server on %s\n", addr)
	err := http.ListenAndServe(addr, s.mux)
	if err != nil {
		log.Fatalf("Server failed: %v", err)
	}
}

func (s *Server) ping(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(200)
	w.Write([]byte("pong"))
}
func (s *Server) handleIndex(w http.ResponseWriter, r *http.Request) {
	inputAddress := strings.TrimSpace(r.URL.Query().Get("pair"))
	if inputAddress == "" {
		inputAddress = s.defaultPairAddress
	}

	pageData := IndexPageData{
		DefaultPair: inputAddress,
		Network:     s.networkLabel,
		Metadata:    s.metadataForInput(r.Context(), inputAddress),
	}

	err := s.templates.ExecuteTemplate(w, "index.html", pageData)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}
func (s *Server) handleMetadata(w http.ResponseWriter, r *http.Request) {

	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	address := strings.TrimSpace(r.FormValue("pair"))
	log.Printf("[metadata] lookup requested input=%s remote=%s", address, r.RemoteAddr)
	metadata := s.metadataForInput(r.Context(), address)
	if metadata.Error != "" {
		log.Printf("[metadata] lookup failed input=%s remote=%s error=%s", address, r.RemoteAddr, metadata.Error)
	} else {
		log.Printf("[metadata] lookup succeeded input=%s base=%s pair=%s symbol=%s remote=%s", address, metadata.BaseAddress, metadata.PairAddress, metadata.Symbol, r.RemoteAddr)
	}

	err := s.templates.ExecuteTemplate(w, "metadata", metadata)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

func (s *Server) handleSSE(w http.ResponseWriter, r *http.Request) {
	inputAddress := strings.TrimSpace(r.URL.Query().Get("pair"))
	if inputAddress == "" {
		inputAddress = s.defaultPairAddress
	}
	if inputAddress == "" {
		log.Printf("[sse] empty subscription request remote=%s", r.RemoteAddr)
		w.WriteHeader(http.StatusNoContent)
		return
	}
	if !common.IsHexAddress(inputAddress) {
		log.Printf("[sse] rejected invalid subscription input=%s remote=%s", inputAddress, r.RemoteAddr)
		http.Error(w, "Invalid token or pair address", http.StatusBadRequest)
		return
	}
	log.Printf("[sse] subscribe requested input=%s remote=%s", inputAddress, r.RemoteAddr)

	resolvedPairAddress, err := token.ResolvePairAddress(r.Context(), s.http_eth_client, common.HexToAddress(inputAddress), s.wrappedNativeToken)
	if err != nil {
		log.Printf("[sse] failed to resolve input=%s remote=%s error=%v", inputAddress, r.RemoteAddr, err)
		http.Error(w, fmt.Sprintf("Unable to subscribe for %s: %v", inputAddress, err), http.StatusBadGateway)
		return
	}

	trackedTokenAddress, err := token.GetTrackedToken(r.Context(), s.http_eth_client, resolvedPairAddress, s.wrappedNativeToken)
	if err != nil {
		log.Printf("[sse] failed to resolve tracked token input=%s pair=%s remote=%s error=%v", inputAddress, resolvedPairAddress.Hex(), r.RemoteAddr, err)
		http.Error(w, fmt.Sprintf("Unable to subscribe for %s: %v", inputAddress, err), http.StatusBadGateway)
		return
	}
	streamConfig, err := token.BuildTransactionStreamConfig(r.Context(), s.http_eth_client, resolvedPairAddress, s.wrappedNativeToken)
	if err != nil {
		log.Printf("[sse] failed to build stream config input=%s pair=%s remote=%s error=%v", inputAddress, resolvedPairAddress.Hex(), r.RemoteAddr, err)
		http.Error(w, fmt.Sprintf("Unable to subscribe for %s: %v", inputAddress, err), http.StatusBadGateway)
		return
	}
	log.Printf("[sse] resolved subscription input=%s pair=%s token=%s remote=%s", inputAddress, resolvedPairAddress.Hex(), trackedTokenAddress.Hex(), r.RemoteAddr)

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("X-Accel-Buffering", "no")

	flusher, ok := w.(http.Flusher)
	if !ok {
		log.Printf("[sse] streaming unsupported pair=%s token=%s remote=%s", resolvedPairAddress.Hex(), trackedTokenAddress.Hex(), r.RemoteAddr)
		http.Error(w, "Streaming unsupported", http.StatusInternalServerError)
		return
	}

	txChan := make(chan token.Tx, 16)
	go s.startTxListener(streamConfig, r.Context(), txChan)

	if _, err := fmt.Fprintf(w, "retry: 10000\n: connected pair=%s token=%s\n\n", resolvedPairAddress.Hex(), trackedTokenAddress.Hex()); err != nil {
		log.Printf("[sse] failed initial flush pair=%s token=%s remote=%s error=%v", resolvedPairAddress.Hex(), trackedTokenAddress.Hex(), r.RemoteAddr, err)
		return
	}
	flusher.Flush()

	pairHex := resolvedPairAddress.Hex()
	history, candles, warmErr := s.warmPairHistoryWithConfig(r.Context(), streamConfig, pairHex, time.Now().Unix())
	if warmErr != nil {
		log.Printf("[sse] failed to warm history for pair=%s error=%v", pairHex, warmErr)
	}
	if len(candles) > 0 {
		if err := writeCandleSnapshotEvent(w, candles); err != nil {
			log.Printf("[sse] failed to send historical candle snapshot pair=%s token=%s remote=%s error=%v", resolvedPairAddress.Hex(), trackedTokenAddress.Hex(), r.RemoteAddr, err)
			return
		}
	}
	if err := writeStreamStatsEvent(w, len(history), len(candles)); err != nil {
		log.Printf("[sse] failed to send historical stats pair=%s token=%s remote=%s error=%v", resolvedPairAddress.Hex(), trackedTokenAddress.Hex(), r.RemoteAddr, err)
		return
	}
	log.Printf("[sse] historical warmup pair=%s token=%s txs=%d candles=%d remote=%s", resolvedPairAddress.Hex(), trackedTokenAddress.Hex(), len(history), len(candles), r.RemoteAddr)

	for i := len(history) - 1; i >= 0; i-- {
		tx := history[i]
		if _, err := fmt.Fprintf(w, "event: message\ndata: %s\n\n", renderTransactionRow(tx)); err != nil {
			log.Printf("[sse] failed to send historical tx pair=%s token=%s hash=%s remote=%s error=%v", resolvedPairAddress.Hex(), trackedTokenAddress.Hex(), tx.Hash, r.RemoteAddr, err)
			return
		}
		if err := writeChartEvent(w, tx, true); err != nil {
			log.Printf("[sse] failed to send historical chart tick pair=%s token=%s hash=%s remote=%s error=%v", resolvedPairAddress.Hex(), trackedTokenAddress.Hex(), tx.Hash, r.RemoteAddr, err)
			return
		}
	}
	flusher.Flush()

	log.Printf("[sse] stream opened pair=%s token=%s remote=%s", resolvedPairAddress.Hex(), trackedTokenAddress.Hex(), r.RemoteAddr)

	notify := r.Context().Done()
	heartbeatTicker := time.NewTicker(20 * time.Second)
	defer heartbeatTicker.Stop()
	for {
		select {
		case <-notify:
			log.Printf("[sse] client disconnected pair=%s token=%s remote=%s err=%v", resolvedPairAddress.Hex(), trackedTokenAddress.Hex(), r.RemoteAddr, r.Context().Err())
			return
		case <-heartbeatTicker.C:
			if _, err := fmt.Fprintf(w, ": keepalive pair=%s token=%s ts=%d\n\n", resolvedPairAddress.Hex(), trackedTokenAddress.Hex(), time.Now().Unix()); err != nil {
				log.Printf("[sse] heartbeat failed pair=%s token=%s remote=%s error=%v", resolvedPairAddress.Hex(), trackedTokenAddress.Hex(), r.RemoteAddr, err)
				return
			}
			flusher.Flush()
		case tx, ok := <-txChan:
			if !ok {
				log.Printf("[sse] listener closed pair=%s token=%s remote=%s", resolvedPairAddress.Hex(), trackedTokenAddress.Hex(), r.RemoteAddr)
				return
			}
			log.Printf("[sse] forwarding tx pair=%s token=%s hash=%s type=%s src=%s dst=%s amount=%s price=%s remote=%s", resolvedPairAddress.Hex(), trackedTokenAddress.Hex(), tx.Hash, tx.Type, tx.Src, tx.Dst, tx.Amount, tx.PriceDisplay, r.RemoteAddr)
			if _, err := fmt.Fprintf(w, "event: message\ndata: %s\n\n", renderTransactionRow(tx)); err != nil {
				log.Printf("[sse] write failed pair=%s token=%s hash=%s remote=%s error=%v", resolvedPairAddress.Hex(), trackedTokenAddress.Hex(), tx.Hash, r.RemoteAddr, err)
				return
			}
			if err := writeChartEvent(w, tx, false); err != nil {
				log.Printf("[sse] chart write failed pair=%s token=%s hash=%s remote=%s error=%v", resolvedPairAddress.Hex(), trackedTokenAddress.Hex(), tx.Hash, r.RemoteAddr, err)
				return
			}
			flusher.Flush()
		}
	}
}

func (s *Server) handleChartData(w http.ResponseWriter, r *http.Request) {
	inputAddress := strings.TrimSpace(r.URL.Query().Get("pair"))
	if inputAddress == "" {
		inputAddress = s.defaultPairAddress
	}
	if inputAddress == "" {
		log.Printf("[chart] empty history request remote=%s", r.RemoteAddr)
		http.Error(w, "Missing token or pair address", http.StatusBadRequest)
		return
	}
	if !common.IsHexAddress(inputAddress) {
		log.Printf("[chart] rejected invalid history input=%s remote=%s", inputAddress, r.RemoteAddr)
		http.Error(w, "Invalid token or pair address", http.StatusBadRequest)
		return
	}

	log.Printf("[chart] history requested input=%s remote=%s", inputAddress, r.RemoteAddr)
	resolvedPairAddress, err := token.ResolvePairAddress(r.Context(), s.http_eth_client, common.HexToAddress(inputAddress), s.wrappedNativeToken)
	if err != nil {
		log.Printf("[chart] failed to resolve history input=%s remote=%s error=%v", inputAddress, r.RemoteAddr, err)
		http.Error(w, fmt.Sprintf("Unable to load chart history for %s: %v", inputAddress, err), http.StatusBadGateway)
		return
	}

	streamConfig, err := token.BuildTransactionStreamConfig(r.Context(), s.http_eth_client, resolvedPairAddress, s.wrappedNativeToken)
	if err != nil {
		log.Printf("[chart] failed to build history config input=%s pair=%s remote=%s error=%v", inputAddress, resolvedPairAddress.Hex(), r.RemoteAddr, err)
		http.Error(w, fmt.Sprintf("Unable to load chart history for %s: %v", inputAddress, err), http.StatusBadGateway)
		return
	}

	pairHex := resolvedPairAddress.Hex()
	nowUnix := time.Now().Unix()
	if candles := token.GetCachedChartCandlesThrough(pairHex, nowUnix); len(candles) > 0 {
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(candles); err != nil {
			log.Printf("[chart] failed to encode cached history input=%s pair=%s remote=%s error=%v", inputAddress, pairHex, r.RemoteAddr, err)
			return
		}

		log.Printf("[chart] history loaded input=%s pair=%s candles=%d source=cache remote=%s", inputAddress, pairHex, len(candles), r.RemoteAddr)
		return
	}

	candles, err := token.GetRecentChartCandles(r.Context(), s.http_eth_client, streamConfig, chartHistoryWindowSeconds, chartHistoryLookbackBlocks)
	if err != nil {
		log.Printf("[chart] log-backed history failed input=%s pair=%s remote=%s error=%v", inputAddress, pairHex, r.RemoteAddr, err)
		transactionCandles, txErr := s.transactionBackedChartCandles(r.Context(), streamConfig, pairHex, nowUnix)
		if txErr != nil || len(transactionCandles) == 0 {
			if txErr != nil {
				log.Printf("[chart] transaction-backed history failed input=%s pair=%s remote=%s error=%v", inputAddress, pairHex, r.RemoteAddr, txErr)
			}
			http.Error(w, fmt.Sprintf("Unable to load chart history for %s: %v", inputAddress, err), http.StatusBadGateway)
			return
		}
		candles = transactionCandles
	} else if len(candles) == 0 {
		transactionCandles, txErr := s.transactionBackedChartCandles(r.Context(), streamConfig, pairHex, nowUnix)
		if txErr != nil {
			log.Printf("[chart] transaction-backed empty-history fallback failed input=%s pair=%s remote=%s error=%v", inputAddress, pairHex, r.RemoteAddr, txErr)
		}
		if len(transactionCandles) > 0 {
			candles = transactionCandles
		}
	}

	candles = token.FillChartCandlesThrough(candles, nowUnix)
	if len(candles) > 0 {
		token.SeedCandleCache(pairHex, candles)
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(candles); err != nil {
		log.Printf("[chart] failed to encode history input=%s pair=%s remote=%s error=%v", inputAddress, pairHex, r.RemoteAddr, err)
		return
	}

	log.Printf("[chart] history loaded input=%s pair=%s candles=%d remote=%s", inputAddress, pairHex, len(candles), r.RemoteAddr)
}

func (s *Server) transactionBackedChartCandles(ctx context.Context, config *token.TransactionStreamConfig, pairHex string, nowUnix int64) ([]token.ChartCandle, error) {
	_, candles, err := s.warmPairHistoryWithConfig(ctx, config, pairHex, nowUnix)
	return candles, err
}

func getTypeColor(t string) string {
	switch t {
	case "Buy":
		return "text-green-400 font-bold"
	case "Sell":
		return "text-red-400 font-bold"
	default:
		return "text-gray-400"
	}
}

func renderTransactionRow(tx token.Tx) string {
	hash := displayOrDash(shortenHash(tx.Hash))
	maker := displayOrDash(shortenAddress(tx.Maker))
	price := tx.PriceUSD
	if strings.TrimSpace(price) == "" && strings.TrimSpace(tx.PriceDisplay) != "" {
		price = tx.PriceDisplay + " WETH"
	}

	return fmt.Sprintf(
		"<div class=\"flex w-full justify-between items-center text-sm font-mono px-6 mb-2\">"+
			"<span class=\"text-white/85 w-[12.5%%] flex justify-center truncate\">%s</span>"+
			"<span class=\"w-[12.5%%] flex justify-center truncate %s\">%s</span>"+
			"<span class=\"text-red-300 w-[12.5%%] flex justify-center truncate\">%s</span>"+
			"<span class=\"text-pink-300 w-[12.5%%] flex justify-center truncate\">%s</span>"+
			"<span class=\"text-blue-200 w-[12.5%%] flex justify-center truncate\">%s</span>"+
			"<span class=\"text-red-300 w-[12.5%%] flex justify-center truncate\">%s</span>"+
			"<span class=\"text-yellow-300 w-[12.5%%] flex justify-center truncate\">%s</span>"+
			"<span class=\"text-red-400 w-[12.5%%] flex justify-center truncate cursor-pointer\" onclick=\"navigator.clipboard.writeText('%s')\" title=\"Copy transaction hash\">%s</span>"+
			"</div>",
		template.HTMLEscapeString(HumanizeDuration(tx.TimeUnix)),
		getTypeColor(tx.Type),
		template.HTMLEscapeString(displayOrDash(tx.Type)),
		template.HTMLEscapeString(displayOrDash(tx.ValueUSD)),
		template.HTMLEscapeString(displayOrDash(tx.Amount)),
		template.HTMLEscapeString(displayOrDash(tx.WrappedAmount)),
		template.HTMLEscapeString(displayOrDash(price)),
		template.HTMLEscapeString(maker),
		template.JSEscapeString(tx.Hash),
		template.HTMLEscapeString(hash),
	)
}

func writeChartEvent(w http.ResponseWriter, tx token.Tx, historical bool) error {
	chartPayload, err := json.Marshal(chartTickPayload{
		Time:         tx.TimeUnix,
		Price:        tx.Price,
		PriceDisplay: tx.PriceDisplay,
		Hash:         tx.Hash,
		Type:         tx.Type,
		Volume:       tx.Volume,
		Historical:   historical,
	})
	if err != nil {
		return err
	}

	_, err = fmt.Fprintf(w, "event: chart\ndata: %s\n\n", chartPayload)
	return err
}

func writeCandleSnapshotEvent(w http.ResponseWriter, candles []token.ChartCandle) error {
	payload, err := json.Marshal(candles)
	if err != nil {
		return err
	}

	_, err = fmt.Fprintf(w, "event: candles\ndata: %s\n\n", payload)
	return err
}

func writeStreamStatsEvent(w http.ResponseWriter, transactionCount int, candleCount int) error {
	payload, err := json.Marshal(streamStatsPayload{
		TransactionCount: transactionCount,
		CandleCount:      candleCount,
	})
	if err != nil {
		return err
	}

	_, err = fmt.Fprintf(w, "event: stats\ndata: %s\n\n", payload)
	return err
}

func displayOrDash(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return "-"
	}
	return value
}

func shortenHash(hash string) string {
	hash = strings.TrimSpace(hash)
	if len(hash) <= 12 {
		return hash
	}
	return hash[:6] + "..." + hash[len(hash)-4:]
}

func shortenAddress(address string) string {
	address = strings.TrimSpace(address)
	if len(address) <= 12 {
		return address
	}
	return address[:6] + "..." + address[len(address)-4:]
}

func (s *Server) metadataForInput(ctx context.Context, address string) *token.TokenMetadata {
	address = strings.TrimSpace(address)
	if address == "" {
		return token.NewMetadataMessage("Enter a token address or a Uniswap V2-style pair address to inspect metadata.")
	}
	if !common.IsHexAddress(address) {
		return token.NewMetadataMessage("Enter a valid 42-character 0x token or pair address.")
	}

	inputAddress := common.HexToAddress(address)
	resolvedPairAddress, err := token.ResolvePairAddress(ctx, s.http_eth_client, inputAddress, s.wrappedNativeToken)
	if err != nil {
		log.Printf("Failed to resolve pool for %s: %v", address, err)
		return token.NewMetadataMessage(token.MetadataLookupMessage(inputAddress, s.networkLabel, err))
	}

	metadata, err := token.GetMetadata(ctx, s.http_eth_client, resolvedPairAddress, s.wrappedNativeToken)
	if err != nil {
		log.Printf("Failed to load metadata for %s: %v", address, err)
		return token.NewMetadataMessage(token.MetadataLookupMessage(inputAddress, s.networkLabel, err))
	}

	if metadata.TotalTxCount == 0 || len(token.GetCachedCandles(resolvedPairAddress.Hex())) == 0 {
		history, candles, err := s.warmPairHistory(ctx, resolvedPairAddress, time.Now().Unix())
		if err != nil {
			log.Printf("[metadata] failed to warm history for pair=%s error=%v", resolvedPairAddress.Hex(), err)
		} else {
			if uint64(len(history)) > metadata.TotalTxCount {
				metadata.TotalTxCount = uint64(len(history))
			}
			log.Printf("[metadata] warmed pair history pair=%s txs=%d candles=%d", resolvedPairAddress.Hex(), len(history), len(candles))
		}
	}

	return metadata
}

func (s *Server) warmPairHistory(ctx context.Context, pairAddress common.Address, nowUnix int64) ([]token.Tx, []token.ChartCandle, error) {
	streamConfig, err := token.BuildTransactionStreamConfig(ctx, s.http_eth_client, pairAddress, s.wrappedNativeToken)
	if err != nil {
		return nil, nil, err
	}

	return s.warmPairHistoryWithConfig(ctx, streamConfig, pairAddress.Hex(), nowUnix)
}

func (s *Server) warmPairHistoryWithConfig(ctx context.Context, config *token.TransactionStreamConfig, pairHex string, nowUnix int64) ([]token.Tx, []token.ChartCandle, error) {
	history := token.GetCachedTransactions(pairHex)
	if len(history) == 0 {
		txs, err := token.GetRecentTransactions(ctx, s.http_eth_client, config, chartHistoryLookbackBlocks)
		if err != nil {
			return nil, nil, err
		}
		history = txs
		token.SeedTransactionCache(pairHex, history)
	}

	candles := token.GetCachedChartCandlesThrough(pairHex, nowUnix)
	if len(history) > 0 {
		historyCandles := token.SeedCandleCacheFromTransactions(pairHex, history, chartHistoryWindowSeconds, nowUnix)
		if len(historyCandles) > 0 {
			candles = historyCandles
		}
	}

	return history, candles, nil
}
func ShortenNumber(raw string) string {
	i := new(big.Int)
	i.SetString(raw, 10)
	f := new(big.Float).SetInt(i)

	oneK := big.NewFloat(1e3)
	oneM := big.NewFloat(1e6)
	oneB := big.NewFloat(1e9)

	switch {
	case f.Cmp(oneB) >= 0:
		v, _ := new(big.Float).Quo(f, oneB).Float64()
		return fmt.Sprintf("%.2fB", v)
	case f.Cmp(oneM) >= 0:
		v, _ := new(big.Float).Quo(f, oneM).Float64()
		return fmt.Sprintf("%.2fM", v)
	case f.Cmp(oneK) >= 0:
		v, _ := new(big.Float).Quo(f, oneK).Float64()
		return fmt.Sprintf("%.2fk", v)
	default:
		v, _ := f.Float64()
		return fmt.Sprintf("%.2f", v)
	}
}

func HumanizeDuration(unixTime int64) string {
	if unixTime <= 0 {
		return "-"
	}

	t := time.Unix(unixTime, 0)
	elapsed := time.Since(t)
	if elapsed < 0 {
		return t.Format("Jan 2 3:04 PM")
	}
	if elapsed < time.Minute {
		return "now"
	}
	if elapsed < time.Hour {
		return fmt.Sprintf("%dm ago", int(elapsed.Minutes()))
	}
	if elapsed < 24*time.Hour {
		return fmt.Sprintf("%dh ago", int(elapsed.Hours()))
	}
	if elapsed < 7*24*time.Hour {
		return fmt.Sprintf("%dd ago", int(elapsed.Hours()/24))
	}
	return t.Format("Jan 2")
}

func (s *Server) startTxListener(config *token.TransactionStreamConfig, ctx context.Context, txChan chan token.Tx) {
	log.Printf("[tx] listener starting token=%s pair=%s", config.TokenAddressHex, config.PairAddressHex)
	token.GetTransactions(ctx, s.ws_eth_client, s.http_eth_client, config, txChan)
}
