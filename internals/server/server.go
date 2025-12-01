package server

import (
	"context"
	"fmt"
	"html/template"
	"log"
	"math/big"
	"net/http"
	"strings"
	"time"

	token "github.com/belalakhter/ethereum_screener/internals/token"
	"github.com/defiweb/go-eth/rpc"
	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/ethclient"
)

type Server struct {
	mux             *http.ServeMux
	templates       *template.Template
	http_eth_client *ethclient.Client
	ws_eth_client   *rpc.Client
}

func NewServer(http_eth_client *ethclient.Client, ws_eth_client *rpc.Client) *Server {
	mux := http.NewServeMux()

	tmpl := template.Must(template.ParseGlob("internals/views/*.html"))

	s := &Server{
		mux:             mux,
		templates:       tmpl,
		http_eth_client: http_eth_client,
		ws_eth_client:   ws_eth_client,
	}
	mux.HandleFunc("/", s.handleIndex)
	fs := http.FileServer(http.Dir("internals/views"))
	mux.HandleFunc("/metadata", s.handleMetadata)
	mux.HandleFunc("/events", s.handleSSE)

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
	address := "0x52c77b0CB827aFbAD022E6d6CAF2C44452eDbc39"
	metadata := token.GetMetadata(r.Context(), s.http_eth_client, common.HexToAddress(address))
	err := s.templates.ExecuteTemplate(w, "index.html", metadata)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}
func (s *Server) handleMetadata(w http.ResponseWriter, r *http.Request) {

	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	address := r.FormValue("pair")
	fmt.Printf("/metadata address-> %v\n", address)
	if address == "" {
		address = "0x52c77b0CB827aFbAD022E6d6CAF2C44452eDbc39"
	}
	metadata := token.GetMetadata(r.Context(), s.http_eth_client, common.HexToAddress(address))

	err := s.templates.ExecuteTemplate(w, "metadata", metadata)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

func (s *Server) handleSSE(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "Streaming unsupported", http.StatusInternalServerError)
		return
	}

	txChan := make(chan token.Tx)
	defer close(txChan)
	pairHash := r.URL.Query().Get("pair")
	pairAddress := common.HexToAddress(pairHash)

	contractAbi, err := abi.JSON(strings.NewReader(token.UniswapV2PairABI))
	if err != nil {
		fmt.Printf("ABI parse failed: %v", err)
	}

	data0, _ := contractAbi.Pack("token0")
	res0, err := s.http_eth_client.CallContract(r.Context(), ethereum.CallMsg{To: &pairAddress, Data: data0}, nil)
	if err != nil {
		fmt.Printf("CallContract failed %v:", err)
	}
	address := common.BytesToAddress(res0)
	if pairHash == "" {
		address = common.HexToAddress("0xE0f63A424a4439cBE457D80E4f4b51aD25b2c56C")
	}
	go s.startTxListener(address.String(), r.Context(), txChan)

	notify := r.Context().Done()
	for {
		select {
		case <-notify:
			return
		case tx := <-txChan:
			fmt.Printf("tx -> %v \n", tx)
			fmt.Fprintf(w, "event: message\ndata: <div class=\"flex w-full justify-between items-center text-sm font-mono px-20 mb-2\">"+
				"<div class=\"w-[25%%] flex justify-center items-center\">"+
				"<span class=\"text-red-400 truncate mr-2\">%s…%s</span>"+
				"<span class=\"text-red-400 hover:text-red-300 cursor-pointer transition-colors\" onclick=\"navigator.clipboard.writeText('%s')\" title=\"Copy transaction hash\">📋</span>"+
				"</div>"+
				"<span class=\"text-yellow-400 w-[25%%] flex justify-center truncate \">%s</span>"+
				"<span class=\"text-green-400 w-[25%%]  flex justify-center truncate \">%s</span>"+
				"<span class=\"text-pink-400 w-[25%%]  flex justify-center truncate \">%s</span>"+
				"<span class=\"text-white w-[25%%] flex justify-center truncate \">%s</span>"+
				"</div>\n\n",
				tx.Hash[:6], tx.Hash[len(tx.Hash)-4:], tx.Hash, tx.Src[:9], tx.Dst[:9], ShortenNumber(tx.Wad), HumanizeDuration(tx.TimeUnix))
			flusher.Flush()
		}
	}
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
	t := time.Unix(unixTime, 0)
	return t.Format("3:04 PM")
}

func (s *Server) startTxListener(tokenHash string, ctx context.Context, txChan chan token.Tx) {

	go token.GetTransactions(ctx, s.ws_eth_client, tokenHash, txChan)
}
