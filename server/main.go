package main

import (
	"flag"
	"log"
	"net/http"

	"encoding/json"
	"fmt"

	"os"

	"io/ioutil"

	"github.com/gorilla/websocket"
	"github.com/krantius/bittrex-data/stats"
	"github.com/olivere/elastic"
)

var addr = flag.String("addr", "localhost:8080", "http service address")
var upgrader = websocket.Upgrader{} // use default options
var tradeC = make(chan TradeData, 1000)
var Listeners map[string]chan *TradeUpdate

func main() {
	Listeners = make(map[string]chan *TradeUpdate)
	/*_, err := os.Create("./output.txt")
	if err != nil {
		fmt.Printf("failed to create output file: %v\n", err)
	}*/

	// Make elastic client
	_, err := elastic.NewSimpleClient(elastic.SetURL("http://192.168.1.125:9200"))
	if err != nil {
		fmt.Printf("failed to create elastic client: %v\n", err)
		return
	}

	// Get all markets
	markets := LoadMarkets("./markets.json")

	// Get all stats for the markets
	candleStats := LoadStats()

	// Initialize listeners
	InitListeners(markets, candleStats)

	// Serve websocket requests
	flag.Parse()
	log.SetFlags(0)
	http.HandleFunc("/ws", echo)
	log.Fatal(http.ListenAndServe(*addr, nil))
}

func echo(w http.ResponseWriter, r *http.Request) {
	fmt.Println("got echo!")
	c, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Print("upgrade:", err)
		return
	}
	defer c.Close()
	for {
		_, message, err := c.ReadMessage()
		if err != nil {
			log.Println("read:", err)
			break
		}

		// Unmarshal into a trade update
		var tradeUpdate *TradeUpdate
		if err := json.Unmarshal(message, tradeUpdate); err != nil {
			fmt.Printf("error unmarshalling trade update: %v\n", err)
			continue
		}

		WriteTradeUpdateToFile(tradeUpdate)

		// Send to the correct listener for processing
		Listeners[tradeUpdate.Pair] <- tradeUpdate

		if err != nil {
			log.Println("write:", err)
			break
		}
	}
}

type TradeUpdate struct {
	Pair string       `json:"pair"`
	Data []*TradeData `json:"data"`
}

type TradeData struct {
	Id        int     `json"id"`
	Quantity  float32 `json:"quantity"`
	Rate      float32 `json"rate"`
	Price     float32 `json:"price"`
	OrderType string  `json:"orderType"`
	Timestamp float32 `json:"timestamp"`
}

func InitListeners(markets []string, m map[string]*stats.CandleStats) {
	for _, market := range markets {
		stats, ok := m[market]
		if !ok {
			fmt.Printf("Market %v did not have any stats\n", market)
			continue
		}

		tl := &TradeListener{
			Market: market,
			C:      make(chan *TradeUpdate, 1000),
			Stats:  stats,
		}

		Listeners[market] = tl.C
	}
}

func LoadMarkets(name string) []string {
	b, err := ioutil.ReadFile("markets.json")
	if err != nil {
		fmt.Printf("failed to read markets file: %v", err)
		return nil
	}

	markets := []string{}
	err = json.Unmarshal(b, &markets)
	if err != nil {
		fmt.Printf("failed to load markets: %v", err)
		return nil
	}

	return markets
}

func LoadStats() map[string]*stats.CandleStats {
	b, err := ioutil.ReadFile("stats.txt")
	if err != nil {
		fmt.Printf("failed to read stats file: %v", err)
		return nil
	}

	candleStats := []*stats.CandleStats{}
	err = json.Unmarshal(b, &candleStats)
	if err != nil {
		fmt.Printf("failed to load markets: %v", err)
		return nil
	}

	m := map[string]*stats.CandleStats{}
	for _, candleStat := range candleStats {
		if candleStat == nil {
			fmt.Println("found nil candle stat")
			continue
		}
		if candleStat.Market == "" {
			fmt.Printf("stat %v didn't have a marget\n", candleStat)
			continue
		}
		m[candleStat.Market] = candleStat
	}

	for _, v := range m {
		fmt.Printf("%+v\n", *v)
	}

	return m
}

func WriteTradeUpdateToFile(t *TradeUpdate) {
	// Open up our output mega file
	f, err := os.OpenFile("./output.txt", os.O_APPEND|os.O_WRONLY, os.ModeAppend)
	if err != nil {
		fmt.Printf("error opening file: %v\n", err)
		return
	}

	for _, trade := range t.Data {
		tradeC <- *trade
		output := fmt.Sprintf("%v %s with %s order %v\n", trade.Timestamp, t.Pair, trade.OrderType, trade.Price)

		if _, err := f.WriteString(output); err != nil {
			fmt.Printf("error writing to file: %v\n", err)
		}
	}

	f.Close()
}
