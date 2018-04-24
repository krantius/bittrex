package main

import (
	"flag"
	"log"
	"net/http"

	"encoding/json"
	"fmt"

	"os"

	"github.com/gorilla/websocket"
)

var addr = flag.String("addr", "localhost:8080", "http service address")

var upgrader = websocket.Upgrader{} // use default options

func echo(w http.ResponseWriter, r *http.Request) {
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

		var tradeUpdate TradeUpdate
		if err := json.Unmarshal(message, &tradeUpdate); err != nil {
			fmt.Printf("error unmarshalling trade update: %v\n", err)
		}

		f, err := os.OpenFile("./output.txt", os.O_APPEND|os.O_WRONLY, os.ModeAppend)
		if err != nil {
			fmt.Printf("error opening file: %v\n", err)
		}

		for _, trade := range tradeUpdate.Data {
			/*if trade.Price < 1 {
				continue
			}*/

			output := fmt.Sprintf("%v %s with %s order %v\n", trade.Timestamp, tradeUpdate.Pair, trade.OrderType, trade.Price)

			if _, err := f.WriteString(output); err != nil {
				fmt.Printf("error writing to file: %v\n", err)
			}
		}

		f.Close()

		if err != nil {
			log.Println("write:", err)
			break
		}
	}
}

type TradeUpdate struct {
	Pair string      `json:"pair"`
	Data []TradeData `json:"data"`
}

type TradeData struct {
	Id        int     `json"id"`
	Quantity  float32 `json:"quantity"`
	Rate      float32 `json"rate"`
	Price     float32 `json:"price"`
	OrderType string  `json:"orderType"`
	Timestamp float32 `json:"timestamp"`
}

func main() {
	_, err := os.Create("./output.txt")
	if err != nil {
		fmt.Printf("failed to create output file: %v\n", err)
	}

	flag.Parse()
	log.SetFlags(0)
	http.HandleFunc("/ws", echo)
	log.Fatal(http.ListenAndServe(*addr, nil))
}
