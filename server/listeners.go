package main

import (
	"context"
	"log"
	"time"

	"fmt"

	"github.com/krantius/bittrex-data/stats"
	"github.com/olivere/elastic"
)

type TradeListener struct {
	C      chan *TradeUpdate
	Market string
	Stats  *stats.CandleStats
}

type Trade struct {
	Market string    `json:"market"`
	Price  float32   `json:"price"`
	Time   time.Time `json:"time"`
	Type   string    `json:"type"`
}

func (l *TradeListener) Run(ctx context.Context) {
	t := MinuteTicker()
	vol := float32(0)
	alerted := false

	for {
		select {
		case <-ctx.Done():
			log.Printf("Context cancelled for %s\n", l.Market)
			return
		case tu := <-l.C:
			for _, t := range tu.Data {
				vol += t.Price
			}

			// if it's greater than the average, alert?
			threshold := l.Stats.Avg * 5
			if vol > threshold && !alerted {
				alerted = true
				log.Printf("Market %s passed avg vol threshold: %v > %v\n", l.Market, vol, threshold)

				// reset in an hour lul
				go func(a *bool) {
					time.Sleep(1 * time.Hour)
					log.Printf("Market %s woke up after 1 hour\n", l.Market)
					*a = false
				}(&alerted)
			}

		case <-t.C:
			vol = 0.0
		}
	}
}

func SaveTrade(t Trade, client *elastic.Client) {
	ctx := context.Background()
	_, err := client.Index().Index("bittrex-trades").Type("trade").BodyJson(t).Do(ctx)
	if err != nil {
		fmt.Printf("error inserting: %v\n", err)
		return
	}
}

func MinuteTicker() *time.Ticker {
	c := make(chan time.Time, 1)
	t := &time.Ticker{C: c}
	go func() {
		for {
			n := time.Now()
			if n.Second() == 0 {
				c <- n
			}
			time.Sleep(time.Second)
		}
	}()
	return t
}
