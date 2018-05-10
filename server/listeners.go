package main

import (
	"context"
	"log"
	"time"

	"fmt"

	"math"

	"github.com/krantius/bittrex-data/stats"
	"github.com/olivere/elastic"
)

const (
	Satoshi              = 100000000
	SecondsBetweenPstUtc = 25200
)

type TradeListener struct {
	Market         string   `json:"market"`
	OpenBuys       int      `json:"openBuys"`
	SuccessfulBuys int      `json:"successfulBuys"`
	History        []*Trade `json:"history"`
	c              chan *TradeUpdate
	stats          *stats.CandleStats
}

type Trade struct {
	Market     string    `json:"market"`
	Rate       float32   `json:"rate"`
	TargetRate float32   `json:"targetRate"`
	BuyTime    time.Time `json:"buyTime"`
	SellTime   time.Time `json:"sellTime"`
	Type       string    `json:"type"`
}

func (l *TradeListener) Run(ctx context.Context) {
	fmt.Printf("Starting listener %v\n", l.Market)

	ticker := FiveMinuteTicker()
	vol := float32(0)

	// buying variables
	lastTrade := &Trade{}
	bought := false
	waitForNextTick := false

	lastCandleRate := float32(math.MaxFloat32)
	currentCandleRate := float32(0)

	for {
		select {
		case <-ctx.Done():
			log.Printf("Context cancelled for %s\n", l.Market)
			return
		case tu := <-l.c:
			if len(tu.Data) == 0 {
				fmt.Printf("Got empty trade updates for market %v\n", l.Market)
				continue
			}

			if waitForNextTick {
				continue
			}

			now := time.Now().Unix() - 5 - SecondsBetweenPstUtc

			for _, t := range tu.Data {
				if int64(t.Timestamp) < now {
					continue
				}

				vol += t.Price

				currentCandleRate = t.Rate * Satoshi
			}

			if bought {
				if currentCandleRate > lastTrade.TargetRate {

					lastTrade.SellTime = time.Now()
					fmt.Printf("SUCCESS buy %+v\n", lastTrade)

					l.SuccessfulBuys++
					bought = false
					waitForNextTick = true

					continue
				}
			}

			// Threshold to buy is 5 times the regular volume
			threshold := l.stats.Avg * 7

			// Only buy if:
			//	- volume is greater than the threshold
			//  - the current rate is higher than the last 5 minute candle rate
			//  - we have not already bought
			if vol > threshold && currentCandleRate > lastCandleRate && !bought {
				bought = true
				buyPrice := currentCandleRate
				targetPrice := buyPrice * 1.03

				// Save the trade
				lastTrade = &Trade{
					Market:     l.Market,
					Type:       "buy",
					Rate:       buyPrice,
					TargetRate: targetPrice,
					BuyTime:    time.Now(),
				}

				l.History = append(l.History, lastTrade)

				// SaveTrade(t, elasticClient)
				l.OpenBuys++
				fmt.Printf("BOUGHT %+v\n", lastTrade)
			}
		case <-ticker.C:
			vol = 0.0
			waitForNextTick = false
			lastCandleRate = currentCandleRate
		}
	}
}

func InitListeners(markets []string, m map[string]*stats.CandleStats) {
	for _, market := range markets {
		stats, ok := m[market]
		if !ok {
			fmt.Printf("Market %v did not have any stats\n", market)
			continue
		}

		tl := &TradeListener{
			Market:  market,
			History: []*Trade{},
			c:       make(chan *TradeUpdate, 1000),
			stats:   stats,
		}

		// Fire off the listener
		go tl.Run(context.Background())

		Listeners[market] = tl
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

func FiveMinuteTicker() *time.Ticker {
	c := make(chan time.Time, 1)
	t := &time.Ticker{C: c}
	go func() {
		for {
			n := time.Now()
			if n.Minute()%5 == 0 || n.Minute() == 0 {
				c <- n
			}
			time.Sleep(time.Minute)
		}
	}()
	return t
}
