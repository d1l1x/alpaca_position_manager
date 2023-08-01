package main

import (
	"context"
	"github.com/alpacahq/alpaca-trade-api-go/v3/alpaca"
	"go.uber.org/zap"
	_ "gorm.io/driver/postgres"
)

const baseURL = "https://paper-api.alpaca.markets"

var log, _ = zap.NewProduction()
var client *alpaca.Client

// MyTradeUpdate Important as table name is derived from type name
type MyTradeUpdate TradeUpdate

func init() {
	log.Info("Connecting to Alpaca")
	client = alpaca.NewClient(alpaca.ClientOpts{
		BaseURL: baseURL,
	})
}

func main() {
	defer log.Sync()

	db := ConnectDatabase(&MyTradeUpdate{})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	client.StreamTradeUpdatesInBackground(ctx, db.handleUpdates)

	<-ctx.Done()
}
