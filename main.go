package main

import (
	"context"
	"fmt"
	"github.com/alpacahq/alpaca-trade-api-go/v3/alpaca"
	"github.com/shopspring/decimal"
	"github.com/spf13/viper"
	"go.uber.org/zap"
	"gorm.io/driver/postgres"
	_ "gorm.io/driver/postgres"
	"gorm.io/gorm"
	"time"
)

const baseURL = "https://paper-api.alpaca.markets"

type serverConfig struct {
	Host   string `mapstructure:"host"`
	User   string `mapstructure:"user"`
	Pass   string `mapstructure:"pass"`
	DBName string `mapstructure:"dbname"`
}

type TradeUpdate struct {
	At          time.Time        `json:"at"`
	Event       string           `json:"event"`
	ExecutionID string           `json:"execution_id"`
	PositionQty *decimal.Decimal `json:"position_qty"`
	Price       *decimal.Decimal `json:"price"`
	Qty         *decimal.Decimal `json:"qty"`
	Timestamp   *time.Time       `json:"timestamp"`
}

var log, _ = zap.NewProduction()

var db *gorm.DB
var client *alpaca.Client

func init() {
	var err error

	var postgresConfig serverConfig

	viper.AddConfigPath(".")
	viper.SetConfigName("postgres")
	viper.SetConfigType("env")

	if err = viper.ReadInConfig(); err != nil {
		log.Fatal("Error reading env file", zap.Error(err))
	}

	if err = viper.Unmarshal(&postgresConfig); err != nil {
		log.Fatal("", zap.Error(err))
	}

	dsn := fmt.Sprintf("host=%s user=%s password=%s dbname=%s",
		postgresConfig.Host,
		postgresConfig.User,
		postgresConfig.Pass, postgresConfig.DBName,
	)

	log.Info("Connecting to database")
	db, err = gorm.Open(postgres.Open(dsn), &gorm.Config{})
	if err != nil {
		panic("failed to connect database")
	}

	log.Info("Migrating database")
	err = db.AutoMigrate(&TradeUpdate{})
	if err != nil {
		panic("failed to migrate database")
	}

	log.Info("Connecting to Alpaca")
	client = alpaca.NewClient(alpaca.ClientOpts{
		BaseURL: baseURL,
	})

}

func tradeUpdates(tu alpaca.TradeUpdate) {
	if tu.Event == "new" {
		log.Info("New Order received", zap.String("order", tu.Order.ID))
	}

	if tu.Event == "fill" {
		log.Info("Add Entry to database")
		update := TradeUpdate{
			At:          tu.At,
			Event:       tu.Event,
			ExecutionID: tu.ExecutionID,
			PositionQty: tu.PositionQty,
			Price:       tu.Price,
			Qty:         tu.Qty,
			Timestamp:   tu.Timestamp,
		}
		db.Create(&update)
	}

}

func main() {
	defer log.Sync()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	client.StreamTradeUpdatesInBackground(ctx, tradeUpdates)

	<-ctx.Done()
}
