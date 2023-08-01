package main

import (
	"fmt"
	"github.com/alpacahq/alpaca-trade-api-go/v3/alpaca"
	"github.com/shopspring/decimal"
	"github.com/spf13/viper"
	"go.uber.org/zap"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"time"
)

type serverConfig struct {
	Host   string `mapstructure:"host"`
	User   string `mapstructure:"user"`
	Pass   string `mapstructure:"pass"`
	DBName string `mapstructure:"dbname"`
}

type Order struct {
	gorm.Model
	ID            string     `json:"id"`
	ClientOrderID string     `json:"client_order_id"`
	CreatedAt     time.Time  `json:"created_at"`
	UpdatedAt     time.Time  `json:"updated_at"`
	SubmittedAt   time.Time  `json:"submitted_at"`
	FilledAt      *time.Time `json:"filled_at"`
	ExpiredAt     *time.Time `json:"expired_at"`
	CanceledAt    *time.Time `json:"canceled_at"`
	FailedAt      *time.Time `json:"failed_at"`
	//ReplacedAt     *time.Time         `json:"replaced_at"`
	//ReplacedBy     *string            `json:"replaced_by"`
	//Replaces       *string            `json:"replaces"`
	AssetID     string             `json:"asset_id"`
	Symbol      string             `json:"symbol"`
	AssetClass  alpaca.AssetClass  `json:"asset_class"`
	OrderClass  alpaca.OrderClass  `json:"order_class"`
	Type        alpaca.OrderType   `json:"type"`
	Side        alpaca.Side        `json:"side"`
	TimeInForce alpaca.TimeInForce `json:"time_in_force"`
	Status      string             `json:"status"`
	//Notional       *decimal.Decimal   `json:"notional"`
	Qty            *decimal.Decimal `json:"qty"`
	FilledQty      decimal.Decimal  `json:"filled_qty"`
	FilledAvgPrice *decimal.Decimal `json:"filled_avg_price"`
	LimitPrice     *decimal.Decimal `json:"limit_price"`
	StopPrice      *decimal.Decimal `json:"stop_price"`
	//TrailPrice     *decimal.Decimal   `json:"trail_price"`
	//TrailPercent   *decimal.Decimal   `json:"trail_percent"`
	//HWM            *decimal.Decimal   `json:"hwm"`
	//ExtendedHours  bool               `json:"extended_hours"`
	Legs []Order `json:"legs" gorm:"foreignKey:ID"`
}

type TradeUpdate struct {
	gorm.Model
	At          time.Time        `json:"at"`
	Event       string           `json:"event"`
	ExecutionID string           `json:"execution_id"`
	PositionQty *decimal.Decimal `json:"position_qty"`
	Price       *decimal.Decimal `json:"price"`
	Qty         *decimal.Decimal `json:"qty"`
	Timestamp   *time.Time       `json:"timestamp"`
	OrderID     string
	Order       Order `json:"order" gorm:"references:ID"`
}

type DBHandle struct {
	db *gorm.DB
}

func ConnectDatabase(updates interface{}) *DBHandle {

	db := new(gorm.DB)

	var postgresConfig serverConfig

	viper.AddConfigPath(".")
	viper.SetConfigName("postgres")
	viper.SetConfigType("env")

	if err := viper.ReadInConfig(); err != nil {
		log.Fatal("Error reading env file", zap.Error(err))
	}

	if err := viper.Unmarshal(&postgresConfig); err != nil {
		log.Fatal("", zap.Error(err))
	}

	dsn := fmt.Sprintf("host=%s user=%s password=%s dbname=%s",
		postgresConfig.Host,
		postgresConfig.User,
		postgresConfig.Pass, postgresConfig.DBName,
	)

	log.Info("Connecting to database")
	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{})
	if err != nil {
		panic("failed to connect database")
	}

	log.Info("Migrating database")
	err = db.AutoMigrate(updates, &Order{})
	if err != nil {
		panic("failed to migrate database")
	}

	return &DBHandle{db: db}
}

func fillUpdate(myField *TradeUpdate, tu alpaca.TradeUpdate) {
	myField.At = tu.At
	myField.Event = tu.Event
	myField.ExecutionID = tu.ExecutionID
	myField.PositionQty = tu.PositionQty
	myField.Price = tu.Price
	myField.Qty = tu.Qty
	myField.Timestamp = tu.Timestamp
	//myField.Order = tu.Order
}

func (dh *DBHandle) handleUpdates(tu alpaca.TradeUpdate) {
	if tu.Event == "new" {
		log.Info("New Order received", zap.String("order", tu.Order.ID))
		update := TradeUpdate{}
		fillUpdate(&update, tu)
		dh.db.Create(&update)
	}

	if tu.Event == "fill" {
		log.Info("Order filled", zap.String("order", tu.Order.ID))
		update := TradeUpdate{}
		fillUpdate(&update, tu)
		dh.db.Create(&update)
	}
}
