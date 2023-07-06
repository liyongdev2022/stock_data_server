package db_model

import (
	"go.mongodb.org/mongo-driver/bson/primitive"
	"time"
)

type TickerStruct struct {
	Ticker          string    `bson:"ticker"`
	CompanyName     string    `bson:"company_name"`
	PrimaryExchange string    `bson:"primary_exchange"`
	Active          bool      `bson:"active"`
	LastUpdatedUtc  time.Time `bson:"last_updated_utc"`
	CurrencyName    string    `bson:"currency_name"`
	Locale          string    `bson:"locale"`
	Cik             string    `bson:"cik"`
	CompositeFiGi   string    `bson:"composite_figi"`
	ShareClassFiGi  string    `bson:"share_class_figi"`
}

type StockDataDetail struct {
	Ticker         string               `bson:"ticker"`
	TimeStamp      primitive.Timestamp  `bson:"timestamp"`
	Open           float64              `bson:"open"`
	High           float64              `bson:"high"`
	Low            float64              `bson:"low"`
	Close          float64              `bson:"close"`
	Volume         float64              `bson:"volume"`
	VolumeWeighted primitive.Decimal128 `bson:"volume_weighted"`
	TradeDate      primitive.DateTime   `bson:"trade_date"`
}
