package db_model

import "time"

type TickerStruct struct {
	Ticker          string    `bson:"ticker" json:"ticker"`
	CompanyName     string    `bson:"company_name" json:"company_name"`
	PrimaryExchange string    `bson:"primary_exchange" json:"primary_exchange"`
	Active          bool      `bson:"active" json:"active"`
	LastUpdatedUtc  time.Time `bson:"last_updated_utc" json:"last_updated_utc"`
	CurrencyName    string    `bson:"currency_name" json:"currency_name"`
	Locale          string    `bson:"locale" json:"locale"`
	Cik             string    `bson:"cik" json:"cik"`
	CompositeFiGi   string    `bson:"composite_figi" json:"composite_figi"`
	ShareClassFiGi  string    `bson:"share_class_fi_gi" json:"share_class_fi_gi"`
}

type StockDataDetail struct {
	Ticker         string    `bson:"ticker" json:"ticker"`
	TimeStamp      time.Time `bson:"time_stamp" json:"time_stamp"`
	Open           float64   `bson:"open" json:"open"`
	High           float64   `bson:"high" json:"high"`
	Low            float64   `bson:"low" json:"low"`
	Close          float64   `bson:"close" json:"close"`
	Volume         float64   `bson:"volume" json:"volume"`
	VolumeWeighted float64   `bson:"volume_weighted" json:"volume_weighted"`
	TradeDate      time.Time `bson:"trade_date" json:"trade_date"`
}
