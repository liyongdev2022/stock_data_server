package main

import (
	"context"
	"encoding/binary"
	"fmt"
	"github.com/go-co-op/gocron"
	polygon "github.com/polygon-io/client-go/rest"
	"github.com/polygon-io/client-go/rest/models"
	"github.com/spf13/viper"
	"github.com/vito-go/mcache"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"log"
	"math"
	"os"
	"stock_data_server/config"
	"stock_data_server/db_model"
	"strconv"
	"strings"
	"sync"
	"time"
)

var (
	// Logger 日志对象
	Logger *log.Logger
	// GConfig 全局配置对象
	GConfig *config.Config
	// AllTicker 股票切换
	AllTicker []*models.Ticker
	// 协程锁
	wg sync.WaitGroup
)

type TickerDetailParams struct {
	Date   time.Time `json:"date"`
	Ticker string    `json:"ticker"`
}

// 初始化
func init() {
	// 捕获panic异常
	defer func() {
		if err := recover(); err != any(nil) {
			fmt.Printf("init func recover panic err:%v", err)
		}
	}()
	initConfig()
}

// 初始化配置文件
func initConfig() {
	v := viper.New()
	v.SetConfigName("config") // 配置文件名称
	v.AddConfigPath(".")      // 从当前目录的哪个文件开始查找
	v.SetConfigType("yaml")   // 配置文件的类型
	err := v.ReadInConfig()   // 读取配置文件
	if err != nil {           // 可以按照这种写法，处理特定的找不到配置文件的情况
		if v, ok := err.(viper.ConfigFileNotFoundError); ok {
			fmt.Println(v)
		} else {
			panic(any(fmt.Errorf("read config err:%v", err)))
		}
	}
	err = v.Unmarshal(&GConfig)
	if err != nil {
		panic(any(fmt.Errorf("viper unmarshal yaml err:%v", err)))
	}
}

// 初始化日志记录器
func initLogger(logFile string) *log.Logger {
	file, err := os.OpenFile(logFile, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		log.Fatalf("无法打开日志文件：%s", err.Error())
	}
	//defer file.Close()
	loggers := log.New(file, "", log.LstdFlags)
	return loggers
}
func main() {
	// 捕获panic异常
	defer func() {
		if err := recover(); err != any(nil) {
			fmt.Printf("main func  recover panic err:%v", err)
		}
	}()

	// 1、初始化日志记录器
	Logger = initLogger(GConfig.LogInfo.LogFile)
	defer Logger.Writer()

	// 2、创建MongoDB客户端
	mongoClient, err := mongo.Connect(context.TODO(), options.Client().ApplyURI(GConfig.MongoInfo.MongoURL))
	if err != nil {
		Logger.Fatalf("create mongo client err:%v\n", err)
	}

	// 3、创建Polygon客户端
	polygonClient := polygon.New(GConfig.ApiInfo.ApiKey)

	// 4、获取指定交易所所有股票交易状态
	getExchangeAllTickerActive(polygonClient, mongoClient)

	// 定时任务规则
	timezone, _ := time.LoadLocation("Asia/Shanghai")
	s := gocron.NewScheduler(timezone)

	// 定时任务配置，执行func

	s.StartBlocking()
}

// 获取指定交易所所有股票交易状态
func getExchangeAllTickerActive(polygonClient *polygon.Client, mongoClient *mongo.Client) {
	// market 类型
	market := models.AssetStocks

	// 声明时区
	loc, err := time.LoadLocation(GConfig.StockInfo.TimeZone)
	if err != nil {
		Logger.Printf("time loadLocation timeZone err:%v", err)
		return
	}

	// 股票切片
	tickerArray := strings.Split(GConfig.StockInfo.Ticker, ",")

	// 声明一个通道结构体进行传参
	chParams := make(chan TickerDetailParams, 100)

	beginDate, err := time.Parse("2006-01-02 15:04:05", GConfig.StockInfo.BeginDate+" 00:00:00")
	if err != nil {
		Logger.Printf("beginDate time parse err:%v", err)
		return
	}
	endDate, err := time.Parse("2006-01-02 15:04:05", GConfig.StockInfo.EndDate+" 00:00:00")
	if err != nil {
		Logger.Printf("endDate time parse err:%v", err)
		return
	}

	// 遍历从开始日期到结束日期
	for date := beginDate; date.Before(endDate) || date.Equal(endDate); date = date.AddDate(0, 0, 1) {

		// 时间类型转换
		dateTicker := models.Date(time.Date(date.Year(), date.Month(), date.Day(), date.Hour(), date.Minute(), date.Second(), date.Nanosecond(), loc))
		// 请求参数
		params := models.ListTickersParams{
			Exchange: &GConfig.StockInfo.Market,
			Market:   &market,
			Date:     &dateTicker,
		}.WithOrder(models.Asc).WithLimit(120)

		// 开始调用API接口：https://polygon.io/docs/stocks/get_v3_reference_tickers
		allTicker := polygonClient.ListTickers(context.TODO(), params)

		// 判断请求返回结果是否异常
		if allTicker.Err() != nil {
			Logger.Printf("get list tickers info err:%v", err)
			return
		}

		// 过滤需要股票信息
		if len(tickerArray) > 0 && tickerArray[0] != "" {
			fmt.Println("one ")
			// 过滤指定交易所下的【指定股票】交易状态
			for allTicker.Next() {
				item := models.Ticker(allTicker.Item())
				//fmt.Println("item ticker name==>", item.Ticker)
				for _, v := range tickerArray {
					//fmt.Println("vv==>", v)
					if v == item.Ticker {
						AllTicker = append(AllTicker, &item)
						if !item.Active {
							// 没有开票的股票进行记录，保存到MongoDB中
							err = saveTickerNoActiveByDate(item, mongoClient)
							if err != nil {
								Logger.Printf("save ticker no active by date err:%v", err)
							}
						} else {
							wg.Add(1)
							// 发送参数
							chParams <- TickerDetailParams{
								Date:   date,
								Ticker: v,
							}
							// 开始调用协程，执行获取指定日期股票交易历史数据
							go fetchTickerDetail(chParams, wg, polygonClient, mongoClient)
						}
					}
				}
			}
		} else {
			// 获取指定交易所下的【所有股票】交易状态
			fmt.Println("all ")
			for allTicker.Next() {
				item := models.Ticker(allTicker.Item())
				AllTicker = append(AllTicker, &item)
				if !item.Active {
					// 没有开票的股票进行记录，保存到MongoDB中
					err = saveTickerNoActiveByDate(item, mongoClient)
					if err != nil {
						Logger.Printf("save ticker no active by date err:%v", err)
					}
				} else {
					wg.Add(1)
					// 发送参数
					chParams <- TickerDetailParams{
						Date:   date,
						Ticker: item.Ticker,
					}
					// 开始调用协程，执行获取指定日期股票交易历史数据
					go fetchTickerDetail(chParams, wg, polygonClient, mongoClient)
				}
			}
		}

		// 防止过快请求polygon站点API接口
		time.Sleep(1 * time.Second)

	}
	wg.Wait()

	fmt.Println("all-ticker==>", len(AllTicker))

}

// 获取指定日期股票交易历史数据
func fetchTickerDetail(ch chan TickerDetailParams, wg sync.WaitGroup, polygonClient *polygon.Client, mongoClient *mongo.Client) {
	defer wg.Done()

	// 获取通道参数
	chParams := <-ch

	// 先获取本地文件缓存标记，判断指定日期股票的数据是否抓取完整
	key := GConfig.StockInfo.Market + "_" + chParams.Date.Format("20060102")

	// MongoDB集合名
	name := "stock_data_" + strconv.Itoa(GConfig.StockInfo.Multiplier) + "min"

	// 获取本地文件缓存标记
	preFlag, errs := getLocalCache(chParams.Ticker, key)
	if errs != nil {
		Logger.Printf("get local cache data err:%v", errs)
	}

	var err error
	var flag bool
	if preFlag == "0" {
		// 抓取数据未完整，删除已存在历史数据
		errs = delTickerHistoryDataBydDate(chParams.Ticker, name, chParams.Date, mongoClient)
		if errs != nil {
			Logger.Printf("delete ticker history data err:%v", errs)
			return
		}
		// 重新抓取
		flag = true

	} else if preFlag == "" {
		// 首次抓取
		flag = true
	}

	// 首次抓取/重新抓取
	if flag {

		// 构造请求参数
		params := models.ListAggsParams{
			Ticker:     chParams.Ticker,
			From:       models.Millis(time.UnixMilli(chParams.Date.UnixMilli() - 8*60*60*1000)),
			To:         models.Millis(time.UnixMilli(chParams.Date.UnixMilli() - 8*60*60*1000)),
			Multiplier: GConfig.StockInfo.Multiplier,
			Timespan:   models.Timespan(GConfig.StockInfo.Timespan),
		}.WithOrder(models.Asc).WithLimit(5000)

		// 开始请求股票API接口
		iterResult := polygonClient.ListAggs(context.TODO(), params)

		if iterResult.Err() != nil {
			// 更新本地文件缓存标记, 请求报错
			setLocalCache(chParams.Ticker, key, "-1")
		}

		for iterResult.Next() {
			fmt.Println("---------------------------------------------------------------------")
			err = saveTickerHistoryDataByDate(models.Agg(iterResult.Item()), chParams.Ticker, mongoClient)
			if err != nil {
				Logger.Printf("save ticker history data err:%v", err)
				break
			}
		}
		if err != nil {
			// 更新本地文件缓存标记, 未完成
			setLocalCache(chParams.Ticker, key, "0")
		} else {
			// 更新本地文件缓存标记, 已完成
			setLocalCache(chParams.Ticker, key, "1")
		}
	}

}

// 保存指定日期股票未开盘信息
func saveTickerNoActiveByDate(model models.Ticker, mongoClient *mongo.Client) error {
	item := db_model.TickerStruct{
		Ticker:          model.Ticker,
		CompanyName:     model.Name,
		PrimaryExchange: model.PrimaryExchange,
		Active:          model.Active,
		LastUpdatedUtc:  time.Time(model.LastUpdatedUTC),
		CurrencyName:    model.CurrencyName,
		Locale:          model.Locale,
		Cik:             model.CIK,
		CompositeFiGi:   model.CompositeFIGI,
		ShareClassFiGi:  model.ShareClassFIGI,
	}
	collection := mongoClient.Database(GConfig.MongoInfo.MongoDB).Collection("stock_tickers_history")
	_, err := collection.InsertOne(context.TODO(), item)
	return err
}

// 保存指定日期股票历史交易数据
func saveTickerHistoryDataByDate(model models.Agg, ticker string, mongoClient *mongo.Client) error {

	vwap, err := ConvertToDecimal128(model.VWAP)
	if err != nil {
		return err
	}
	item := db_model.StockDataDetail{
		Ticker:         ticker,
		TimeStamp:      primitive.Timestamp{T: uint32(time.Time(model.Timestamp).Unix()), I: 0},
		Open:           model.Open,
		High:           model.High,
		Low:            model.Low,
		Close:          model.Close,
		Volume:         model.Volume,
		VolumeWeighted: vwap,
		TradeDate:      primitive.NewDateTimeFromTime(time.Time(model.Timestamp)), // 不确定是不是需要转换一下时区？
	}
	name := "stock_data_" + strconv.Itoa(GConfig.StockInfo.Multiplier) + "min"
	collection := mongoClient.Database(GConfig.MongoInfo.MongoDB).Collection(name)
	_, err = collection.InsertOne(context.TODO(), item)
	return err
}

// 删除指定日期股票历史交易数据
func delTickerHistoryDataBydDate(ticker string, collectionName string, date time.Time, mongoClient *mongo.Client) error {
	c := mongoClient.Database(GConfig.MongoInfo.MongoDB).Collection(collectionName)
	_, err := c.DeleteMany(context.TODO(), bson.D{{"ticker", ticker}, {"trade_date", date.Format("2006-01-02")}})
	return err
}

// 设置本地缓存
func setLocalCache(ticker string, key string, value string) error {
	c, err := mcache.NewMcache(ticker)
	if err != nil {
		return err
	}
	err = c.Set(key, []byte(value))
	return err

}

// 获取本地缓存
func getLocalCache(ticker string, key string) (string, error) {
	c, err := mcache.NewMcache(ticker)
	if err != nil {
		return "", err
	}
	return string(c.Get(key)), nil
}

//Float64ToByte Float64转byte
func Float64ToByte(float float64) []byte {
	bits := math.Float64bits(float)
	bytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(bytes, bits)
	return bytes
}

//ByteToFloat64 byte转Float64
func ByteToFloat64(bytes []byte) float64 {
	bits := binary.LittleEndian.Uint64(bytes)
	return math.Float64frombits(bits)
}

func ConvertToDecimal128(value float64) (primitive.Decimal128, error) {
	stringValue := strconv.FormatFloat(value, 'f', -1, 64)
	decimalValue, err := primitive.ParseDecimal128(stringValue)
	if err != nil {
		return primitive.Decimal128{}, err
	}

	return decimalValue, nil
}
