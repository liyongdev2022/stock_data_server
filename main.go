package main

import (
	"context"
	"fmt"
	"github.com/go-co-op/gocron"
	polygon "github.com/polygon-io/client-go/rest"
	"github.com/polygon-io/client-go/rest/models"
	"github.com/spf13/viper"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"log"
	"os"
	"stock_data_server/config"
	"strings"
	"time"
)

var (
	// Logger 日志对象
	Logger *log.Logger
	// GConfig 全局配置对象
	GConfig   *config.Config
	AllTicker []*models.Ticker
)

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
	market := models.AssetStocks
	loc, _ := time.LoadLocation(GConfig.StockInfo.TimeZone)
	tickerArray := strings.Split(GConfig.StockInfo.Ticker, ",")

	beginDate, err := time.ParseInLocation("2006-01-02 15:04:05", GConfig.StockInfo.BeginDate+" 00:00:00", loc)
	if err != nil {
		Logger.Printf("beginDate time parse err:%v", err)
		return
	}
	endDate, err := time.ParseInLocation("2006-01-02 15:04:05", GConfig.StockInfo.EndDate+" 00:00:00", loc)
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
				}
			}
		}

		// 防止过快请求polygon站点API接口
		time.Sleep(1 * time.Second)

	}

	fmt.Println("all-ticker==>", len(AllTicker))

}
