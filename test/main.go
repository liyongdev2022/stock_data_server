package main

import (
	"fmt"
	"time"
)

func main() {
	// 获取当前时间
	now := time.Now()

	// 输出当前时间
	fmt.Println("当前时间：", now)

	// 设置要使用的时区
	location, err := time.LoadLocation("America/New_York")
	if err != nil {
		fmt.Println(err)
		return
	}

	// 将时间转换为指定时区的时间
	nyTime := now.In(location)

	// 输出指定时区的时间
	fmt.Println("纽约时间：", nyTime)
}
