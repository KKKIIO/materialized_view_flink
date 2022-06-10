package main

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"strings"
	"time"

	"github.com/brianvoe/gofakeit/v6"

	_ "github.com/go-sql-driver/mysql"
)

type Customer struct {
	Id            int64
	FirstName     string
	LastName      string
	Freq          int
	FirstOrderDay int
	NextOrderDay  int
	TestData      struct {
		OrderCount            int
		LastOrderTime         int64
		ExpectedNextOrderTime int64
	}
}

func main() {
	var lag int
	var maxNewUserCount int
	var verbose bool
	flag.IntVar(&lag, "lag", 3, "lag in seconds")
	flag.IntVar(&maxNewUserCount, "max-new-user", 100, "max new user count")
	flag.BoolVar(&verbose, "verbose", false, "verbose log")
	flag.Parse()
	if lag < 0 {
		flag.Usage()
		return
	}
	seed := time.Now().UnixNano()
	faker := gofakeit.New(seed)
	random := rand.New(rand.NewSource(seed))
	log.Printf("Seed: %d", seed)
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Kill)
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		<-signals
		cancel()
	}()
	db, err := sql.Open("mysql", "root:123456@/demo")
	if err != nil {
		log.Fatal(err)
	}
	conn, err := db.Conn(ctx)
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()
	if _, err := conn.ExecContext(ctx, "TRUNCATE TABLE customer_tab"); err != nil {
		log.Fatal(err)
	}
	if _, err := conn.ExecContext(ctx, "TRUNCATE TABLE order_tab"); err != nil {
		log.Fatal(err)
	}
	if _, err := conn.ExecContext(ctx, "TRUNCATE TABLE customer_reorder_tab"); err != nil {
		log.Fatal(err)
	}
	customers := make([]*Customer, 0)
	customerMap := make(map[int64]*Customer)
	startDay := time.Now()
	for day := 0; ; day++ {
		log.Printf("Day %d\n", day)
		newCustomerCount := random.Intn(maxNewUserCount)
		for i := 0; i < newCustomerCount; i++ {
			newCustomer := faker.Person()
			firstOrderDay := day + random.Intn(5)
			customers = append(customers, &Customer{
				FirstName:     newCustomer.FirstName,
				LastName:      newCustomer.LastName,
				Freq:          3 + random.Intn(10),
				FirstOrderDay: firstOrderDay,
				NextOrderDay:  firstOrderDay,
			})
		}
		random.Shuffle(len(customers), func(i, j int) {
			customers[i], customers[j] = customers[j], customers[i]
		})
		newOrderCount := 0
		for _, c := range customers {
			if c.Id == 0 {
				res, err := conn.ExecContext(ctx, "INSERT INTO customer_tab (first_name, last_name) VALUES (?, ?)", c.FirstName, c.LastName)
				if err != nil {
					log.Fatal(err)
				}
				id, err := res.LastInsertId()
				if err != nil {
					log.Fatal(err)
				}
				c.Id = id
				customerMap[c.Id] = c
			}
			if day+3 >= c.NextOrderDay {
				orderTime := startDay.AddDate(0, 0, c.NextOrderDay)
				createTime := startDay.AddDate(0, 0, day)
				if _, err := conn.ExecContext(ctx, "INSERT INTO order_tab (customer_id, order_time, create_time) VALUES (?, ?, ?)", c.Id, orderTime.UnixMilli(), createTime.UnixMilli()); err != nil {
					log.Fatal(err)
				}
				c.TestData.OrderCount += 1
				calcFreq := (c.NextOrderDay - c.FirstOrderDay) / c.TestData.OrderCount
				c.NextOrderDay = c.NextOrderDay + c.Freq + random.Intn(5) - 2
				c.TestData.LastOrderTime = orderTime.UnixMilli()
				if calcFreq > 0 {
					c.TestData.ExpectedNextOrderTime = orderTime.AddDate(0, 0, calcFreq).UnixMilli()
				}
				newOrderCount++
				if _, err := conn.ExecContext(ctx, "INSERT INTO customer_preference_tab (customer_id, frequency) VALUES (?, ?) ON DUPLICATE KEY UPDATE frequency=VALUES(frequency)", c.Id, calcFreq); err != nil {
					log.Fatal(err)
				}
			}
		}
		log.Printf("new customer count: %d, new order count: %d\n", newCustomerCount, newOrderCount)
		waitTime := time.Duration(lag) * time.Second
		select {
		case <-ctx.Done():
			return
		case <-time.After(waitTime):
		}
		chunkSize := 100
		var mismatchCount int
		sampleSize := 10
		mismatchSamples := make([]string, 0, sampleSize)
		for i := 0; i < len(customers); i += chunkSize {
			end := i + chunkSize
			if end > len(customers) {
				end = len(customers)
			}
			ids := make([]interface{}, end-i)
			placeholders := make([]string, len(ids))
			idSet := make(map[int64]bool)
			for j := i; j < end; j++ {
				ids[j-i] = customers[j].Id
				placeholders[j-i] = "?"
				idSet[customers[j].Id] = true
			}
			sql := "SELECT customer_id, first_name, last_name, order_count, last_order_time, expected_next_order_time FROM customer_reorder_tab WHERE customer_id IN (" + strings.Join(placeholders, ",") + ")"
			rows, err := conn.QueryContext(ctx, sql, ids...)
			if err != nil {
				log.Fatal(err)
			}
			var row Customer
			for rows.Next() {
				if err := rows.Scan(&row.Id, &row.FirstName, &row.LastName, &row.TestData.OrderCount, &row.TestData.LastOrderTime, &row.TestData.ExpectedNextOrderTime); err != nil {
					log.Fatal(err)
				}
				c := customerMap[row.Id]
				var mismatch string
				if c.FirstName != row.FirstName {
					mismatch = fmt.Sprintf("#%d, exp.FirstName(%s)!=row.FirstName(%s)", row.Id, c.FirstName, row.FirstName)
				} else if c.LastName != row.LastName {
					mismatch = fmt.Sprintf("#%d, exp.LastName(%s)!=row.LastName(%s)", row.Id, c.LastName, row.LastName)
				} else if c.TestData.OrderCount != row.TestData.OrderCount {
					mismatch = fmt.Sprintf("#%d, exp.OrderCount(%d)!=row.OrderCount(%d)", row.Id, c.TestData.OrderCount, row.TestData.OrderCount)
				} else if c.TestData.LastOrderTime != row.TestData.LastOrderTime {
					mismatch = fmt.Sprintf("#%d, exp.LastOrderTime(%d)!=row.LastOrderTime(%d)", row.Id, c.TestData.LastOrderTime, row.TestData.LastOrderTime)
				} else if c.TestData.ExpectedNextOrderTime != row.TestData.ExpectedNextOrderTime {
					mismatch = fmt.Sprintf("#%d, exp.ExpectedNextOrderTime(%d)!=row.ExpectedNextOrderTime(%d)", row.Id, c.TestData.ExpectedNextOrderTime, row.TestData.ExpectedNextOrderTime)
				}
				if mismatch != "" {
					mismatchCount++
					if len(mismatchSamples) < sampleSize {
						mismatchSamples = append(mismatchSamples, mismatch)
					} else if i := random.Intn(mismatchCount); i < sampleSize {
						mismatchSamples[i] = mismatch
					}
				}
				delete(idSet, row.Id)
			}
			for k := range idSet {
				mismatchCount++
				mismatch := fmt.Sprintf("#%d, not found", k)
				if len(mismatchSamples) < sampleSize {
					mismatchSamples = append(mismatchSamples, mismatch)
				} else if i := random.Intn(mismatchCount); i < sampleSize {
					mismatchSamples[i] = mismatch
				}
			}
		}
		log.Printf("after waiting %v, mismatch count: %d\n", waitTime, mismatchCount)
		if verbose && len(mismatchSamples) > 0 {
			log.Printf("mismatch samples:\n")
			for _, s := range mismatchSamples {
				log.Printf("%s\n", s)
			}
		}
	}
}
