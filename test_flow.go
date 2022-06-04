package main

import (
	"context"
	"database/sql"
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
		newCustomerCount := random.Intn(100)
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
		waitTime := 3 * time.Second
		select {
		case <-ctx.Done():
			return
		case <-time.After(waitTime):
		}
		chunkSize := 100
		mismatchCount := 0
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
				if c.FirstName != row.FirstName ||
					c.LastName != c.LastName ||
					c.TestData.OrderCount != row.TestData.OrderCount ||
					c.TestData.LastOrderTime != row.TestData.LastOrderTime ||
					c.TestData.ExpectedNextOrderTime != row.TestData.ExpectedNextOrderTime {
					mismatchCount++
				}
				delete(idSet, row.Id)
			}
			mismatchCount += len(idSet)
		}
		log.Printf("after waiting %v, mismatch count: %d\n", waitTime, mismatchCount)
	}
}
