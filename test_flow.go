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
	"golang.org/x/exp/maps"

	_ "github.com/go-sql-driver/mysql"
)

type Customer struct {
	Id            int64
	FirstName     string
	LastName      string
	Freq          int
	NextOrderDay  int
	OrderCount    int
	LastOrderTime int64
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
			customers = append(customers, &Customer{
				FirstName:    newCustomer.FirstName,
				LastName:     newCustomer.LastName,
				Freq:         3 + random.Intn(10),
				NextOrderDay: day + random.Intn(5),
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
				_, err := conn.ExecContext(ctx, "INSERT INTO order_tab (customer_id, order_time, create_time) VALUES (?, ?, ?)", c.Id, orderTime.UnixMilli(), createTime.UnixMilli())
				if err != nil {
					log.Fatal(err)
				}
				c.NextOrderDay = c.NextOrderDay + c.Freq + random.Intn(5) - 2
				c.OrderCount += 1
				c.LastOrderTime = orderTime.UnixMilli()
				newOrderCount++
			}
		}
		log.Printf("new customer count: %d, new order count: %d\n", newCustomerCount, newOrderCount)
		select {
		case <-ctx.Done():
			return
		case <-time.After(5 * time.Second):
		}
		chunkSize := 100
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
			sql := "SELECT customer_id, first_name, last_name, order_count, last_order_time FROM customer_reorder_tab WHERE customer_id IN (" + strings.Join(placeholders, ",") + ")"
			rows, err := conn.QueryContext(ctx, sql, ids...)
			if err != nil {
				log.Fatal(err)
			}
			var row Customer
			for rows.Next() {
				if err := rows.Scan(&row.Id, &row.FirstName, &row.LastName, &row.OrderCount, &row.LastOrderTime); err != nil {
					log.Fatal(err)
				}
				c := customerMap[row.Id]
				if c.FirstName != row.FirstName || c.LastName != c.LastName || c.OrderCount != row.OrderCount || c.LastOrderTime != row.LastOrderTime {
					log.Fatalf("%+v != %+v", c, row)
				}
				delete(idSet, row.Id)
			}
			if len(idSet) > 0 {
				log.Fatalf("some customer_reorder info not found: %+v", maps.Keys(idSet))
			}
		}
	}
}
