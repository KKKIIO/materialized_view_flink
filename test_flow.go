package main

import (
	"context"
	"database/sql"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"time"

	"github.com/brianvoe/gofakeit/v6"

	_ "github.com/go-sql-driver/mysql"
)

func main() {
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
	for {
		newCustomer := gofakeit.Person()
		_,err:=conn.ExecContext(ctx, "INSERT INTO customer_tab (first_name, last_name) VALUES (?, ?)", newCustomer.FirstName, newCustomer.LastName)
		if err != nil {
			panic(err)
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(time.Duration(rand.Intn(10)) * 100 * time.Millisecond):
		}
	}
}
