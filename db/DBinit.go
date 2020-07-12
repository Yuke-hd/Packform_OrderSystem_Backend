package main

import (
	"database/sql"
	"encoding/csv"
	"fmt"
	"github.com/lib/pq"
	"io"
	"log"
	"os"
)

const (
	DB_DSN            = "postgres://postgres:123056@localhost:5000/postgres?sslmode=disable"
	orders            = "Test task - Postgres - orders.csv"
	orderitems        = "Test task - Postgres - order_items.csv"
	deliveries        = "Test task - Postgres - deliveries.csv"
	customers         = "Test task - Mongo - customers.csv"
	customercompanies = "Test task - Mongo - customer_companies.csv"
)

func main() {
	creatTable()
	insertIntoOrders()
	insertIntoOrderItem()
	insertIntoDeliveries()
	insertIntoCustomers()
	insertIntoCustomerCompanies()
}

func creatTable() {
	db, err := sql.Open("postgres", DB_DSN)
	if err != nil {
		log.Print("Failed to open a DB connection: ", err)
	}
	defer db.Close()

	// create orders
	_, err = db.Query("create table if not exists orders(id integer not null primary key,created_at timestamp,order_name text,customer_id text);")
	// create orders_items
	_, err = db.Query("create table if not exists order_items(id integer not null primary key,order_id integer,price_per_unit double precision,quantity integer,product text);")
	// create deliveries
	_, err = db.Query("create table if not exists deliveries(id integer not null primary key,order_item_id integer,delivered_quantity integer);")
	// create customers
	_, err = db.Query("create table if not exists customers(user_id text not null primary key,login text,password text,name text,company_id integer,credit_cards text);")
	// create customers_companies
	_, err = db.Query("create table if not exists customer_companies(company_id integer not null primary key,company_name text);")

	if err != nil {
		log.Print("Failed to execute query: ", err)
	}
	fmt.Println("create table successful")
}

func loadCSV(filename string) (records [][]string) {
	csvfile, err := os.Open(filename)
	if err != nil {
		log.Fatalln("Couldn't open the csv file", err)
	}

	// Parse the file
	r := csv.NewReader(csvfile)

	// Iterate through the csv
	for {
		// Read each record from csv
		record, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatal(err)
		}
		for i, _ := range record {
			if len(record[i]) == 0 {
				record[i] = "0.0"
			}
		}
		records = append(records, record)
	}
	records = records[1:]
	return
}

func insertIntoOrderItem() {
	var records = loadCSV(orderitems)

	db, err := sql.Open("postgres", DB_DSN)
	if err != nil {
		log.Fatalf("open: %v", err)
	}
	if err = db.Ping(); err != nil {
		log.Fatalf("open ping: %v", err)
	}

	defer db.Close()

	txn, err := db.Begin()
	if err != nil {
		log.Fatalf("begin: %v", err)
	}

	stmt, err := txn.Prepare(pq.CopyIn("order_items", "id", "order_id", "price_per_unit", "quantity", "product"))
	if err != nil {
		log.Fatalf("prepare: %v", err)
	}

	for _, r := range records {
		_, err = stmt.Exec(r[0], r[1], r[2], r[3], r[4])
		if err != nil {
			log.Fatalf("exec: %v", err)
		}
	}

	_, err = stmt.Exec()
	if err != nil {
		log.Fatalf("exec: %v", err)
	}

	err = stmt.Close()
	if err != nil {
		log.Fatalf("stmt close: %v", err)
	}

	err = txn.Commit()
	if err != nil {
		log.Fatalf("commit: %v", err)
	}
}

func insertIntoOrders() {
	var records = loadCSV(orders)
	db, err := sql.Open("postgres", DB_DSN)
	if err != nil {
		log.Fatalf("open: %v", err)
	}
	if err = db.Ping(); err != nil {
		log.Fatalf("open ping: %v", err)
	}

	defer db.Close()

	txn, err := db.Begin()
	if err != nil {
		log.Fatalf("begin: %v", err)
	}

	stmt, err := txn.Prepare(pq.CopyIn("orders", "id", "created_at", "order_name", "customer_id"))
	if err != nil {
		log.Fatalf("prepare: %v", err)
	}

	for _, r := range records {
		_, err = stmt.Exec(r[0], r[1], r[2], r[3])
		if err != nil {
			log.Fatalf("exec: %v", err)
		}
	}

	_, err = stmt.Exec()
	if err != nil {
		log.Fatalf("exec: %v", err)
	}

	err = stmt.Close()
	if err != nil {
		log.Fatalf("stmt close: %v", err)
	}

	err = txn.Commit()
	if err != nil {
		log.Fatalf("commit: %v", err)
	}
}

func insertIntoDeliveries() {
	var records = loadCSV(deliveries)
	db, err := sql.Open("postgres", DB_DSN)
	if err != nil {
		log.Fatalf("open: %v", err)
	}
	if err = db.Ping(); err != nil {
		log.Fatalf("open ping: %v", err)
	}

	defer db.Close()

	txn, err := db.Begin()
	if err != nil {
		log.Fatalf("begin: %v", err)
	}

	stmt, err := txn.Prepare(pq.CopyIn("deliveries", "id", "order_item_id", "delivered_quantity"))
	if err != nil {
		log.Fatalf("prepare: %v", err)
	}

	for _, r := range records {
		_, err = stmt.Exec(r[0], r[1], r[2])
		if err != nil {
			log.Fatalf("exec: %v", err)
		}
	}

	_, err = stmt.Exec()
	if err != nil {
		log.Fatalf("exec: %v", err)
	}

	err = stmt.Close()
	if err != nil {
		log.Fatalf("stmt close: %v", err)
	}

	err = txn.Commit()
	if err != nil {
		log.Fatalf("commit: %v", err)
	}
}

func insertIntoCustomers() {
	var records = loadCSV(customers)
	db, err := sql.Open("postgres", DB_DSN)
	if err != nil {
		log.Fatalf("open: %v", err)
	}
	if err = db.Ping(); err != nil {
		log.Fatalf("open ping: %v", err)
	}

	defer db.Close()

	txn, err := db.Begin()
	if err != nil {
		log.Fatalf("begin: %v", err)
	}

	stmt, err := txn.Prepare(pq.CopyIn("customers", "user_id", "login", "password", "name", "company_id", "credit_cards"))
	if err != nil {
		log.Fatalf("prepare: %v", err)
	}

	for _, r := range records {
		_, err = stmt.Exec(r[0], r[1], r[2], r[3], r[4], r[5])
		if err != nil {
			log.Fatalf("exec: %v", err)
		}
	}

	_, err = stmt.Exec()
	if err != nil {
		log.Fatalf("exec: %v", err)
	}

	err = stmt.Close()
	if err != nil {
		log.Fatalf("stmt close: %v", err)
	}

	err = txn.Commit()
	if err != nil {
		log.Fatalf("commit: %v", err)
	}
}

func insertIntoCustomerCompanies() {
	var records = loadCSV(customercompanies)
	db, err := sql.Open("postgres", DB_DSN)
	if err != nil {
		log.Fatalf("open: %v", err)
	}
	if err = db.Ping(); err != nil {
		log.Fatalf("open ping: %v", err)
	}

	defer db.Close()

	txn, err := db.Begin()
	if err != nil {
		log.Fatalf("begin: %v", err)
	}

	stmt, err := txn.Prepare(pq.CopyIn("customer_companies", "company_id", "company_name"))
	if err != nil {
		log.Fatalf("prepare: %v", err)
	}

	for _, r := range records {
		_, err = stmt.Exec(r[0], r[1])
		if err != nil {
			log.Fatalf("exec: %v", err)
		}
	}

	_, err = stmt.Exec()
	if err != nil {
		log.Fatalf("exec: %v", err)
	}

	err = stmt.Close()
	if err != nil {
		log.Fatalf("stmt close: %v", err)
	}

	err = txn.Commit()
	if err != nil {
		log.Fatalf("commit: %v", err)
	}
}
