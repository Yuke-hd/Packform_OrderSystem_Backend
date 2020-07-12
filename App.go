package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/gorilla/mux"
	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
	"log"
	"net/http"
	"time"
)

const (
	serverAdd = ":8888" // change here if port 8888 is taken dont forget to change request urk in frontend app
)

func handleRequests() {
	myRouter := mux.NewRouter().StrictSlash(true)
	myRouter.HandleFunc("/orders/{orderName}", lookupOrder)

	log.Fatal(http.ListenAndServe(serverAdd, myRouter))
}

func main() {
	fmt.Println("Order System v1.0 - Rest API")
	handleRequests()
}

const (
	DB_DSN = "postgres://postgres:123056@localhost:5000/postgres?sslmode=disable"
)

type Order struct {
	OrderName      string    `json:"order_name"`
	CompanyName    string    `json:"company_name"`
	Name           string    `json:"name"`
	Date           time.Time `json:"date"`
	OrderTotal     float64   `json:"order_total"`
	DeliveredTotal float64   `json:"delivered_total"`
}

type RawOrder struct {
	OrderName         string          `db:"order_name"`
	CompanyName       string          `db:"company_name"`
	Name              string          `db:"name"`
	Date              time.Time       `db:"created_at"`
	Price             sql.NullFloat64 `db:"price_per_unit"`
	Quantity          int32           `db:"quantity"`
	DeliveredQuantity sql.NullInt32   `db:"delivered_quantity"`
}

func lookupOrder(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	orderName := "'%" + vars["orderName"] + "%'"
	fmt.Println(orderName)

	//Allow CORS requests
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
	// Create an empty user and make the sql query (using $1 for the parameter)
	//var myString string
	// Create DB pool
	db, err := sqlx.Open("postgres", DB_DSN)
	if err != nil {
		log.Print("Failed to open a DB connection: ", err)
	}
	defer db.Close()

	//var orderName = "'PO #005-P'"
	rows, err := db.Queryx(
		"select orders.order_name,company_name,c.name,created_at,price_per_unit,quantity,delivered_quantity " +
			"from orders " +
			"left outer join order_items oi on orders.id = oi.order_id " +
			"left join deliveries d on oi.id = d.order_item_id " +
			"left join customers c on orders.customer_id = c.user_id " +
			"left join customer_companies cc on c.company_id = cc.company_id " +
			"where order_name like " + orderName +
			" order by order_name")
	if err != nil {
		log.Print("1Failed to execute query: ", err)
	}

	var results []Order
	var lastOrderName string
	var entry Order

	for rows.Next() {
		var tempOrder RawOrder

		err = rows.StructScan(&tempOrder)
		if err != nil {
			log.Fatal("2Failed to execute query: ", err)
		}
		if tempOrder.OrderName != lastOrderName {
			fmt.Println("result ", entry, "\n")
			results = append(results, entry)
		}
		fmt.Println(tempOrder)

		var deliveredQuantity = 0
		var ppu = 0.0
		if tempOrder.Price.Valid {
			ppu = tempOrder.Price.Float64
		}
		if tempOrder.DeliveredQuantity.Valid {
			deliveredQuantity = int(tempOrder.DeliveredQuantity.Int32)
		}

		if tempOrder.OrderName == lastOrderName {
			entry.OrderTotal += ppu * float64(tempOrder.Quantity)
			entry.DeliveredTotal += ppu * float64(deliveredQuantity)
		} else {
			lastOrderName = tempOrder.OrderName
			entry.OrderName = tempOrder.OrderName
			entry.CompanyName = tempOrder.CompanyName
			entry.Name = tempOrder.Name
			entry.Date = tempOrder.Date
			entry.OrderTotal = float64(tempOrder.Quantity) * ppu
			entry.DeliveredTotal = float64(deliveredQuantity) * ppu
		}
	}
	fmt.Println("result ", entry, "\n")
	results = append(results, entry)

	json.NewEncoder(w).Encode(results)
}
