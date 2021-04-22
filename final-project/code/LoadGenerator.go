package main

import (
	"database/sql"
	"fmt"
	"github.com/gocql/gocql"
	_ "github.com/lib/pq"
	"log"
	"sync"
	"time"
)

const POSTGRES_1_IP_ADDRESS = "35.208.251.185"
const POSTGRES_2_IP_ADDRESS = "35.209.208.246"
const CASSANDRA_1_IP_ADDRESS = "35.209.210.66"
const CASSANDRA_2_IP_ADDRESS = "35.208.184.187"

const POSTGRES_QUERY_1 = `SELECT email
					FROM customer, invoice
					WHERE customer.id = invoice.customer_id AND invoice.id = 206648`

const CASSANDRA_QUERY_1 = `SELECT customer_email 
					FROM bigdata.customer_invoice 
					WHERE invoice_id =206648 ALLOW FILTERING`

const POSTGRES_QUERY_2 = `SELECT invoice_date
					FROM customer, invoice
					WHERE customer.id = 14607 AND invoice.customer_id = 14607
					ORDER BY invoice.invoice_date  DESC LIMIT 1`

const CASSANDRA_QUERY_2 = `SELECT invoice_date 
					FROM bigdata.customer_invoice 
					WHERE customer_id = 14607 
					ORDER BY invoice_date DESC LIMIT 1`

const POSTGRES_QUERY_3 = `SELECT customer.id AS customer_id, invoice.id AS invocie_id, invoice.invoice_date, product.code AS product_code, product.description, invoice.quantity, product.unit_price 
					FROM customer, invoice, product
					WHERE customer.id = 14607 AND invoice.customer_id = 14607 AND product.code = invoice.product_code AND invoice.invoice_date >= '2011-11-08 00:00:00' AND invoice.invoice_date <= '2012-11-08 00:00:00'
					ORDER BY invoice.invoice_date DESC`

const CASSANDRA_QUERY_3 = `SELECT customer_id, invoice_id, invoice_date, product_code, product_description, product_quantity, product_unit_price 
					FROM bigdata.customer_invoice
					WHERE customer_id = 14607 AND invoice_date >= '2011-11-08 00:00:00' AND invoice_date <= '2012-11-08 00:00:00'
					ORDER BY invoice_date DESC`

const POSTGRES_QUERY_4 = `SELECT customer.id AS customer_id, invoice.id AS invocie_id, invoice.invoice_date, product.code AS product_code, product.description, invoice.quantity, product.unit_price 
					FROM customer, invoice, product
					WHERE customer.id = 14607 AND invoice.customer_id = 14607 AND product.code = invoice.product_code
					ORDER BY invoice.invoice_date DESC`

const CASSANDRA_QUERY_4 = `SELECT customer_id, invoice_id, invoice_date, product_code, product_description, product_quantity, product_unit_price 
					FROM bigdata.customer_invoice
					WHERE customer_id = 14607 ORDER BY invoice_date DESC`

const POSTGRES_QUERY_5 = `SELECT email, SUM(quantity * unit_price) AS total_expenditure
    				FROM customer
   	 				INNER JOIN invoice
   	 				ON customer.id=invoice.customer_id
					INNER JOIN product
					ON product.code=invoice.product_code
   			 		GROUP BY customer_id, email
   			 		ORDER BY total_expenditure
   				 	DESC LIMIT 100`

const CASSANDRA_QUERY_5 = `SELECT customer_id, sum(invoice_total) AS customer_expenditure
					FROM bigdata.customer_invoice
					GROUP BY customer_id`

const POSTGRES_QUERY_6 = `SELECT customer.id,
    				SUM(quantity*unit_price)/COUNT(customer.id) AS average_order_expenditure
   	 				FROM customer
        			INNER JOIN invoice
       		 		ON customer.id=invoice.customer_id
					INNER JOIN product
           		 	ON product.code=invoice.product_code
            		GROUP BY customer.id
            		ORDER BY average_order_expenditure DESC`

const CASSANDRA_QUERY_6 = `SELECT customer_id, sum(invoice_total) AS customer_total_expenditure, 
					count(customer_id) AS number_of_orders
					FROM bigdata.customer_invoice
					GROUP BY customer_id`

const POSTGRES_QUERY_7 = `SELECT invoice.invoice_date::date AS date, SUM(quantity * unit_price) AS total_revenue
					FROM product
   	 				INNER JOIN invoice
   	 				ON product.code=invoice.product_code
   		 			GROUP BY date 
					ORDER BY date DESC`

const CASSANDRA_QUERY_7 = `SELECT invoice_date, daily_revenue
    				FROM bigdata.daily_revenue`

const POSTGRES_QUERY_8 = `SELECT code, description, SUM(quantity) AS total_quantity_sold
					FROM product
    				INNER JOIN invoice
    				ON product.code=invoice.product_code
        			GROUP BY product.code, product.description
        			ORDER BY total_quantity_sold DESC`

const CASSANDRA_QUERY_8 = `SELECT product_code, product_description, product_total_quantity_sold
    				FROM bigdata.product_sale_counts`

const POSTGRES_QUERY_9 = `SELECT code, description, SUM(quantity * unit_price) AS total_revenue
    				FROM product
   	 				INNER JOIN invoice
   	 				ON product.code=invoice.product_code
   		 			GROUP BY product.code, product.description
   		 			ORDER BY total_revenue DESC`

const CASSANDRA_QUERY_9 = `SELECT product_code, product_description, product_total_revenue
    				FROM bigdata.product_revenue`

const POSTGRES_QUERY_10 = `SSELECT invoice.id AS invoice_id
					FROM customer, invoice
					WHERE customer.id = invoice.customer_id AND customer.postcode = '51524'`

const CASSANDRA_QUERY_10 = `SELECT invoice_id
					FROM bigdata.customer_invoice
					WHERE customer_postcode = '21187' ALLOW FILTERING`

// Global counters to make sure that the program doesn't continue before all async tasks are finished
var mainApplication sync.WaitGroup
var postgresQueries sync.WaitGroup
var cassandraQueries sync.WaitGroup

func main() {
	println("Query1")
	executeQueryTest(POSTGRES_QUERY_1, CASSANDRA_QUERY_1)
	println("Query2")
	executeQueryTest(POSTGRES_QUERY_2, CASSANDRA_QUERY_2)
	println("Query3")
	executeQueryTest(POSTGRES_QUERY_3, CASSANDRA_QUERY_3)
	println("Query4")
	executeQueryTest(POSTGRES_QUERY_4, CASSANDRA_QUERY_4)
	println("Query5")
	executeQueryTest(POSTGRES_QUERY_5, CASSANDRA_QUERY_5)
	println("Query6")
	executeQueryTest(POSTGRES_QUERY_6, CASSANDRA_QUERY_6)
	println("Query7")
	executeQueryTest(POSTGRES_QUERY_7, CASSANDRA_QUERY_7)
	println("Query8")
	executeQueryTest(POSTGRES_QUERY_8, CASSANDRA_QUERY_8)
	println("Query9")
	executeQueryTest(POSTGRES_QUERY_9, CASSANDRA_QUERY_9)
	println("Query10")
	executeQueryTest(POSTGRES_QUERY_10, CASSANDRA_QUERY_10)
}

func executeQueryTest(postgresQuery string, cassandraQuery string) {
	println("LOAD 1: " + time.Now().String())

	load := 1

	// global counters
	mainApplication.Add(4)
	postgresQueries.Add(load * 2)
	cassandraQueries.Add(load * 2)

	go loadTestPostgres(POSTGRES_2_IP_ADDRESS, postgresQuery, load, "2")
	go loadTestCassandra(CASSANDRA_2_IP_ADDRESS, cassandraQuery, load, "2")

	go loadTestPostgres(POSTGRES_1_IP_ADDRESS, postgresQuery, load, "1")
	go loadTestCassandra(CASSANDRA_1_IP_ADDRESS, cassandraQuery, load, "1")
	mainApplication.Wait()

	time.Sleep(1 * time.Minute)

	println("LOAD 100: " + time.Now().String())

	load = 100

	// global counters
	mainApplication.Add(4)
	postgresQueries.Add(load * 2)
	cassandraQueries.Add(load * 2)

	go loadTestPostgres(POSTGRES_2_IP_ADDRESS, postgresQuery, load, "2")
	go loadTestCassandra(CASSANDRA_2_IP_ADDRESS, cassandraQuery, load, "2")

	go loadTestPostgres(POSTGRES_1_IP_ADDRESS, postgresQuery, load, "1")
	go loadTestCassandra(CASSANDRA_1_IP_ADDRESS, cassandraQuery, load, "1")
	mainApplication.Wait()

	time.Sleep(1 * time.Minute)

	println("LOAD 1000: " + time.Now().String())

	load = 1000

	// global counters
	mainApplication.Add(4)
	postgresQueries.Add(load * 2)
	cassandraQueries.Add(load * 2)

	go loadTestPostgres(POSTGRES_2_IP_ADDRESS, postgresQuery, load, "2")
	go loadTestCassandra(CASSANDRA_2_IP_ADDRESS, cassandraQuery, load, "2")

	go loadTestPostgres(POSTGRES_1_IP_ADDRESS, postgresQuery, load, "1")
	go loadTestCassandra(CASSANDRA_1_IP_ADDRESS, cassandraQuery, load, "1")
	mainApplication.Wait()

	time.Sleep(1 * time.Minute)

	println("LOAD 10000: " + time.Now().String())

	load = 10000

	// global counters
	mainApplication.Add(4)
	postgresQueries.Add(load * 2)
	cassandraQueries.Add(load * 2)

	go loadTestPostgres(POSTGRES_2_IP_ADDRESS, postgresQuery, load, "2")
	go loadTestCassandra(CASSANDRA_2_IP_ADDRESS, cassandraQuery, load, "2")

	go loadTestPostgres(POSTGRES_1_IP_ADDRESS, postgresQuery, load, "1")
	go loadTestCassandra(CASSANDRA_1_IP_ADDRESS, cassandraQuery, load, "1")
	mainApplication.Wait()

	time.Sleep(1 * time.Minute)
}

func loadTestPostgres(ipAddress string, query string, load int, db_number string) {
	// Create DB pool
	connStr := fmt.Sprintf("user=postgres password=moomins dbname=postgres host=%s", ipAddress)
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		log.Fatal("Failed to open a DB connection: ", err)
	}
	defer db.Close()
	db.SetConnMaxLifetime(time.Hour)
	timeElapsedForQuery := make([]int64, load)

	for i := 0; i < load; i++ {
		go queryPostgresData(db, query, timeElapsedForQuery[:], i)
	}

	postgresQueries.Wait()
	delay := sum(timeElapsedForQuery[:]) / float64(load)
	fmt.Printf("Postgres %s latency: %f milli-seconds\n", db_number, delay)
	mainApplication.Done()
}

func loadTestCassandra(ipAddress string, query string, load int, db_number string) {
	cluster := gocql.NewCluster(ipAddress)
	cluster.ConnectTimeout = time.Hour * 1
	cluster.DisableInitialHostLookup = true
	cluster.Keyspace = "bigdata"
	session, err := cluster.CreateSession()

	if err != nil {
		log.Panic(err)
	}

	timeElapsedForQuery := make([]int64, load)

	for i := 0; i < load; i++ {
		go queryCassandraData(session, query, timeElapsedForQuery[:], i)
	}

	cassandraQueries.Wait()
	delay := sum(timeElapsedForQuery[:]) / float64(load)
	fmt.Printf("Cassandra %s latency: %f milli-seconds\n", db_number, delay)
	mainApplication.Done()
}

func queryPostgresData(db *sql.DB, query string, timeElapsedForQuery []int64, iteration int) {
	start := time.Now()
	db.Query(query)
	end := time.Now()
	elapsed := end.Sub(start)
	timeElapsedForQuery[iteration] = elapsed.Milliseconds()
	defer postgresQueries.Done() // decreases global counter by 1
}

func queryCassandraData(session *gocql.Session, query string, timeElapsedForQuery []int64, iteration int) {
	start := time.Now()
	session.Query(query).Iter().Scanner()
	end := time.Now()
	elapsed := end.Sub(start)
	timeElapsedForQuery[iteration] = elapsed.Milliseconds()
	defer cassandraQueries.Done() // decreases global counter by 1
}

func sum(array []int64) float64 {
	result := int64(0)
	for _, v := range array {
		result += v
	}
	return float64(result)
}
