package main

import "gopolodb/pkg/polodb"

func main() {
	db, _ := polodb.CreateDb()
	db.CreateCollection("col")
	db.Insert(map[string]interface{}{"name": "joseph", "age": 13})
	db.Close()
}
