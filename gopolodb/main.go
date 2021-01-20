package main

import (
	"fmt"
	"gopolodb/pkg/polodb"
)

func main() {
	db, _ := polodb.CreateDb()
	db.CreateCollection("col")
	//db.Insert(map[string]interface{}{"name": "joseph", "age": "13"})
	fmt.Println(db.Find(map[string]interface{}{"age": "13"}))
	db.Close()
}
