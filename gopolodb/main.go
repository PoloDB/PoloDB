package main

import (
	"fmt"
	"gopolodb/pkg/polodb"
)

func main() {
	db, _ := polodb.CreateDb()
	db.CreateCollection("col")
	//db.Insert(map[string]interface{}{"name": "joseph", "age": "13"})
	res, _ := db.Find(map[string]interface{}{"name": "joseph"})
	fmt.Println(res["name"])
	db.Close()
}
