package polodb

/*
#cgo CFLAGS: -I../../lib
#cgo LDFLAGS: -L../../lib -lpolodb
#include "../../lib/polodb.h"
*/
import "C"
import (
	"errors"
	"fmt"
	"time"
)

func CreateDb() (*C.Database, error) {
	database := C.PLDB_open(C.CString("/tmp/polodb.db"))
	if database == nil {
		return nil, errors.New("Error while creating database\n")
	}
	return database, nil
}

func (db *C.Database) CreateCollection(colName string) error {
	colId := C.uint(0)
	metaVersion := C.uint(1)
	errCode := C.PLDB_create_collection(db, C.CString(colName), &colId, &metaVersion)
	if errCode != C.int(1) {
		return errors.New("Error while creating collection\n")
	}
	return nil
}

func (db *C.Database) Find(arguments map[string]interface{}) (map[string]interface{}, error) {
	doc, err := createDocument(arguments)
	res := make(map[string]interface{})
	if err != nil {
		return nil, errors.New("Error while creating document\n")
	}
	var handle *C.DbHandle
	errCode := C.PLDB_find(db, 0, 1, doc, &handle)
	if errCode != C.int(0) {
		return nil, errors.New("Error searching into database\n")
	}

	errCode = C.PLDB_step(handle)
	if errCode != C.int(0) {
		return nil, errors.New("Error searching into database\n")
	}

	for C.PLDB_handle_state(handle) == 2 {
		var val *C.DbValue
		C.PLDB_handle_get(handle, &val)
		res, err = documentToObj(val, res)
		if err != nil {
			return nil, errors.New("Error searching into database\n")
		}
		C.PLDB_free_value(val)
		errCode = C.PLDB_step(handle)
		if errCode != C.int(0) {
			return nil, errors.New("Error searching into database\n")
		}
	}
	defer C.PLDB_free_handle(handle)
	defer C.PLDB_free_doc(doc)
	return res, nil
}

func (db *C.Database) Insert(values map[string]interface{}) error {
	doc, err := createDocument(values)
	if err != nil {
		return errors.New("Error while creating document\n")
	}
	errCode := C.PLDB_insert(db, 0, 1, doc)
	defer C.PLDB_free_doc(doc)
	if errCode != C.int(0) {
		return errors.New("Error inserting into database\n")
	}
	return nil
}

func (db *C.Database) Close() {
	C.PLDB_close(db)
}

func createDocument(values map[string]interface{}) (*C.DbDocument, error) {
	doc := C.PLDB_mk_doc()
	if values == nil {
		return nil, errors.New("Empty map given\n")
	}

	for key, value := range values {
		err := doc.setProperty(key, value)
		if err != nil {
			C.PLDB_free_doc(doc)
			return nil, errors.New("Error while inserting key: " + key + "\n")
		}
	}
	return doc, nil
}

func (doc *C.DbDocument) setProperty(key string, value interface{}) error {
	var errCode C.int
	switch value.(type) {
	case string:
		errCode = C.PLDB_doc_set_string(doc, C.CString(key), C.CString(value.(string)))
	case time.Time:
		errCode = C.PLDB_doc_set_UTCDateTime(doc, C.CString(key), C.longlong(value.(time.Time).Unix()))
	case int:
		errCode = C.PLDB_doc_set_int(doc, C.CString(key), C.longlong(value.(int)))
	default:
		errCode = -1
	}

	switch errCode {
	case -1:
		return errors.New("Unsupported type\n")
	case 0:
		return nil
	default:
		return errors.New("Error while setting document property\n")
	}
}

func documentToObj(val *C.DbValue, keyStr map[string]interface{}) (map[string]interface{}, error) {
	var doc *C.DbDocument
	resCode := C.PLDB_value_get_document(val, &doc)
	if resCode < 0 {
		return map[string]interface{}{}, errors.New("DbValue get document error\n")
	}
	var keyBuf = C.CString("")
	var tempVal *C.DbValue
	iterObj := C.PLDB_doc_iter(doc)
	fmt.Println(iterObj)
	for C.PLDB_doc_iter_next(iterObj, keyBuf, 512, &tempVal) > C.int(0) {
		valString, _ := stringToObj(tempVal)
		keyStr[C.GoString(keyBuf)] = valString
		C.PLDB_free_value(tempVal)
	}
	C.PLDB_free_doc_iter(iterObj)
	C.PLDB_free_doc(doc)
	return keyStr, nil
}

func stringToObj(val *C.DbValue) (string, error) {
	var resString *C.char
	resCode := C.PLDB_value_get_string_utf8(val, &resString)
	if resCode < 0 {
		return "", errors.New("DbValue get string error")
	}
	return C.GoString(resString), nil
}

func intToObj(val *C.DbValue) (int, error) {
	var res int
	resCode := C.PLDB_value_get_int(val, &res)
	if resCode < 0 {
		return -1, errors.New("DbValue get string error")
	}
	return res, nil
}
