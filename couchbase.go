package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"math"
	"net/http"
	"strconv"
	"time"

	"gopkg.in/couchbase/gocb.v1"
)

type datastorer interface {
	get(k string) []byte
	set(k string, v interface{}, ttl int) error
	delete(k string) error
	append(k string, v []byte) error
	validKey(k string) error
	validValue(v []byte) error
}

var (
	errNotFound      = errors.New("NOT_FOUND")
	errKeyExists     = errors.New("KEY_EXISTS_ERROR")
	errOversizedBody = errors.New("OVERSIZED_BODY")
	errEmptyBody     = errors.New("EMPTY_BODY")
	errInvalidKey    = errors.New("INVALID_KEY")
	errInvalidBody   = errors.New("INVALID_BODY")
)

const (
	maxTTLInSec   = 60 * 60 * 24 * 30
	maxSizeInByte = 20 * 1024 * 1024
	maxKeyLength  = 250
)

type couchbaseDatastore gocb.Bucket

var ds datastorer

var timeout = time.Millisecond * 100

func newDatastore() (ds *couchbaseDatastore, err error) {
	url, bucket, pass := parseFlag()

	if c, err := gocb.Connect(url); err == nil {
		if b, err := c.OpenBucket(bucket, pass); err == nil {
			return (*couchbaseDatastore)(b), nil
		}
	}
	return nil, err
}

func parseFlag() (string, string, string) {
	host := flag.String(couchbaseHost, couchbaseHostName, "host name (defaults to localhost)")
	port := flag.Int(port, couchbasePort, "port number (defaults to 8091)")
	bucket := flag.String(bucket, couchbaseBucketName, "bucket name (defaults to couchcache)")
	pass := flag.String(password, passwordVal, "password (defaults to password)")

	flag.Parse()

	url := fmt.Sprintf("http://%s:%d", *host, *port)
	log.Println(url)
	return url, *bucket, *pass
}

func (ds *couchbaseDatastore) get(k string) []byte {
	//var val []uint
	var data []byte

	var value interface{}
	if _, err := (*gocb.Bucket)(ds).Get(k, &value); err != nil {
		if err.Error() != "Key not found." {
			log.Println(err)
		}
		return data
	}
	j, _ := json.Marshal(value)
	//fmt.Println(string(j), err)
	return j
}

func (ds *couchbaseDatastore) set(k string, v interface{}, ttl int) error {

	_, err := (*gocb.Bucket)(ds).Upsert(k, v, uint32(ttl))
	return memdErrorToDatastoreError(err)

}

func (ds *couchbaseDatastore) delete(k string) error {
	if err := ds.validKey(k); err != nil {
		return errInvalidKey
	}

	_, err := (*gocb.Bucket)(ds).Remove(k, gocb.Cas(0))
	return memdErrorToDatastoreError(err)
}

func (ds *couchbaseDatastore) append(k string, v []byte) error {
	if err := ds.validKey(k); err != nil {
		return errInvalidKey
	}

	if err := ds.validValue(v); err != nil {
		return err
	}

	_, err := (*gocb.Bucket)(ds).Append(k, string(v))
	return memdErrorToDatastoreError(err)

}

func (ds *couchbaseDatastore) validKey(key string) error {
	if len(key) < 1 || len(key) > maxKeyLength {
		return errInvalidKey
	}
	return nil
}

func (ds *couchbaseDatastore) validValue(v []byte) error {
	if len(v) == 0 {
		log.Println("body is empty")
		return errEmptyBody
	}

	if len(v) > maxSizeInByte {
		log.Println("body is too large")
		return errOversizedBody
	}

	return nil
}

func memdErrorToDatastoreError(err error) error {
	if err == nil {
		return nil
	}

	log.Println(err.Error())
	switch err.Error() {
	case "Key not found.":
		return errNotFound
	case "The document could not be stored.":
		return errNotFound
	case "Document value was too large.":
		return errOversizedBody
	default:
		log.Println(err)
		return err
	}
}

func couchbaseInit() {
	if d, err := newDatastore(); err != nil {
		log.Fatalln(err)
	} else {
		ds = datastorer(d)
	}
}

func getFromCache(custID string) []byte {
	//couchbaseInit()
	//t0 := time.Now().UnixNano()
	ch := ds.get(custID)
	fmt.Println(string(ch))

	return ch
}

func postHandler(k string, ttl string, v interface{}) {
	t0 := time.Now().UnixNano()

	ttl1, _ := strconv.Atoi(ttl)
	ds.set(k, v, ttl1)
	log.Println("set ["+k+"] in", timeSpent(t0), "ms")
	return
}

func deleteHandler(key string) {
	t0 := time.Now().UnixNano()

	if err := ds.delete(key); err == nil {
		log.Println("delete ["+key+"] in", timeSpent(t0), "ms")
		//w.WriteHeader(http.StatusNoContent)
	} else {
		log.Println(err)
		//datastoreErrorToHTTPError(err, w)
	}
}

func returnTimeout(w http.ResponseWriter, k string) {
	log.Println(k + ": timeout")
	http.Error(w, k+": timeout", http.StatusRequestTimeout)
}

func timeSpent(t0 int64) int64 {
	return int64(math.Floor(float64(time.Now().UnixNano()-t0)/1000000 + .5))
}