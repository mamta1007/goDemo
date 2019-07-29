package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/jinzhu/gorm"
	_ "github.com/jinzhu/gorm/dialects/mysql"
)

var appRouterPath = "/ap1/v1/customers"

var gormDBType = "mysql"
var mysqlDBConnection = "root:admin@/customers?charset=utf8&parseTime=True&loc=Local"

var mongoDBURI = "mongodb://localhost:27017"
var mongoDBDatabase = "test"
var mongoDBGridFSName = "myFiles"

var couchbaseHost = "host"
var couchbaseHostName = "localhost"
var port = "port"
var couchbasePort = 8091
var bucket = "bucket"
var couchbaseBucketName = "customerDemo"
var password = "pass"
var passwordVal = "customer"

var db *gorm.DB

func init() {
	//open a db connection
	var err error
	db, err = gorm.Open(gormDBType, mysqlDBConnection)
	if err != nil {
		panic("failed to connect MySQL database")
	}

	//Migrate the schema
	db.AutoMigrate(&customerModel{})
	couchbaseInit()
	initMongoDB()
}

func main() {
	router := gin.Default()
	v1 := router.Group(appRouterPath)
	{
		v1.POST("/", createCustomer)
		v1.GET("/", fetchAllCustomers)
		v1.GET("/:id", fetchSingleCustomer)
		v1.PUT("/:id", updateCustomer)
		v1.DELETE("/:id", deleteCustomer)

	}
	router.Run()
}

type (
	// customerModel describes a customerModel type
	customerModel struct {
		gorm.Model
		FirstName string `json:"firstName"`
		LastName  string `json:"lastName"`
		Email     string `json:"email"`
	}
)

// createCustomer add a new customer
func createCustomer(c *gin.Context) {
	t0 := time.Now().UnixNano()
	firstName := c.Query("firstName")
	lastName := c.Query("lastName")
	email := c.Query("email")

	//create in mongo
	r := c.Request
	r.ParseForm()
	// FormFile returns the first file for the given key `myFile`
	// it also returns the FileHeader so we can get the Filename,
	// the Header and the size of the file

	file, handler, err := r.FormFile("addressProof")

	if err != nil {
		fmt.Println("Error Retrieving the File")
		fmt.Println(err)
		c.JSON(http.StatusNotFound, gin.H{"status": http.StatusNotFound, "message": "Error Retrieving the File! "})
	} else {
		defer file.Close()
		fmt.Printf("Uploaded File: %+v\n", handler.Filename)
		fmt.Printf("File Size: %+v\n", handler.Size)
		fmt.Printf("MIME Header: %+v\n", handler.Header)

		// read all of the contents of our uploaded file into a
		// byte array
		fileBytes, err := ioutil.ReadAll(file)
		if err != nil {
			fmt.Println(err)
		}

		customer := customerModel{
			FirstName: firstName,
			LastName:  lastName,
			Email:     email}

		db.Save(&customer)
		saveFileToMongo(http.DetectContentType(fileBytes), handler.Filename, fmt.Sprint(customer.ID), fileBytes)

		timeTaken := fmt.Sprint(timeSpent(t0)) + " ms"
		c.JSON(http.StatusCreated, gin.H{"status": http.StatusCreated, "message": "Customer item created successfully! ",
			"resourceId": customer.ID, "time": timeTaken})
	}
}

// fetchAllCustomers fetch all customers
func fetchAllCustomers(c *gin.Context) {
	t0 := time.Now().UnixNano()
	var customers []customerModel

	db.Find(&customers)

	if len(customers) <= 0 {
		c.JSON(http.StatusNotFound, gin.H{"status": http.StatusNotFound, "message": "No customers found!"})
		return
	}

	c.JSON(http.StatusOK, gin.H{"status": http.StatusOK, "data": customers, "time": fmt.Sprint(timeSpent(t0)) + " ms"})

}

// fetchSingleCustomer fetch a customer
func fetchSingleCustomer(c *gin.Context) {
	t0 := time.Now().UnixNano()
	var timeTaken string
	var timeTakenForDoc string
	var datasource string
	var customer customerModel
	custID := c.Param("id")
	// check cache first
	cachedRes := getFromCache("customer_" + custID)
	if cachedRes != nil {
		datasource = "cache"

		err := json.Unmarshal(cachedRes, &customer)
		if err != nil {
			fmt.Println("error:", err)
			c.JSON(http.StatusInternalServerError, gin.H{"status": http.StatusInternalServerError, "message": err})
			return
		}
		timeTaken = fmt.Sprint(timeSpent(t0)) + " ms"
	} else {
		datasource = "DB"
		db.First(&customer, custID)

		//save in cache
		//resData, _ := json.Marshal(&customer)
		//enc := json.NewEncoder(os.Stdout)
		//enc.Encode(customer)
		postHandler("customer_"+custID, "1990", customer)

		if customer.ID == 0 {
			c.JSON(http.StatusNotFound, gin.H{"status": http.StatusNotFound, "message": "No customer found!"})
			return
		}
		timeTaken = fmt.Sprint(timeSpent(t0)) + " ms"
	}

	//get customer file from mongo
	t0 = time.Now().UnixNano()
	var customerFileName string
	customerFile := retrieveFileFromMongo(custID)
	if customerFile != nil {
		customerFileName = customerFile.Name()
	} else {
		customerFileName = "No file found !"
	}
	/*w := c.Writer

	fileHeader := make([]byte, 512)
	customerFile.Read(fileHeader)

	w.Header().Set("Content-Disposition", "attachment; filename="+customerFile.Name())
	w.Header().Set("Content-Type", customerFile.ContentType())
	w.Header().Set("Content-Length", strconv.FormatInt(customerFile.Size(), 10))
	w.Write(fileHeader)
	w.Flush()*/
	fmt.Println(customer)
	timeTakenForDoc = fmt.Sprint(timeSpent(t0)) + " ms"
	c.JSON(http.StatusOK, gin.H{"status": http.StatusOK, "data": customer, "documentName": customerFileName,
		"dataSource": datasource, "recordTime": timeTaken, "docTime": timeTakenForDoc})
}

// updateCustomer update a customer
func updateCustomer(c *gin.Context) {
	t0 := time.Now().UnixNano()
	var customer customerModel
	custID := c.Param("id")

	db.First(&customer, custID)

	if customer.ID == 0 {
		c.JSON(http.StatusNotFound, gin.H{"status": http.StatusNotFound, "message": "No customer found!"})
		return
	}

	if c.Query("firstName") != "" {
		db.Model(&customer).Update("firstName", c.Query("firstName"))
	}
	if c.Query("lastName") != "" {
		db.Model(&customer).Update("lastName", c.Query("lastName"))
	}
	if c.Query("email") != "" {
		db.Model(&customer).Update("email", c.Query("email"))
	}

	//create in mongo
	r := c.Request
	r.ParseForm()
	file, handler, err := r.FormFile("addressProof")
	if err != nil {
		fmt.Println("Error Retrieving the File")
		fmt.Println(err)
	} else {

		fmt.Printf("Uploaded File: %+v\n", handler.Filename)
		fmt.Printf("File Size: %+v\n", handler.Size)
		fmt.Printf("MIME Header: %+v\n", handler.Header)

		// read all of the contents of our uploaded file into a
		// byte array
		fileBytes, err := ioutil.ReadAll(file)
		if err != nil {
			fmt.Println(err)
		}

		deleteFileFromMongo(fmt.Sprint(customer.ID))
		saveFileToMongo(http.DetectContentType(fileBytes), handler.Filename, fmt.Sprint(customer.ID), fileBytes)
		defer file.Close()
	}

	deleteHandler("customer_" + custID)
	c.JSON(http.StatusOK, gin.H{"status": http.StatusOK, "message": "Customer updated successfully!",
		"time": fmt.Sprint(timeSpent(t0)) + " ms"})
}

// deleteCustomer delete a  customer
func deleteCustomer(c *gin.Context) {
	t0 := time.Now().UnixNano()
	var customer customerModel
	custID := c.Param("id")

	db.First(&customer, custID)

	if customer.ID == 0 {
		c.JSON(http.StatusNotFound, gin.H{"status": http.StatusNotFound, "message": "No customer found!"})
		return
	}

	db.Delete(&customer)
	deleteFileFromMongo(custID)
	deleteHandler("customer_" + custID)
	c.JSON(http.StatusOK, gin.H{"status": http.StatusOK, "message": "Customer deleted successfully!",
		"time": fmt.Sprint(timeSpent(t0)) + " ms"})
}
