package main

import (
	"fmt"
	"log"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"

)

var dbMongoSession *mgo.Session
var dbMongo *mgo.Database


func initMongoDB() {
	var err error

	/*
	 * Connect to the server and get a database handle
	 */
	if dbMongoSession, err = mgo.Dial(mongoDBURI); err != nil {
		panic(err)
	}

	dbMongo = dbMongoSession.DB(mongoDBDatabase)
	//defer dbMongoSession.Close()
}

// Store file in MongoDB GridFS
func saveFileToMongo(contentType string, fileName string, fileID string, fileBytes []byte) {

	mdbfile, err := dbMongo.GridFS(mongoDBGridFSName).Create(fileName)
	if err == nil {
		mdbfile.SetContentType(contentType)
		//mdbfile.SetId(objId)
		log.Println("Copying to: %s", fileName)
		mdbfile.Write(fileBytes)
		mdbfile.SetId(fileID)
		mdbfile.SetMeta(bson.M{
			"content-type": contentType})

		log.Println("Done copying, closing")
		err = mdbfile.Close()
		if err != nil {
			log.Println("Unable to close copy to mongo")
		}
		log.Println("MongoDB body file saved")

	}
}

func retrieveFileFromMongo(fileID string) *mgo.GridFile {
	var gridFile *mgo.GridFile
	var err error
	if gridFile, err = dbMongo.GridFS(mongoDBGridFSName).OpenId(fileID); err != nil {
		fmt.Printf("Error getting file from GridFS: %s\n", err.Error())
		// return ctx.String(http.StatusInternalServerError, "Error getting file from database")
	}

	//defer gridFile.Close()

	return gridFile

}

func deleteFileFromMongo(fileID string) error {
	err := dbMongo.GridFS(mongoDBGridFSName).Files.Remove(bson.M{"_id": fileID})
	if err != nil {
		return err
	}
	_, err = dbMongo.GridFS(mongoDBGridFSName).Chunks.RemoveAll(bson.D{{"files_id", fileID}})
	return err
}

func updateFileToMongo(contentType string, fileName string, fileID string, fileBytes []byte) {
	err := deleteFileFromMongo(fileID)
	if err != nil {
		saveFileToMongo(contentType, fileName, fileID, fileBytes)
	}
}