package mongoconnect

import (
	"context"
	"fmt"
	"log"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// MongoSession for DB requests
type MongoSession struct {
	Client   *mongo.Client
	Database *mongo.Database
}

func (ms *MongoSession) WatchCollection(ctx context.Context, collection string, sendChan chan []byte) {
	matchStage := bson.D{{"$match", bson.D{{"operationType", "insert"}}}}
	col := ms.Database.Collection(collection, nil)
	changeStream, err := col.Watch(ctx, mongo.Pipeline{matchStage}, nil)
	if err != nil {
		log.Println("Could not create changeStream: ", err)
		return
	}

	// print out all change stream events in the order they're received
	// see the mongo.ChangeStream documentation for more examples of using change streams
	for changeStream.Next(ctx) {
		sendChan <- changeStream.Current
	}
}

// InsertOneWSWatch Inserts a single document to the given collection and to cig-news(WSWatch) as well
func (ms *MongoSession) InsertOneWSWatch(ctx context.Context, col string, payload interface{}) (err error) {
	id, err := ms.Database.Collection("cig-news").InsertOne(ctx, payload)
	if err != nil {
		log.Printf("YT could not insert one to cig-news: %s\n", err)
		return err
	}
	log.Printf("Succesfully inserted to cig-news collection. Document ID: %s\n", id)

	id, err = ms.Database.Collection(col).InsertOne(ctx, payload)
	if err != nil {
		log.Printf("YT could not insert one to %s: %s\n", col, err)
		return err
	}
	log.Printf("Succesfully inserted to %s collection. Document ID: %s\n", col, id)
	return err
}

// FindOne finds one document in a given collection
func (ms *MongoSession) FindOne(ctx context.Context, filter interface{}, col string) (err error) {
	cur, err := ms.Database.Collection(col).Find(ctx, filter)

	if err := cur.Decode(&filter); err != nil {
		log.Printf("error with cursor Decode: %s ", err)
		return err
	}
	log.Println(filter)
	return cur.Err()
}

// InsertOne Inserts one document
func (ms *MongoSession) InsertOne(ctx context.Context, payload string, collection string) error {
	var bdoc interface{}
	err := bson.UnmarshalExtJSON([]byte(payload), true, &bdoc)
	if err != nil {
		log.Println("error while bson Unmarshall")
		return err
	}
	log.Println(bdoc)
	insertResult, err := ms.Database.Collection(collection).InsertOne(ctx, bdoc)
	if err != nil {
		log.Println("error while insert")
		return err
	}
	log.Println("Inserted a single document: ", insertResult.InsertedID)
	return nil
}

// NewMongoSession connects to a DB and returns the client
func NewMongoSession(ctx context.Context, URI string, DB string) (*MongoSession, error) {
	context, ctxCancel := context.WithTimeout(ctx, 10*time.Second)
	defer ctxCancel()
	var dbOpts options.ClientOptions
	dbOpts.ApplyURI(URI)
	client, err := mongo.Connect(context, &dbOpts)

	if err != nil {
		log.Print("cannot make connection to DB", err)
		return nil, err
	}
	err = client.Ping(context, nil)

	if err != nil {
		log.Println("cannot connect to DB, timed out ping", err)
		return nil, err
	}

	fmt.Println("Connected to MongoDB!")
	return &MongoSession{Client: client, Database: client.Database(DB)}, nil
}
