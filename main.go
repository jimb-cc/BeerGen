package main

import (
	"fmt"
	"github.com/a-h/round"
	"github.com/brianvoe/gofakeit"
	"github.com/gosuri/uiprogress"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
	"math"
	"sync"
	"time"
	"math/rand"
	"crypto/tls"
	"net"
)


type Beer struct {
	BeerName    string
	BreweryName string
	City        string
	Style       string
	Hop         string
	Yeast       string
	Rating      int
	Alcohol     float32
	Price       float32
}

func main() {
	fmt.Println("\n#----------------------")
	fmt.Println("#-- Jim's mongGO Hammer")
	fmt.Println("#----------------------")
	fmt.Println("#-- Connecting to MongoDB")

	// Connect to Mongo
	//Session, err := mgo.Dial("mongodb://localhost")

	//Session, err := mgo.Dial("mongodb://jimtest3-1.jimdemo.8913.mongodbdns.com:27000")
	//Session, err := mgo.Dial("mongodb://Jim:jim123@jimtest5-1.jimdemo.8913.mongodbdns.com:27000/Jim?replicaSet=jimtest1")
//	Session, err := mgo.Dial("mongodb://Jim:jim123@jimdemo-11.jimdemo.8913.mongodbdns.com:27000/Jim")

	// mongodb://Jim:<PASSWORD>@jimdemocluster-shard-00-00-kwnwy.mongodb.net:27017,jimdemocluster-shard-00-01-kwnwy.mongodb.net:27017,jimdemocluster-shard-00-02-kwnwy.mongodb.net:27017/<DATABASE>?ssl=true&replicaSet=JimDemoCluster-shard-0&authSource=admin

	//Connect to Atlas

		tlsConfig := &tls.Config{}

		dialInfo := &mgo.DialInfo{
			Addrs: []string{"cluster0-shard-00-00-kwnwy.mongodb.net:27017",
					"cluster0-shard-00-01-kwnwy.mongodb.net:27017",
					"cluster0-shard-00-02-kwnwy.mongodb.net:27017"},
			Database: "admin",
			Username: "gogo",
			Password: "gogo123",
		}
		dialInfo.DialServer = func(addr *mgo.ServerAddr) (net.Conn, error) {
			conn, err := tls.Dial("tcp", addr.String(), tlsConfig)
			return conn, err
		}
	Session, err := mgo.DialWithInfo(dialInfo)


	//------ uncomment here

	if err != nil {
		panic(err)
	}

	defer Session.Close()
	//Session.SetMode(mgo.Strong, true)

	listDBs(Session)

	// define DB names and Collection
	var dbname = "CraftBeer"
	var collname = "beer"

	// Create a wait group to manage the goroutines.
	var waitGroup sync.WaitGroup

	// Perform 10 concurrent queries against the database.
	var concQueries = 10
	var numDocs = 1000000
	var batchSize = 1000




	fmt.Printf("#-- We're going to insert %v docs, in batches of %v\n", numDocs, batchSize)
	var timeNow = time.Now()
	fmt.Printf("#-- time now is %s\n", timeNow)

	waitGroup.Add(concQueries)
	for query := 0; query < concQueries; query++ {
		go insertConcDocs(query, &waitGroup, Session, dbname, collname, numDocs/concQueries, batchSize)
	}

	// Wait for all the queries to complete.
	waitGroup.Wait()
	fmt.Println("All Inserts Completed")
	fmt.Println("\n#----------------------")
	elapsedTime := time.Now().Sub(timeNow)
	fmt.Printf("#-- insert took %v\n", elapsedTime)
	fmt.Printf("#-- a rate of %v docs per second\n", (float32(numDocs) / float32(elapsedTime)))

	readDocs(Session, dbname, collname)
}

func insertConcDocs(query int, waitGroup *sync.WaitGroup, mongoSession *mgo.Session, dbname string, collname string, numDocs int, batchSize int) {

	// Decrement the wait group count so the program knows this
	// has been completed once the goroutine exits.
	defer waitGroup.Done()

	// Request a socket connection from the session to process our query.
	// Close the session when the goroutine exits and put the connection back
	// into the pool.
	sessionCopy := mongoSession.Copy()
	defer sessionCopy.Close()

	// we want to split the workload up into batches so that MongoDB can process it efficiently
	// we know how many docs to insert, and how big the batches are (1000 is the max for .insertMany)
	// So work out how many batches we have, and how many docs are left over
	numBatches, remainder := math.Modf(float64(numDocs) / float64(batchSize))
	rem := round.ToEven(remainder*float64(batchSize), 0)

	c := mongoSession.DB(dbname).C(collname)

	uiprogress.Start()                                                           // start rendering
	bar := uiprogress.AddBar(int(numBatches)).AppendCompleted().PrependElapsed() // Add a new bar

	// inset batches

	var docs []interface{}

	for batch_i := 1; batch_i <= int(numBatches); batch_i++ {
		for docs_i := 1; docs_i <= int(batchSize); docs_i++ {
			docs = append(docs, makeDoc())
		}

		bulk := c.Bulk()
		bulk.Unordered()
		bulk.Insert(docs...)
		_, bulkErr := bulk.Run()
		if bulkErr != nil {
			panic(bulkErr)
		}
		bar.Incr()
		docs = nil



	}

	// inset remainders

	if rem > 0 {
		for rem_i := 1; rem_i <= int(rem); rem_i++ {
			docs = append(docs, makeDoc())
		}

		bulk := c.Bulk()
		bulk.Unordered()
		bulk.Insert(docs...)
		_, bulkErr := bulk.Run()
		if bulkErr != nil {
			panic(bulkErr)
		}

		bar.Incr()
		docs = nil

	}

}


func makeDoc() Beer{
	newbeer := Beer{
		gofakeit.BeerName(),
		"The " + gofakeit.HipsterWord() + " " + gofakeit.HipsterWord() + " beer company",
		gofakeit.City(),
		gofakeit.BeerStyle(),
		gofakeit.BeerHop(),
		gofakeit.BeerYeast(),
		rand.Intn(10),
		rand.Float32(),
		rand.Float32(),
	}
	return newbeer
}

func listDBs(mongoSession *mgo.Session) {

	// Request a socket connection from the session to process our query.
	// Close the session when the goroutine exits and put the connection back
	// into the pool.
	sessionCopy := mongoSession.Copy()
	defer sessionCopy.Close()
	dbnames, err := sessionCopy.DatabaseNames()
	if err != nil {
		panic(err)
	}
	fmt.Printf("#-- %v\n", dbnames)

}



func countDocs(mongoSession *mgo.Session, dbname string, collname string) {

	// Request a socket connection from the session to process our query.
	// Close the session when the goroutine exits and put the connection back
	// into the pool.
	sessionCopy := mongoSession.Copy()
	defer sessionCopy.Close()

	c := sessionCopy.DB(dbname).C(collname)

	n,err := c.Find(nil).Count()
	if err != nil {
		panic(err)
	}
	fmt.Printf("there are %v docs in the collection", n)
}

func readDocs(mongoSession *mgo.Session, dbname string, collname string) {

	// Request a socket connection from the session to process our query.
	// Close the session when the goroutine exits and put the connection back
	// into the pool.
	sessionCopy := mongoSession.Copy()
	defer sessionCopy.Close()

	c := sessionCopy.DB(dbname).C(collname)

	m := c.Find(bson.M{"style":"Stout"})

	fmt.Println(m)
}






