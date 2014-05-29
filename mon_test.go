package mysql_engine

import "testing"
import "fmt"
import "time"

var graphiteHost = "192.168.1.2"
var graphitePort = 2010

func TestKeyMatcher(t *testing.T) {
	db := &Database{}
	db.AddMatchers([]string{"Inno.*", "Delayed_writes"})
	fmt.Println(db.TrackedKey("Inno_db"))
	fmt.Println(db.TrackedKey("Delayed_writes"))
}

func TestMonAddRemove(t *testing.T) {
	mon := &MonMon{Checkers: map[string]chan bool{}}
	//Create result channel with buffer in case data sink is disconnected
	var resultC = make(chan *MonResult, 1000)
	go ResultsToGraphite(resultC, graphiteHost, graphitePort)

	database := &Database{
		KeyPrefix: "product.local.stack-rds.20",
		Name:      "localhost.co.nz",
	}
	database.AddMatchers([]string{"Innodb_pages_written"})
	mon.AddDatabase(database, resultC)
	time.Sleep(15 * time.Second)
	mon.RemoveDatabase(database)
	time.Sleep(5 * time.Second)
}
