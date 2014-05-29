// natasha-mysqlmon project main.go
package mysql_engine

import (
	"database/sql"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"github.com/marpaia/graphite-golang"
	"regexp"
	"strconv"
	"strings"
	"time"
)

type MonMon struct {
	Interval int
	Checkers map[string]chan bool
}

type Database struct {
	Name       string
	Protocol   string
	Address    string
	User       string
	Password   string
	KeyPrefix  string
	KeySuffix  string
	KeyMatcher *regexp.Regexp
}

type DatabaseChecker struct {
	DB   *Database
	Quit chan bool
}

type MonResult struct {
	DB        *Database
	KeyValues map[string]int
}

func ResultsToGraphite(c <-chan *MonResult, host string, port int) {
	//Must be able to connect to graphite first or conn will be nill
	//Con is not exported so we can't check it later
	var Graphite *graphite.Graphite
	for {
		Graphite = &graphite.Graphite{Host: host, Port: port}
		err := Graphite.Connect()
		if err != nil {
			fmt.Println("Unable to connect to graphite")
			time.Sleep(2 * time.Second)
			continue
		}
		break
	}
	for {
		res := <-c
		for k, v := range res.KeyValues {
			fmt.Println(k, v)
			graphiteKey := fmt.Sprintf("%s.%s", res.DB.KeyPrefix, strings.Replace(res.DB.Name, ".", "_", -1))
			if res.DB.KeySuffix != "" {
				graphiteKey = fmt.Sprintf("%s.%s", graphiteKey, res.DB.KeySuffix)
			}
			graphiteKey = fmt.Sprintf("%s.%s", graphiteKey, strings.ToLower(k))
			fmt.Println("Sending metric: ", graphiteKey, " ", v)
			err := Graphite.SimpleSend(graphiteKey, strconv.Itoa(v))
			if err != nil {
				fmt.Println("Sending failed, attempting to reconnect")
				err := Graphite.Connect()
				if err != nil {
					fmt.Println("Reconnnect failed")
				}
				continue
			}
		}
	}
}

func MonDatabase(database *Database, c chan *MonResult, quit chan bool) {
	dsn := fmt.Sprintf("%s:%s@%s(%s)/", database.User, database.Password, database.Protocol, database.Address)
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		panic(err.Error())
	}
	ticker := time.NewTicker(5 * time.Second)
	for {
		select {
		case <-quit:
			fmt.Println("Quitting.")
			return
		default:
			<-ticker.C
			fmt.Println("Tick!")
			res, err := db.Query("show status;")
			if err != nil {
				panic(err.Error())
			}

			result := &MonResult{
				KeyValues: make(map[string]int),
				DB:        database,
			}
			for res.Next() {
				var variable_name string
				var value int
				err = res.Scan(&variable_name, &value)
				if database.TrackedKey(variable_name) {
					result.KeyValues[variable_name] = value
				}
			}
			select {
			case c <- result:
			default:
				fmt.Println("Result channel full!")
			}
		}
	}
}

func (db *Database) AddMatchers(matchers []string) {
	macroMatcher := strings.Join(matchers, ")|(")
	cappedMacroMatcher := fmt.Sprintf("(%s)", macroMatcher)
	db.KeyMatcher = regexp.MustCompile(cappedMacroMatcher)
}

func (db *Database) TrackedKey(key string) bool {
	return db.KeyMatcher.MatchString(key)
}

func (mon *MonMon) AddDatabase(db *Database, c chan *MonResult) {
	var quit = make(chan bool)
	go MonDatabase(db, c, quit)
	mon.Checkers[db.Name] = quit
}

func (mon *MonMon) RemoveDatabase(db *Database) {
	mon.Checkers[db.Name] <- true
	delete(mon.Checkers, db.Name)
}
