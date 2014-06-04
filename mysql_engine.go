// natasha-mysqlmon project main.go
package mysql_engine

import (
	"database/sql"
	"fmt"
	"github.com/go-sql-driver/mysql"
	"github.com/hailiang/socks"
	"github.com/marpaia/graphite-golang"
	"net"
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
	Time      int64
}

func init() {
	mysql.RegisterDial("socks", SocksDial)
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
	LOOP:
		for k, v := range res.KeyValues {
			fmt.Println(k, v)
			graphiteKey := fmt.Sprintf("%s.%s", res.DB.KeyPrefix, strings.Replace(res.DB.Name, ".", "_", -1))
			if res.DB.KeySuffix != "" {
				graphiteKey = fmt.Sprintf("%s.%s", graphiteKey, res.DB.KeySuffix)
			}
			graphiteKey = fmt.Sprintf("%s.%s", graphiteKey, strings.ToLower(k))
			metric := graphite.Metric{
				Name:      graphiteKey,
				Value:     strconv.Itoa(v),
				Timestamp: res.Time,
			}
			fmt.Println("Sending metric: ", metric)
			err := Graphite.SendMetric(metric)
			if err != nil {
				//Sending the metric has failed, likely due to a connection issue
				//Attempt to reconnect and then restart sending this result
				fmt.Println("Sending failed, attempting to reconnect")
				for {
					if err := Graphite.Connect(); err == nil {
						break
					}
					fmt.Println("Graphite reconnnect failed: ", err.Error())
					time.Sleep(5 * time.Second)
				}
				goto LOOP
			}
		}
	}
}

func MonDatabase(database *Database, c chan *MonResult, quit chan bool) {
	dsn := fmt.Sprintf("%s:%s@%s(%s)/?timeout=10s", database.User, database.Password, database.Protocol, database.Address)
	fmt.Println("Connectings to: ", dsn)
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		fmt.Println(".")
	}
	err = db.Ping()
	if err != nil {
		fmt.Println("Unable to connect to database: ", err.Error())
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
				fmt.Println("Unable query database")
				continue
			}

			result := &MonResult{
				KeyValues: make(map[string]int),
				DB:        database,
				Time:      time.Now().Unix(),
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

func SocksDial(addr string) (net.Conn, error) {
	fmt.Println("Connection to ", addr, " requested!")
	addrParts := strings.Split(addr, ":")
	dialSocksProxy := socks.DialSocksProxy(socks.SOCKS5, strings.Join(addrParts[:2], ":"))
	con, err := dialSocksProxy("tcp", strings.Join(addrParts[2:4], ":"))
	if err != nil {
		fmt.Println(err.Error())
	}
	return con, err
}
