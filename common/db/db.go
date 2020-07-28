package db

import (
	"database/sql"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"RabbitmqConnectionDispatcher/common/log"
	"RabbitmqConnectionDispatcher/config/bootstrap"
	"os"
	"strconv"
	"time"
)

var sharedDB *sql.DB
var dbLog *DBLog

type DBLog struct {
	FILEPATH string
}

func NewDBLog() *DBLog {
	return &DBLog{FILEPATH: "./db_stats.log"}
}

func (log *DBLog) info(str string) {
	file, _ := os.OpenFile(log.FILEPATH, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	defer file.Close()
	file.WriteString(str)
	file.WriteString("\n")
}

func init() {
	var (
		connectionString string
		err              error
	)
	dbConfig := bootstrap.App.AppConfig.Map("mysql.test")
	connectionString = fmt.Sprintf("%s:%s@tcp(%s:%s)/%s", dbConfig["username"], dbConfig["password"], dbConfig["host"], dbConfig["port"], dbConfig["database"])
	db, err := sql.Open("mysql", connectionString)
	log.Logger.Info(connectionString)
	if err != nil {
		log.Logger.Panic(err)
	}
	sharedDB = db
	openConns, _ := strconv.Atoi(dbConfig["open_conns"])
	idleConns, _ := strconv.Atoi(dbConfig["idle_conns"])
	maxLifeTime, _ := strconv.Atoi(dbConfig["max_life_time"])
	sharedDB.SetMaxOpenConns(openConns)
	sharedDB.SetMaxIdleConns(idleConns)
	sharedDB.SetConnMaxLifetime(time.Duration(maxLifeTime) * time.Second)

	err = sharedDB.Ping()

	stats := sharedDB.Stats()
	dbLog = NewDBLog()
	scheduleDebug(func() {
		stats = sharedDB.Stats()
		dbLog.info(fmt.Sprintf("%s\nMaxOpenConnections: %d\nIdle: %d\nOpenConnections: %d\n"+
			"InUse: %d\nWaitCount: %d\nWaitDuration: %d\nMaxIdleClosed: %d\nMaxLifetimeClosed: %d",
			time.Now().String(), stats.MaxOpenConnections, stats.Idle, stats.OpenConnections, stats.InUse, stats.WaitCount,
			stats.WaitDuration, stats.MaxIdleClosed, stats.MaxLifetimeClosed))
	})
	if err != nil {
		log.Logger.Panic(err)
	} else {
		log.Logger.Info("initialize db")
	}
}

func scheduleDebug(f func()) {
	go func() {
		for {
			f()
			now := time.Now()
			next := now.Add(time.Minute * 60)
			next = time.Date(next.Year(), next.Month(), next.Day(), next.Hour(), next.Minute(), next.Second(), 0, next.Location())
			t := time.NewTimer(next.Sub(now))
			<-t.C
		}
	}()
}

func GetDBConnection() (db *sql.DB) {
	return sharedDB
}
