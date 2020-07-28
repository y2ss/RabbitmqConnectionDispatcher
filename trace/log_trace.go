package trace

import (
	"fmt"
	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
	"github.com/hpcloud/tail"
	"RabbitmqConnectionDispatcher/common/log"
	"net/http"
	"strconv"
	"sync"
	"time"
)

type JSON map[string]interface{}
var trace *Trace

func init() {
	trace = NewTrace()
	go trace.ping()
}

type Trace struct {
	wsConns map[string]*websocket.Conn
	mu      sync.RWMutex
	unique  map[string]int
}

func NewTrace() *Trace {
	return &Trace{
		wsConns: make(map[string]*websocket.Conn),
		unique: make(map[string]int),
	}
}

func (t *Trace) recordKeyLevel(key string, level int) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.unique[key] = level
}

func (t *Trace) searchLevelByKey(key string) int {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.unique[key]
}

func (t *Trace) createWSConn(key string, w *websocket.Conn) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.wsConns[key] = w
}

func (t *Trace) ping() {
	tick := time.NewTicker(30 * time.Second)
	defer tick.Stop()
	for true {
		select {
		case <- tick.C:
			t.mu.Lock()
			for key, ws := range t.wsConns {
				if ws == nil {
					continue
				}
				err := ws.WriteMessage(websocket.PingMessage, []byte("ping"))
				if err != nil {
					log.Logger.Error("websocket " + key + " ping pong failed, remove this connection.")
					ws.Close()
					delete(t.wsConns, key)
				}
			}
			t.mu.Unlock()
		}
	}
}

func traceWatchHandler(c *gin.Context) {
	level := c.Query("level")
	dateStr := c.Query("date")
	lvl, err := strconv.Atoi(level)
	date, err2 := strconv.Atoi(dateStr)
	key := c.Query("key")
	if err != nil || lvl < 0 || lvl > 5 || err2 != nil || dateStr == "" || len(dateStr) != 8 || key == "" {
		c.JSON(http.StatusOK, JSON{ "code" : -1 })
		return
	}
	//记录对应key和当前订阅lvl
	trace.recordKeyLevel(key, lvl)
	go traceLogWith(key, lvl, date)
}

var upgrader = websocket.Upgrader {
	CheckOrigin: func(r *http.Request) bool {
		return true
	},
}

func createWebsocketConnHandler(c *gin.Context) {
	ws, err := upgrader.Upgrade(c.Writer, c.Request, nil)
	key := c.Query("key")
	if err != nil {
		log.Logger.Error("try to update websocket failed error: %v", err)
		c.JSON(http.StatusBadRequest, JSON{"code":-2, "error": err})
		return
	} else {
		log.Logger.Info("update websocket connecting")
	}
	trace.createWSConn(key, ws)
	c.JSON(http.StatusOK, JSON{"code":0})
}

func (t *Trace) sendMessage(key string, text string) error {
	t.mu.Lock()
	ws := t.wsConns[key]
	if ws == nil {
		t.mu.Unlock()
		return fmt.Errorf("websocket lost")
	}
	err := ws.WriteMessage(websocket.TextMessage, []byte(text))
	t.mu.Unlock()

	if err != nil {
		t.mu.Lock()
		log.Logger.Error("websocket " + key + " ping pong failed, remove this connection, err:", err.Error())
		ws.Close()
		delete(t.wsConns, key)
		t.mu.Unlock()
		return err
	}
	return nil
}

func traceLogWith(key string, level int, date int) {
	fileName := fileName(level, date)
	tails, err := tail.TailFile("./" + fileName, tail.Config{
		ReOpen: true, // true表示读取文件被删时阻塞等待新建该文件，false则文件被删掉时程序结束
		Follow: true, // true表示一直阻塞并监听指定文件，false表示一次读完就结束程序
		Location: &tail.SeekInfo{ Offset:0, Whence:2 },
		MustExist: false, // true则没有找到文件就报错并结束，false则没有找到文件就阻塞保持住
		Poll: true, // 使用Linux的Poll函数，poll的作用是把当前的文件指针挂到等待队列
	})
	if err != nil {
		log.Logger.Error("tail file err:", err)
	}

	var msg *tail.Line
	var ok bool
	var out bool = false
	for true {
		msg, ok = <- tails.Lines
		if !ok {
			log.Logger.Info("tail file close reopen, filenam:%s", tails, fileName)
			time.Sleep(100 * time.Millisecond)
			continue
		}
		//检查对应level被改变
		nowLevel := trace.searchLevelByKey(key)
		if nowLevel != level {
			out = true
		}

		err := trace.sendMessage(key, msg.Text + "\n")
		if err != nil {
			out = true
		}
		if out {
			break
		}
	}
	log.Logger.Debug("websocket " + key + " stop trace!")
	tails.Stop()
	tails.Cleanup()
}

func fileName(level int, date int) string {
	var l string
	switch level {
	case 1://error
		l = "error"
		break
	case 2://debug
		l = "debug"
		break
	case 3://info
		l = "info"
		break
	default:
		l = "info"
	}
	return fmt.Sprintf("%s_%d_server.log", l, date)
}