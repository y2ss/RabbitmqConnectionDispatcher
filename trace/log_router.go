package trace

import (
	"github.com/gin-contrib/cors"
	"github.com/gin-gonic/gin"
	"RabbitmqConnectionDispatcher/common"
	"RabbitmqConnectionDispatcher/common/log"
	"net/http"
	"os"
	"strconv"
	"time"
)

func InitRouter() *gin.Engine {
	router := gin.Default()
	router.Use(cors.Default())
	router.GET("/trace/watch", traceWatchHandler)
	router.GET("/trace/ws", createWebsocketConnHandler)
	router.GET("/trace/file", downloadFile)
	router.GET("/trace/test", func(c *gin.Context) {
		c.JSON(http.StatusOK, JSON{ "message" : "success" })
	})
	return router
}

func downloadFile(c *gin.Context) {
	level := c.Query("level")
	dateStr := c.Query("date")
	lvl, err := strconv.Atoi(level)
	date, err2 := strconv.Atoi(dateStr)
	if err != nil || lvl < 0 || lvl > 5 || err2 != nil || dateStr == "" || len(dateStr) != 8 {
		c.JSON(http.StatusOK, JSON{ "code" : -1 })
		return
	}
	fileName := fileName(lvl, date)
	exists := common.PathExists("./" + fileName)
	if !exists {
		c.JSON(http.StatusOK, JSON{ "code" : -2 })
		return
	}
	fileHandler, err := os.Open("./" + fileName)
	if err != nil {
		log.Logger.Error("open file error")
		c.JSON(http.StatusOK, JSON{ "code" : -2 })
		return
	}
	dest := fileName + ".tar.gz"
	destName := "./" + dest
	err = common.Compress([]*os.File{fileHandler}, destName)
	if err != nil {
		log.Logger.Error("compress file error")
		c.JSON(http.StatusOK, JSON{ "code" : -2 })
		return
	}
	c.Header("Content-type", "application/octet-stream")
	c.Header("Content-Disposition", "attachment; filename="+dest)
	c.Header("Content-Transfer-Encoding", "binary")
	c.Header("Expires", "0")

	http.ServeFile(c.Writer, c.Request, destName)
	deleteFile(destName)
}

func deleteFile(dest string) {
	common.DoingAfterAsync(30 * time.Second, func() {
		os.Remove(dest)
	})
}
