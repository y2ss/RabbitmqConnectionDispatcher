<template>
  <div class="trace">
    <h1>日志平台</h1>
    <p class="tips">*只有当天的日志才会实时滚动更新，其他日期的日志需要下载才能查看</p>
    <p class="tips">*error记录标准错误日志</p>
    <div>
      <el-select v-model="lvl" placeholder="level" style="margin-right:10px">
        <el-option v-for="item in level" :key="item.value" :label="item.label" :value="item.value"></el-option>
      </el-select>
      <el-date-picker v-model="date" type="date" format="yyyy-MM-dd" value-format="yyyyMMdd" placeholder="选择日期"></el-date-picker>
      <el-button @click="search"  style="margin-left:10px">查询</el-button>
      <el-button @click="clear"  style="margin-left:10px">清空日志</el-button>
      <el-button @click="downloadFile"  style="margin-left:10px">下载该日文件</el-button>
    </div>
    <div class="record">
      <div class="board" v-infinite-scroll="scrollPlanAçtion"> {{ details }} </div>
    </div>
  </div>
</template>

<script>
const BASEURL = "http://localhost:9089"
export default {
  name: 'Trace',
  props: {
    msg: String
  },
  data() {
    return {
      key: "",
      details: "",
      lvl: 1,
      date: '',
      level: [
        {
          value: 1,
          label: 'error',
        },
        {
          value: 2,
          label: 'debug'
        },
        {
          value: 3,
          label: 'info'
        }
      ],
    }
  },
  created() {
    //页面刚进入时开启长连接
    this.key = Date.parse(new Date()) + this.randomString(16)
    this.initWebSocket(this.key);
    console.log("key:", this.key)
  },
  methods: {
    downloadFile() {
      if (this.date.length == 0) {
          this.$message({ message: '参数不对', type: 'warning' });
          return
      }
      window.open(BASEURL + '/trace/file?&date='+this.date+"&level="+this.lvl)
    },
    clear() {
      this.details = ""
    },
    scrollPlanAçtion() {
    },
    randomString(len) {
      len = len || 32;
      var $chars = 'ABCDEFGHJKMNPQRSTWXYZabcdefhijkmnprstwxyz2345678';
      var maxPos = $chars.length;
      var pwd = '';
      for (var i = 0; i < len; i++) {
        pwd += $chars.charAt(Math.floor(Math.random() * maxPos));
      }
      return pwd;
    },
    search() {
      this.clear()
      this.subscribeLog()
    },
    initWebSocket(key) {
      const wsuri = "ws://localhost:9089/trace/ws?key="+key;
      this.websock = new WebSocket(wsuri);
      this.websock.onopen = this.websocketonopen;
      this.websock.onerror = this.websocketonerror;
      this.websock.onmessage = this.websocketonmessage;
      this.websock.onclose = this.websocketclose;
    },
    websocketonopen() {
      console.log("WebSocket连接成功");
      this.$message({
          message: '连接服务器成功',
          type: 'success'
      });
    },
    websocketonerror(e) {
      console.log(e)
      console.log("WebSocket连接发生错误");
      this.$message.error('连接服务器失败，请刷新页面');
    },
    websocketonmessage(e) {
      console.log(e)
      this.details += e.data
    },
    websocketsend(agentData) {
      this.websock.send(agentData);
    },
    websocketclose(e) {
      console.log("connection closed (" + e + ")");
      this.$message({
          message: '与服务器连接中断，请刷新页面',
          type: 'warning'
        });
    },
    subscribeLog() {
      this.$http.get(BASEURL + '/trace/watch?key='+this.key+"&date="+this.date+"&level="+this.lvl)
      .then(response => {
          console.log(response)
          if (response.data.code == -1) {
            this.$message.error('参数错误');
          }
      }, error => {
          console.log(error)
          this.$message.error('连接服务器失败');
      })
    }
  }
}
</script>

<style scoped>

.record {
  /* background-color: #42b983; */
  width: 100%;
  height: 80%;
  margin-top: 20px;
  display: flex;
  position: absolute;
}

.board {
  white-space: pre-line;
  text-align: left;
  width: 100%;
  height: 100%;
  font-size: 15px;
}

.tips {
  font-size: 12px;
}

h3 {
  margin: 20px 0 0;
}
ul {
  list-style-type: none;
  padding: 0;
}
li {
  display: inline-block;
  margin: 0 10px;
}
a {
  color: #42b983;
}
</style>
