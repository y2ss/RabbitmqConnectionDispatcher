package worker

import (
	"RabbitmqConnectionDispatcher/common"
	"RabbitmqConnectionDispatcher/common/cache/redis"
	"RabbitmqConnectionDispatcher/common/log"
	"RabbitmqConnectionDispatcher/common/queue"
	"sync"
	"time"
)

type WorkType int
const (
	WorkTypeONE          = 1 << 0
	WorkTypeTWO          = 1 << 1
	WorkTypeTHREE        = 1 << 2
)

type dispatchChunk struct {
	id      string
	ct      WorkType
	extra   interface{}
}

var center WorkerCenter
var dispatcher chan(*dispatchChunk)
func init() {
	center = WorkerCenter{}
	dispatcher = make(chan *dispatchChunk, 1000)
	listenerDispatch()
}

type Worker struct {
	Id            string
	TaskQueue     *queue.SafeQueue
}

func NewWorker(device string) *Worker {
	return &Worker{
		Id: device,
		TaskQueue: queue.NewSafeQueue(1000),
	}
}

type WorkerCenter struct {
	Workers     sync.Map
}

func DispatchSerial(device string, t WorkType, extra interface{}) {
	dispatcher <- &dispatchChunk{
		id: device,
		ct: t,
		extra: extra,
	}
}

func listenerDispatch() {
	go func() {
		for d := range dispatcher {
			id := d.id
			extra := d.extra
			t := d.ct
			if v, ok := center.Workers.Load(id); ok == true {
				//已经存在worker
				w := v.(*Worker)
				w.continueWorker(id, t, extra)
			} else {
				//不存在 创建新Worker
				if !isDiscardWorkType(t) {
					if extra == nil {
						//虽然收到indextable任务 但是extra为nil表示没有需要同步任何的it
					} else {
						center.registerWorker(id, extra, t)
					}
				}
			}
		}
	}()
}

type MissionType int
const (
	MissionTypeSyncONE        = 1 << 0
	MissionTypeSyncTWO        = 1 << 1
	MissionTypeSyncTHREE      = 1 << 2
)

func isDiscardWorkType(t WorkType) bool {
	return t == WorkTypeONE || t == WorkTypeTWO
}

func isDependentOnWorkType(t WorkType) bool {
	return t == WorkTypeTHREE
}

type Mission struct {
	t               MissionType
	isFinished      bool
	isBegan         bool
	mu              sync.RWMutex
	extra           map[string]interface{}
	beginTimestamp  int64
	updateTimeStamp int64
	isRetry         bool
}

func (wc *Worker) continueWorker(id string, ct WorkType, extra interface{}) {
	switch ct {
	case WorkTypeONE:
		//收到om消息 继续同步ecg rri it
		if wc.TaskQueue.Length() < 20 {
			wc.createSyncItMissionQueue()
		} else {
			log.Logger.Info("worker " + id + " has too much task left, so it wouldn't enqueue!!");
		}
		break
	case WorkTypeTWO:
		log.Logger.Info("worker " + id + " receive ecg index table")
		wc.workerReceiveIndexTable(WorkTypeTWO, extra)
		break
	case WorkTypeTHREE:
		wc.workerReceiveEcgData(WorkTypeTHREE, extra)
		break
	}
}

func (wc *Worker) currentMission() (*Mission) {
	f := wc.TaskQueue.Front()
	if f == nil {
		log.Logger.Info("worker " + wc.Id + " mission should not be nill!!!")
		return nil
	}
	v := f.Value()
	if v == nil {
		log.Logger.Info("worker " + wc.Id + " mission should not be nill!!!")
		return nil
	}
	return v.(*Mission)
}

func (wc *Worker) updateMissionTimestamp(extra interface{}) {
	if extra == nil { return }
	if m := wc.currentMission(); m != nil {
		seq := extra.(uint32)
		if seq % 4 == 0 {
			m.updateTimeStamp = time.Now().Unix()
		}
	}
}

func (wc *Worker) workerReceiveRriData(ct WorkType, extra interface{}) {
	if m := wc.currentMission(); m != nil {
		// ...
	} else {
		//重试
		log.Logger.Info("worker " + wc.Id + " has miss job")
	}
}

func (wc *Worker) workerReceiveIndexTable(ct WorkType, extra interface{}) {
	if m := wc.currentMission(); m != nil {
		//将需要同步的ecg/rri indextable 放入任务队列中
		if extra == nil {
			//没有需要同步的数据
		} else {
			//有需要同步的数据
			var t MissionType = MissionTypeSyncECGData
			if ct == WorkTypeReceiveRRIIT {
				t = MissionTypeSyncRRIData
			}
			wc.createSyncDataMission(t, extra)
		}
		m.finish(ct, wc.Id) //置标志位为true 结束om任务
	}
}

func (wc *WorkerCenter) removeWorker(device string) {
	wc.Workers.Delete(device)
}

func (wc *WorkerCenter) registerWorker(id string, extra interface{}, ct WorkType) {
	nw := NewWorker(id)
	wc.Workers.Store(id, nw)
	//将待处理任务放入队列中
	//注册worker 一般是收到om数据后再处理
	switch ct {
	case WorkTypeONE:
		//注册新的同步任务
		nw.createMissionQueue()
		break
	case WorkTypeTWO:
		//注册获取ecg data任务
		if extra != nil {
			nw.createSyncDataMission(MissionTypeSyncONE, extra)
		}
		break
	case WorkTypeTHREE:
		//注册获取rri data任务
		if extra != nil {
			nw.createSyncDataMission(MissionTypeSyncTWO, extra)
		}
		break
	}
	nw.startWork()
}

func (w *Worker) createSyncDataMission(t MissionType, extra interface{}) {
	if extra == nil {
		log.Logger.Error("extra is nil, this should not happen")
		return
	}
	extraMap := extra.(map[string]interface{})
	blocks := extraMap["B"].([]*protobuf.HisBlock)

	if t == MissionTypeSyncONE {
		indextables := extraMap["I"].([]*ecg.ECGIndexTable)
		for idx, it := range indextables {
			n := make(map[string]interface{})
			n["I"] = it
			n["B"] = blocks[idx]
			w.TaskQueue.Enqueue(&Mission{
				t: t,
				isFinished: false,
				isBegan: false,
				isRetry: false,
				extra: n,
			})
		}
	} else {
		indextables := extraMap["I"].([]*rri.RRIIndexTable)
		for idx, it := range indextables {
			n := make(map[string]interface{})
			n["I"] = it
			n["B"] = blocks[idx]
			w.TaskQueue.Enqueue(&Mission{
				t: t,
				isFinished: false,
				isBegan: false,
				isRetry: false,
				extra: n,
			})
		}
	}
}

func (w *Worker) createMissionQueue() {
	w.TaskQueue.Enqueue(&Mission{
		t: MissionTypeSyncONE,
		isFinished: false,
		isBegan: false,
		isRetry: false,
	})
	w.TaskQueue.Enqueue(&Mission{
		t: MissionTypeSyncTWO,
		isFinished: false,
		isBegan: false,
		isRetry: false,
	})
	w.TaskQueue.Enqueue(&Mission{
		t: MissionTypeSyncTHREE,
		isFinished: false,
		isBegan: false,
		isRetry: false,
	})
}

func (w *Worker) startWork() {
	//在子线程处理同步逻辑
	go func() {
		log.Logger.Info("worker " + w.Id + " start work on goroutine ", common.GetGoroutineID())
		t := time.NewTicker(1 * time.Second)
		defer t.Stop()
		out := false
		count := 0
		continuousFailCount := 0
		for {
			count++
			select {
			case <- t.C:
				//每秒检查任务是否完成
				//完成后开启下一个任务
				if w.TaskQueue.IsEmpty() {
					//队列为空
					out = true
					break
				}
				mission := w.currentMission()
				if !mission.isBegin() {
					mission.doMission(w.Id)
				}
				if mission.isFinish() {
					//将任务移除队列
					continuousFailCount = 0
					w.TaskQueue.Dequeue()
					log.Logger.Info("worker "  + w.Id + " finish mission type:", missionTypeDesc(mission.t))
					log.Logger.Info("worker " + w.Id + " has ", w.TaskQueue.Length(), " tasks left")
				} else {
					//没有完成继续等待 超过2分钟则放弃该任务 连续失败3个任务则退出worker
					if count % 5 == 0 {
						if mission.isSyncDataEvent() {
							if w.judgeFailOnRequestIndexTableEvent(mission) {
								continuousFailCount++
							}
						} else {
							if w.judgeFailOnOtherEvent(mission) {
								continuousFailCount++
							}
						}
					}
				}
			}
			if out {
				log.Logger.Info("worker " + w.Id + " finish work, exit looping")
				break
			}
			if continuousFailCount > 3 {
				log.Logger.Info("worker " + w.Id + " failed too much mission, exit looping")
				break
			}
		}
		//删除worker
		center.removeWorker(w.Id)
	}()
}

//true: failed false:no failed
func (w *Worker) judgeFailOnRequestIndexTableEvent(mission *Mission) bool {
	now := time.Now().Unix()
	if now - mission.beginTimestamp > 180 ||
		now - mission.updateTimeStamp > 25 {
		if !mission.isRetry {
			log.Logger.Info("worker " + w.Id + " failed on mission " + missionTypeDesc(mission.t) + ", and retry again.")
			//重试任务一次
			mission.isRetry = true
			mission.isBegan = false
			return false
		}
		//已经重试过了
		log.Logger.Info("worker " + w.Id + " failed on mission " + missionTypeDesc(mission.t) + ", it wouldn't retry anymore, discard mission.")
		w.TaskQueue.Dequeue()
		return true
	}
	return false
}

//true: failed false:no failed
func (w *Worker) judgeFailOnOtherEvent(mission *Mission) bool {
	now := time.Now().Unix()
	if now - mission.beginTimestamp > 180 ||
		now - mission.updateTimeStamp > 15 {
		log.Logger.Info("worker " + w.Id + " failed on mission " + missionTypeDesc(mission.t))
		w.TaskQueue.Dequeue()
		return true
	}
	return false
}

func (m *Mission) isSyncDataEvent() bool {
	return m.t == MissionTypeSyncONE
}

func (m *Mission) finish(t WorkType, id string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	switch t {
	case WorkTypeONE:
		if m.t == MissionTypeSyncONE { //判断当前任务是否正在执行相应的任务类型 不相同则表示正在进行其他任务
			m.isFinished = true
			return
		}
	case WorkTypeTHREE:
		if m.t == MissionTypeSyncTWO {
			m.isFinished = true
			return
		}
	}
	log.Logger.Info("device " + id + " want to finish job but job type doesn't match, workType: " + workTypeDesc(t) + " missionType: " + missionTypeDesc(m.t))
}

func (m *Mission) isBegin() bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.isBegan
}

func (m *Mission) isFinish() bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.isFinished
}

func (m *Mission) doMission(id string) {
	log.Logger.Info("device:" + id + " do mission type:" + missionTypeDesc(m.t))
	m.mu.Lock()
	m.isBegan = true
	t := time.Now().Unix()
	m.beginTimestamp = t
	m.updateTimeStamp = t
	m.mu.Unlock()
	switch m.t {
	case MissionTypeSyncONE:
		break
	case MissionTypeSyncTWO:
		break
	case MissionTypeSyncTHREE:
		break
	default:
		log.Logger.Info("unprocessed mission type:" + missionTypeDesc(m.t))
		break
	}
}


func missionTypeDesc(t MissionType) string {
	switch t {
	case MissionTypeSyncONE:
		return "MissionTypeSyncONE"
	case MissionTypeSyncTWO:
		return "MissionTypeSyncTWO"
	case MissionTypeSyncTHREE:
		return "MissionTypeSyncTHREE"
	}
	return "unknown type"
}

func workTypeDesc(t WorkType) string {
	switch t {
	case WorkTypeONE:
		return "WorkTypeONE"
	case WorkTypeTWO:
		return "WorkTypeTWO"
	case WorkTypeTHREE:
		return "WorkTypeTHREE"
	}
	return "unknown type"
}