package utils

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/tsuna/gohbase/hrpc"
)

// WriteManager 写入管理器
type WriteManager struct {
	isRunning      bool
	stopChan       chan struct{}
	writeLogs      []map[string]interface{}
	mu             sync.Mutex
	writeStats     map[string]int // 记录每个电影ID的写入次数
	writeStatsTime time.Time      // 统计开始时间
}

// 全局写入管理器
var WriteManagerInstance = &WriteManager{
	isRunning:      false,
	stopChan:       make(chan struct{}),
	writeLogs:      make([]map[string]interface{}, 0, 100),
	writeStats:     make(map[string]int),
	writeStatsTime: time.Now(),
}

// StartRandomWrites 开始随机写入操作
func (wm *WriteManager) StartRandomWrites() {
	wm.mu.Lock()
	defer wm.mu.Unlock()

	if wm.isRunning {
		return
	}

	wm.isRunning = true
	wm.stopChan = make(chan struct{})
	wm.resetStats()

	// 开启协程进行异步写入
	go wm.writeRoutine()

	logrus.Info("随机写入服务已启动")
}

// StopRandomWrites 停止随机写入操作
func (wm *WriteManager) StopRandomWrites() {
	wm.mu.Lock()
	defer wm.mu.Unlock()

	if !wm.isRunning {
		return
	}

	close(wm.stopChan)
	wm.isRunning = false

	logrus.Info("随机写入服务已停止")
}

// IsRunning 检查写入服务是否正在运行
func (wm *WriteManager) IsRunning() bool {
	wm.mu.Lock()
	defer wm.mu.Unlock()
	return wm.isRunning
}

// GetLogs 获取写入日志
func (wm *WriteManager) GetLogs() []map[string]interface{} {
	wm.mu.Lock()
	defer wm.mu.Unlock()

	// 返回副本以避免数据竞争
	logs := make([]map[string]interface{}, len(wm.writeLogs))
	copy(logs, wm.writeLogs)
	return logs
}

// GetHotspots 获取热点电影ID（被写入最多的）
func (wm *WriteManager) GetHotspots(limit int) []map[string]interface{} {
	wm.mu.Lock()
	defer wm.mu.Unlock()

	// 创建一个切片存储电影ID和写入次数
	type movieWrite struct {
		MovieID string
		Count   int
	}

	stats := make([]movieWrite, 0, len(wm.writeStats))
	for movieID, count := range wm.writeStats {
		stats = append(stats, movieWrite{
			MovieID: movieID,
			Count:   count,
		})
	}

	// 按写入次数降序排序
	for i := 0; i < len(stats)-1; i++ {
		for j := 0; j < len(stats)-i-1; j++ {
			if stats[j].Count < stats[j+1].Count {
				stats[j], stats[j+1] = stats[j+1], stats[j]
			}
		}
	}

	// 限制返回数量
	if limit > len(stats) {
		limit = len(stats)
	}

	// 格式化返回结果
	result := make([]map[string]interface{}, limit)
	for i := 0; i < limit; i++ {
		if i < len(stats) {
			result[i] = map[string]interface{}{
				"movieId": stats[i].MovieID,
				"count":   stats[i].Count,
			}
		}
	}

	return result
}

// 重置统计数据
func (wm *WriteManager) resetStats() {
	wm.writeStats = make(map[string]int)
	wm.writeStatsTime = time.Now()
	wm.writeLogs = make([]map[string]interface{}, 0, 100)
}

// 写入协程
func (wm *WriteManager) writeRoutine() {
	// 创建工作池，使用5个协程处理写入操作
	workerCount := 5
	taskChan := make(chan task, 20)

	// 启动工作协程
	for i := 0; i < workerCount; i++ {
		go worker(taskChan)
	}

	// 每3秒执行一批写入
	ticker := time.NewTicker(3 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-wm.stopChan:
			return
		case <-ticker.C:
			// 生成1-5个随机写入任务
			taskCount := rand.Intn(5) + 1

			for i := 0; i < taskCount; i++ {
				// 随机选择1-100之间的电影ID
				movieID := fmt.Sprintf("%d", rand.Intn(100)+1)
				// 随机用户ID（1-1000）
				userID := fmt.Sprintf("%d", rand.Intn(1000)+1)
				// 随机评分（1-5，支持0.5分）
				rating := float64(rand.Intn(10)+1) / 2.0

				// 创建日志
				logEntry := map[string]interface{}{
					"timestamp": time.Now().Format(time.RFC3339),
					"movieId":   movieID,
					"userId":    userID,
					"rating":    rating,
					"status":    "pending",
				}

				// 更新统计信息
				wm.mu.Lock()
				wm.writeStats[movieID]++
				wm.writeLogs = append(wm.writeLogs, logEntry)
				// 保持日志不超过100条
				if len(wm.writeLogs) > 100 {
					wm.writeLogs = wm.writeLogs[1:]
				}
				wm.mu.Unlock()

				// 发送写入任务到工作池
				t := task{
					movieID:  movieID,
					userID:   userID,
					rating:   rating,
					logEntry: logEntry,
				}

				select {
				case taskChan <- t:
					// 任务成功提交
				default:
					// 通道已满，记录错误
					logrus.Warn("写入队列已满，丢弃任务")
					wm.mu.Lock()
					logEntry["status"] = "failed"
					logEntry["error"] = "写入队列已满"
					wm.mu.Unlock()
				}
			}
		}
	}
}

// 写入任务
type task struct {
	movieID  string
	userID   string
	rating   float64
	logEntry map[string]interface{}
}

// 工作协程
func worker(taskChan <-chan task) {
	for t := range taskChan {
		// 执行写入操作
		err := writeRating(t.movieID, t.userID, t.rating)

		// 更新日志状态
		WriteManagerInstance.mu.Lock()
		if err != nil {
			t.logEntry["status"] = "failed"
			t.logEntry["error"] = err.Error()
			logrus.Errorf("写入评分失败: %v", err)
		} else {
			t.logEntry["status"] = "success"
		}
		WriteManagerInstance.mu.Unlock()

		// 随机休眠50-200ms，模拟处理时间并减轻数据库负载
		time.Sleep(time.Duration(50+rand.Intn(150)) * time.Millisecond)
	}
}

// writeRating 写入评分数据到HBase
func writeRating(movieID, userID string, rating float64) error {
	ctx := context.Background()
	timestamp := time.Now().UnixNano() / 1000000 // 转为毫秒

	// 1. 写入ratings表（userId_movieId格式）
	ratingKey := fmt.Sprintf("%s_%s", userID, movieID)
	values := map[string]map[string][]byte{
		"data": {
			"rating":    []byte(fmt.Sprintf("%.1f", rating)),
			"timestamp": []byte(fmt.Sprintf("%d", timestamp)),
		},
	}

	putRequest, err := hrpc.NewPutStr(ctx, "ratings", ratingKey, values)
	if err != nil {
		return fmt.Errorf("创建ratings表Put请求失败: %v", err)
	}

	_, err = hbaseClient.Put(putRequest)
	if err != nil {
		return fmt.Errorf("ratings表写入失败: %v", err)
	}

	// 2. 写入movie_ratings表（movieId_userId格式）
	movieRatingKey := fmt.Sprintf("%s_%s", movieID, userID)
	values = map[string]map[string][]byte{
		"data": {
			"rating":    []byte(fmt.Sprintf("%.1f", rating)),
			"timestamp": []byte(fmt.Sprintf("%d", timestamp)),
		},
	}

	putRequest, err = hrpc.NewPutStr(ctx, "movie_ratings", movieRatingKey, values)
	if err != nil {
		return fmt.Errorf("创建movie_ratings表Put请求失败: %v", err)
	}

	_, err = hbaseClient.Put(putRequest)
	if err != nil {
		return fmt.Errorf("movie_ratings表写入失败: %v", err)
	}

	return nil
}
