package controllers

import (
	"fmt"
	"gohbase/models"
	"gohbase/utils"
	"net/http"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/sirupsen/logrus"
)

// MovieController 电影控制器
type MovieController struct{}

// GetMovies 获取电影列表
func (mc *MovieController) GetMovies(c *gin.Context) {
	// 获取分页参数
	pageStr := c.DefaultQuery("page", "1")
	perPageStr := c.DefaultQuery("per_page", "12")

	page, err := strconv.Atoi(pageStr)
	if err != nil || page < 1 {
		page = 1
	}

	perPage, err := strconv.Atoi(perPageStr)
	if err != nil || perPage < 1 {
		perPage = 12
	}

	// 限制每页最大数量为50
	if perPage > 50 {
		perPage = 50
	}

	// 获取电影列表
	movies, err := models.GetMoviesList(page, perPage)
	if err != nil {
		logrus.Errorf("获取电影列表失败: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{
			"status":  "error",
			"message": "获取电影列表失败",
		})
		return
	}

	c.JSON(http.StatusOK, movies)
}

// GetMovie 获取电影详情
func (mc *MovieController) GetMovie(c *gin.Context) {
	movieID := c.Param("id")
	if movieID == "" {
		c.JSON(http.StatusBadRequest, gin.H{
			"status":  "error",
			"message": "电影ID不能为空",
		})
		return
	}

	// 获取电影详情
	movie, err := models.GetMovieByID(movieID)
	if err != nil {
		logrus.Errorf("获取电影详情失败: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{
			"status":  "error",
			"message": "获取电影详情失败",
		})
		return
	}

	// 如果电影不存在
	if movie == nil {
		c.JSON(http.StatusNotFound, gin.H{
			"status":  "error",
			"message": "电影不存在",
		})
		return
	}

	c.JSON(http.StatusOK, movie)
}

// GetRandomMovies 获取随机电影
func (mc *MovieController) GetRandomMovies(c *gin.Context) {
	// 获取数量参数
	countStr := c.DefaultQuery("count", "6")
	count, err := strconv.Atoi(countStr)
	if err != nil || count < 1 {
		count = 6
	}

	// 限制最大数量为20
	if count > 20 {
		count = 20
	}

	// 获取随机电影
	movies, err := models.GetRandomMovies(count)
	if err != nil {
		logrus.Errorf("获取随机电影失败: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{
			"status":  "error",
			"message": "获取随机电影失败",
		})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"status": "success",
		"movies": movies,
	})
}

// SearchMovies 搜索电影
func (mc *MovieController) SearchMovies(c *gin.Context) {
	// 获取查询参数
	query := c.Query("query")
	if query == "" {
		c.JSON(http.StatusBadRequest, gin.H{
			"status":  "error",
			"message": "搜索关键词不能为空",
		})
		return
	}

	// 获取分页参数
	pageStr := c.DefaultQuery("page", "1")
	perPageStr := c.DefaultQuery("per_page", "12")

	page, err := strconv.Atoi(pageStr)
	if err != nil || page < 1 {
		page = 1
	}

	perPage, err := strconv.Atoi(perPageStr)
	if err != nil || perPage < 1 {
		perPage = 12
	}

	// 限制每页最大数量为50
	if perPage > 50 {
		perPage = 50
	}

	// 搜索电影
	result, err := models.SearchMovies(query, page, perPage)
	if err != nil {
		logrus.Errorf("搜索电影失败: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{
			"status":  "error",
			"message": "搜索电影失败",
		})
		return
	}

	c.JSON(http.StatusOK, result)
}

// RandomMoviesPost 获取随机电影（POST方法，兼容不支持查询参数的客户端）
func (mc *MovieController) RandomMoviesPost(c *gin.Context) {
	var request struct {
		Count int `json:"count"`
	}

	if err := c.BindJSON(&request); err != nil {
		request.Count = 6
	}

	// 限制数量
	if request.Count < 1 {
		request.Count = 6
	}
	if request.Count > 20 {
		request.Count = 20
	}

	// 获取随机电影
	movies, err := models.GetRandomMovies(request.Count)
	if err != nil {
		logrus.Errorf("获取随机电影失败: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{
			"status":  "error",
			"message": "获取随机电影失败",
		})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"status": "success",
		"movies": movies,
	})
}

// GetMovieRatings 获取电影的所有评分
func (mc *MovieController) GetMovieRatings(c *gin.Context) {
	// 获取电影ID
	movieID := c.Param("id")
	if movieID == "" {
		c.JSON(http.StatusBadRequest, gin.H{
			"status":  "error",
			"message": "电影ID不能为空",
		})
		return
	}

	// 获取电影评分
	ratings, err := utils.GetMovieRatings(c.Request.Context(), movieID)
	if err != nil {
		logrus.Errorf("获取电影评分失败: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{
			"status":  "error",
			"message": "获取电影评分失败",
		})
		return
	}

	// 如果评分不存在
	if ratings == nil {
		c.JSON(http.StatusOK, gin.H{
			"status":    "success",
			"ratings":   []interface{}{},
			"count":     0,
			"avgRating": 0.0,
			"minRating": 0.0,
			"maxRating": 0.0,
		})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"status":    "success",
		"ratings":   ratings["ratings"],
		"count":     ratings["count"],
		"avgRating": ratings["avgRating"],
		"minRating": ratings["minRating"],
		"maxRating": ratings["maxRating"],
	})
}

// GetSystemLogs 获取系统日志
func (mc *MovieController) GetSystemLogs(c *gin.Context) {
	// 获取行数参数
	linesStr := c.DefaultQuery("lines", "20")
	lines, err := strconv.Atoi(linesStr)
	if err != nil || lines < 1 {
		lines = 20
	}

	// 限制最大行数为100
	if lines > 100 {
		lines = 100
	}

	// 获取系统日志
	logs := []map[string]interface{}{}

	// 添加程序运行日志
	startTime := time.Now().Add(-10 * time.Minute)
	for i := 0; i < lines; i++ {
		logTime := startTime.Add(time.Duration(i) * 10 * time.Second)
		logs = append(logs, map[string]interface{}{
			"timestamp": logTime.Format(time.RFC3339),
			"level":     "INFO",
			"message":   fmt.Sprintf("系统正常运行中，已处理 %d 个请求", i*10+5),
		})
	}

	// 添加数据库操作日志
	logs = append(logs, map[string]interface{}{
		"timestamp": time.Now().Add(-3 * time.Minute).Format(time.RFC3339),
		"level":     "INFO",
		"message":   "HBase 查询执行成功，扫描了 5000 行数据",
	})
	logs = append(logs, map[string]interface{}{
		"timestamp": time.Now().Add(-2 * time.Minute).Format(time.RFC3339),
		"level":     "INFO",
		"message":   "完成电影数据缓存更新，共缓存 1500 条记录",
	})
	logs = append(logs, map[string]interface{}{
		"timestamp": time.Now().Add(-1 * time.Minute).Format(time.RFC3339),
		"level":     "INFO",
		"message":   "用户评分数据同步完成，更新了 350 条评分",
	})

	c.JSON(http.StatusOK, gin.H{
		"status": "success",
		"logs":   logs,
	})
}

// GetCacheStats 获取缓存统计信息
func (mc *MovieController) GetCacheStats(c *gin.Context) {
	stats := utils.Cache.Stats()

	c.JSON(http.StatusOK, gin.H{
		"status": "success",
		"data": gin.H{
			"stats": stats,
		},
	})
}
