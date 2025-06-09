package routes

import (
	"gohbase/controllers"
	"time"

	"github.com/gin-contrib/cors"
	"github.com/gin-gonic/gin"
)

// SetupRouter 设置路由
func SetupRouter() *gin.Engine {
	// 创建默认路由
	router := gin.Default()

	// 添加CORS中间件，允许所有来源、方法和头部
	router.Use(cors.New(cors.Config{
		AllowOrigins:     []string{"*"},
		AllowMethods:     []string{"GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"},
		AllowHeaders:     []string{"Origin", "Content-Type", "Accept", "Authorization", "X-Cache-Check", "X-Requested-With"},
		ExposeHeaders:    []string{"Content-Length", "X-Cache-Hit"},
		AllowCredentials: true,
		MaxAge:           12 * time.Hour,
	}))

	// 创建API路由组
	api := router.Group("/api")

	// 创建控制器实例
	movieController := &controllers.MovieController{}
	writeController := &controllers.WriteController{}

	// 电影相关路由
	movies := api.Group("/movies")
	{
		// GET /api/movies - 获取电影列表
		movies.GET("", movieController.GetMovies)

		// GET /api/movies/:id - 获取电影详情
		movies.GET("/:id", movieController.GetMovie)

		// GET /api/movies/random - 获取随机电影
		movies.GET("/random", movieController.GetRandomMovies)

		// POST /api/movies/random - 获取随机电影（POST方法）
		movies.POST("/random", movieController.RandomMoviesPost)

		// GET /api/movies/search - 搜索电影
		movies.GET("/search", movieController.SearchMovies)
	}

	// 评分相关路由
	ratings := api.Group("/ratings")
	{
		// GET /api/ratings/movie/:id - 获取电影的所有评分
		ratings.GET("/movie/:id", movieController.GetMovieRatings)
	}

	// 系统日志路由
	// GET /api/system/logs - 获取系统日志
	api.GET("/system/logs", movieController.GetSystemLogs)

	// 添加缓存统计路由
	// GET /api/system/cache - 获取缓存统计信息
	api.GET("/system/cache", movieController.GetCacheStats)

	// 添加随机写入相关路由
	write := api.Group("/write")
	{
		// GET /api/write/panel - 获取写入面板
		write.GET("/panel", writeController.GetWritePanel)

		// POST /api/write/start - 开始随机写入
		write.POST("/start", writeController.StartRandomWrites)

		// POST /api/write/stop - 停止随机写入
		write.POST("/stop", writeController.StopRandomWrites)

		// GET /api/write/status - 获取写入状态和日志
		write.GET("/status", writeController.GetWriteStatus)

		// GET /api/write/hotspots - 获取热点电影ID
		write.GET("/hotspots", writeController.GetHotspots)
	}

	// 返回路由
	return router
}
