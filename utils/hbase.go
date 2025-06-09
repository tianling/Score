package utils

import (
	"context"
	"fmt"
	"gohbase/config"
	"strconv"
	"strings"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/tsuna/gohbase"
	"github.com/tsuna/gohbase/hrpc"
)

var hbaseClient gohbase.Client

const RatingCacheTTL = 24 * time.Hour

// InitHBase 初始化HBase客户端
func InitHBase(conf *config.HBaseConfig) error {
	// 构建ZooKeeper连接字符串
	zkQuorum := fmt.Sprintf("%s:%s", conf.ZkQuorum, conf.ZkPort)

	// 创建HBase客户端
	hbaseClient = gohbase.NewClient(zkQuorum)

	// 测试连接是否成功
	ctx := context.Background()
	// 尝试获取一条记录来测试连接
	get, err := hrpc.NewGetStr(ctx, "movies", "1")
	if err != nil {
		logrus.Errorf("创建Get请求失败: %v", err)
		return err
	}

	_, err = hbaseClient.Get(get)
	if err != nil {
		logrus.Errorf("HBase连接失败: %v", err)
		return err
	}

	logrus.Info("HBase连接成功")
	return nil
}

// GetClient 获取HBase客户端
func GetClient() gohbase.Client {
	return hbaseClient
}

// GetMovie 根据ID获取电影信息，从多个表中获取数据
func GetMovie(ctx context.Context, movieID string) (map[string]map[string][]byte, error) {
	// 存储结果的映射
	resultMap := make(map[string]map[string][]byte)

	// 1. 从movies表获取基本信息
	movieGet, err := hrpc.NewGetStr(ctx, "movies", movieID)
	if err != nil {
		logrus.Errorf("创建电影信息Get请求失败: %v", err)
		return nil, err
	}

	movieResult, err := hbaseClient.Get(movieGet)
	if err != nil {
		logrus.Errorf("获取电影基本信息失败: %v", err)
		return nil, err
	}

	// 如果没有找到电影，直接返回空
	if movieResult.Cells == nil || len(movieResult.Cells) == 0 {
		return nil, nil
	}

	// 处理电影基本信息
	for _, cell := range movieResult.Cells {
		family := string(cell.Family)
		qualifier := string(cell.Qualifier)

		if _, ok := resultMap[family]; !ok {
			resultMap[family] = make(map[string][]byte)
		}

		resultMap[family][qualifier] = cell.Value
	}

	// 2. 从links表获取链接信息
	linksGet, err := hrpc.NewGetStr(ctx, "links", movieID)
	if err == nil { // 忽略错误，链接可能不存在
		linksResult, err := hbaseClient.Get(linksGet)
		if err == nil && linksResult.Cells != nil && len(linksResult.Cells) > 0 {
			for _, cell := range linksResult.Cells {
				family := "link" // 使用link作为映射键以保持与旧代码兼容
				qualifier := string(cell.Qualifier)

				if _, ok := resultMap[family]; !ok {
					resultMap[family] = make(map[string][]byte)
				}

				resultMap[family][qualifier] = cell.Value
			}
		}
	}

	// 3. 从avg_ratings表获取平均评分信息
	ratingGet, err := hrpc.NewGetStr(ctx, "avg_ratings", movieID)
	if err == nil { // 忽略错误，评分可能不存在
		ratingResult, err := hbaseClient.Get(ratingGet)
		if err == nil && ratingResult.Cells != nil && len(ratingResult.Cells) > 0 {
			for _, cell := range ratingResult.Cells {
				family := "rating" // 使用rating作为映射键以保持与旧代码兼容
				qualifier := string(cell.Qualifier)

				if _, ok := resultMap[family]; !ok {
					resultMap[family] = make(map[string][]byte)
				}

				resultMap[family][qualifier] = cell.Value
			}
		}
	}

	return resultMap, nil
}

// GetMovieWithFamilies 根据ID和指定的列族获取电影信息
func GetMovieWithFamilies(ctx context.Context, movieID string, families []string) (map[string]map[string][]byte, error) {
	// 构建列族映射
	familiesMap := make(map[string][]string)
	for _, family := range families {
		familiesMap[family] = nil
	}

	// 创建Get请求并指定列族
	get, err := hrpc.NewGetStr(ctx, "moviedata", movieID, hrpc.Families(familiesMap))
	if err != nil {
		return nil, err
	}

	result, err := hbaseClient.Get(get)
	if err != nil {
		return nil, err
	}

	// 如果没有找到电影
	if result.Cells == nil || len(result.Cells) == 0 {
		return nil, nil
	}

	// 手动构建结果映射
	resultMap := make(map[string]map[string][]byte)

	for _, cell := range result.Cells {
		family := string(cell.Family)
		qualifier := string(cell.Qualifier)

		if _, ok := resultMap[family]; !ok {
			resultMap[family] = make(map[string][]byte)
		}

		resultMap[family][qualifier] = cell.Value
	}

	return resultMap, nil
}

// GetMoviesMultiple 根据多个ID获取电影信息
func GetMoviesMultiple(ctx context.Context, movieIDs []string) (map[string]map[string]map[string][]byte, error) {
	results := make(map[string]map[string]map[string][]byte)

	// 使用goroutine并发获取多部电影信息
	type result struct {
		id   string
		data map[string]map[string][]byte
		err  error
	}

	resultChan := make(chan result, len(movieIDs))

	for _, id := range movieIDs {
		go func(movieID string) {
			data, err := GetMovie(ctx, movieID)
			resultChan <- result{id: movieID, data: data, err: err}
		}(id)
	}

	// 收集结果
	for range movieIDs {
		res := <-resultChan
		if res.err == nil && res.data != nil {
			results[res.id] = res.data
		}
	}

	return results, nil
}

// ParseMovieData 从HBase结果解析电影数据
func ParseMovieData(movieID string, data map[string]map[string][]byte) map[string]interface{} {
	result := map[string]interface{}{
		"movieId": movieID,
	}

	// 处理基本信息 - 来自movies表，列族为info
	if movieData, ok := data["info"]; ok {
		if title, ok := movieData["title"]; ok {
			result["title"] = string(title)
		}
		if genres, ok := movieData["genres"]; ok {
			result["genres"] = strings.Split(string(genres), "|")
		}
	}

	// 处理链接信息 - 来自links表，列族为external
	if linkData, ok := data["external"]; ok {
		links := map[string]interface{}{}

		if imdbId, ok := linkData["imdbId"]; ok {
			imdbIdStr := string(imdbId)
			links["imdbId"] = imdbIdStr
			links["imdbUrl"] = fmt.Sprintf("https://www.imdb.com/title/tt%s/", imdbIdStr)
		}

		if tmdbId, ok := linkData["tmdbId"]; ok {
			tmdbIdStr := string(tmdbId)
			links["tmdbId"] = tmdbIdStr
			links["tmdbUrl"] = fmt.Sprintf("https://www.themoviedb.org/movie/%s", tmdbIdStr)
		}

		result["links"] = links
	} else if linkData, ok := data["link"]; ok {
		// 兼容旧代码的link列族
		links := map[string]interface{}{}

		if imdbId, ok := linkData["imdbId"]; ok {
			imdbIdStr := string(imdbId)
			links["imdbId"] = imdbIdStr
			links["imdbUrl"] = fmt.Sprintf("https://www.imdb.com/title/tt%s/", imdbIdStr)
		}

		if tmdbId, ok := linkData["tmdbId"]; ok {
			tmdbIdStr := string(tmdbId)
			links["tmdbId"] = tmdbIdStr
			links["tmdbUrl"] = fmt.Sprintf("https://www.themoviedb.org/movie/%s", tmdbIdStr)
		}

		result["links"] = links
	} else {
		// 添加一个空的链接对象以避免前端错误
		result["links"] = map[string]interface{}{}
	}

	// 处理评分统计 - 来自avg_ratings表，列族为stats
	if statsData, ok := data["stats"]; ok {
		if avgRating, ok := statsData["avg_rating"]; ok {
			if rating, err := strconv.ParseFloat(string(avgRating), 64); err == nil {
				result["avgRating"] = rating
			}
		}

		if ratingCount, ok := statsData["rating_count"]; ok {
			if count, err := strconv.ParseInt(string(ratingCount), 10, 64); err == nil {
				result["ratingCount"] = count
			}
		}
	} else if ratingData, ok := data["rating"]; ok {
		// 兼容旧代码的rating列族
		if avgRating, ok := ratingData["rating"]; ok {
			if rating, err := strconv.ParseFloat(string(avgRating), 64); err == nil {
				result["avgRating"] = rating
			}
		}
	}

	return result
}

// ScanMovies 扫描电影列表（带缓存）
func ScanMovies(ctx context.Context, startRow, endRow string, limit int64) ([]*hrpc.Result, error) {
	// 构建缓存键
	cacheKey := fmt.Sprintf("scan_movies:%s:%s:%d", startRow, endRow, limit)

	// 检查缓存
	if cachedResults, found := Cache.Get(cacheKey); found {
		return cachedResults.([]*hrpc.Result), nil
	}

	// 创建扫描
	scan, err := hrpc.NewScanRangeStr(
		ctx,
		"movies",
		startRow,
		endRow,
		hrpc.NumberOfRows(uint32(limit)), // 设置最大行数
	)
	if err != nil {
		return nil, err
	}

	// 获取扫描器
	scanner := hbaseClient.Scan(scan)
	var results []*hrpc.Result

	// 扫描并获取结果
	for {
		res, err := scanner.Next()
		if err != nil {
			break // 到达结尾或发生错误，终止循环
		}

		results = append(results, res)
	}

	// 将结果存入缓存
	Cache.Set(cacheKey, results)

	return results, nil
}

// ScanMoviesWithFamilies 带特定列族的电影列表扫描
func ScanMoviesWithFamilies(ctx context.Context, startRow, endRow string, families []string, limit int64) ([]*hrpc.Result, error) {
	// 构建列族映射
	familiesMap := make(map[string][]string)
	for _, family := range families {
		familiesMap[family] = nil
	}

	// 创建扫描
	scan, err := hrpc.NewScanRangeStr(
		ctx,
		"movies",
		startRow,
		endRow,
		hrpc.Families(familiesMap),
		hrpc.NumberOfRows(uint32(limit)),
	)
	if err != nil {
		return nil, err
	}

	// 获取扫描器
	scanner := hbaseClient.Scan(scan)
	var results []*hrpc.Result

	// 扫描并获取结果
	for {
		res, err := scanner.Next()
		if err != nil {
			break // 到达结尾或发生错误，终止循环
		}

		results = append(results, res)
	}

	return results, nil
}

// ScanMoviesByGenre 按类型扫描电影
func ScanMoviesByGenre(ctx context.Context, genre string, limit int64) ([]*hrpc.Result, error) {
	// 创建扫描
	scan, err := hrpc.NewScanStr(ctx, "moviedata")
	if err != nil {
		return nil, err
	}

	// 获取扫描器
	scanner := hbaseClient.Scan(scan)
	var results []*hrpc.Result

	// 扫描并获取结果，在应用层进行过滤
	for {
		res, err := scanner.Next()
		if err != nil {
			break // 到达结尾或发生错误，终止循环
		}

		// 过滤结果，检查是否包含指定类型
		hasGenre := false
		for _, cell := range res.Cells {
			if string(cell.Family) == "movie" &&
				string(cell.Qualifier) == "genres" &&
				strings.Contains(string(cell.Value), genre) {
				hasGenre = true
				break
			}
		}

		if hasGenre {
			results = append(results, res)

			// 如果结果数量已经达到限制，则停止扫描
			if int64(len(results)) >= limit {
				break
			}
		}
	}

	return results, nil
}

// ScanMoviesByTag 按标签扫描电影
func ScanMoviesByTag(ctx context.Context, tag string, limit int64) ([]*hrpc.Result, error) {
	// 设置要获取的列族
	familiesMap := map[string][]string{"tag": nil}

	// 创建扫描请求
	scan, err := hrpc.NewScanStr(ctx, "moviedata",
		hrpc.Families(familiesMap))
	if err != nil {
		return nil, err
	}

	// 获取扫描器
	scanner := hbaseClient.Scan(scan)
	var results []*hrpc.Result

	// 扫描并获取结果并在应用层筛选包含标签的结果
	for {
		res, err := scanner.Next()
		if err != nil {
			break // 到达结尾或发生错误，终止循环
		}

		// 过滤结果，检查是否包含指定标签
		hasTag := false
		for _, cell := range res.Cells {
			if string(cell.Family) == "tag" && strings.Contains(string(cell.Value), tag) {
				hasTag = true
				break
			}
		}

		if hasTag {
			results = append(results, res)

			// 如果结果数量已经达到限制，则停止扫描
			if int64(len(results)) >= limit {
				break
			}
		}
	}

	return results, nil
}

// ScanMoviesWithPagination 扫描电影列表并支持分页
func ScanMoviesWithPagination(ctx context.Context, page, pageSize int) ([]*hrpc.Result, int, error) {
	// 计算分页参数
	startRow := strconv.Itoa((page-1)*pageSize + 1) // 从1开始
	endRow := strconv.Itoa(page*pageSize + 1)       // 不包含

	// 创建扫描
	scan, err := hrpc.NewScanRangeStr(
		ctx,
		"movies",
		startRow,
		endRow,
		hrpc.NumberOfRows(uint32(pageSize)),
	)
	if err != nil {
		return nil, 0, err
	}

	// 获取扫描器
	scanner := hbaseClient.Scan(scan)
	var results []*hrpc.Result

	// 扫描并获取结果
	for {
		res, err := scanner.Next()
		if err != nil {
			break // 到达结尾或发生错误，终止循环
		}

		results = append(results, res)
	}

	// 获取总记录数 - 这里我们假设固定数量，实际应用中应该从HBase获取
	totalRecords := 9742 // 从文档了解到的总电影数量

	return results, totalRecords, nil
}

// GetMovieRatingStats 获取电影评分统计信息
func GetMovieRatingStats(ctx context.Context, movieID string) (map[string]float64, error) {
	// 构建缓存键
	cacheKey := fmt.Sprintf("movie_rating_stats:%s", movieID)

	// 检查缓存
	if cachedData, found := Cache.Get(cacheKey); found {
		return cachedData.(map[string]float64), nil
	}

	// 创建Get请求，从avg_ratings表获取数据
	get, err := hrpc.NewGetStr(ctx, "avg_ratings", movieID)
	if err != nil {
		return nil, err
	}

	result, err := hbaseClient.Get(get)
	if err != nil {
		return nil, err
	}

	// 如果没有找到电影或没有评分，则触发实时计算
	if result.Cells == nil || len(result.Cells) == 0 {
		logrus.Infof("电影ID %s 的评分统计信息未在avg_ratings中找到，正在重新计算...", movieID)

		// 缓存未命中，从头开始计算
		fullRatingsData, err := calculateMovieRatings(ctx, movieID)
		if err != nil {
			return nil, fmt.Errorf("计算电影 %s 的评分失败: %v", movieID, err)
		}

		// 异步保存新的统计数据到avg_ratings表
		go func() {
			// 为后台任务创建一个新的上下文以避免被取消
			bgCtx := context.Background()
			if err := SaveMovieStats(bgCtx, movieID, fullRatingsData); err != nil {
				logrus.Errorf("后台保存电影 %s 的统计信息失败: %v", movieID, err)
			}
		}()

		// 转换数据为函数期望的返回类型
		stats := map[string]float64{
			"avgRating":    fullRatingsData["avgRating"].(float64),
			"minRating":    fullRatingsData["minRating"].(float64),
			"maxRating":    fullRatingsData["maxRating"].(float64),
			"countRatings": float64(fullRatingsData["count"].(int)),
		}

		// 将新计算的结果存入缓存
		Cache.Set(cacheKey, stats)

		return stats, nil
	}

	// 解析评分数据
	var avg, min, max float64
	var count int64

	// 遍历所有单元格
	for _, cell := range result.Cells {
		family := string(cell.Family)
		qualifier := string(cell.Qualifier)
		value := string(cell.Value)

		if family == "stats" {
			switch qualifier {
			case "avg_rating":
				avg, _ = strconv.ParseFloat(value, 64)
			case "min_rating":
				min, _ = strconv.ParseFloat(value, 64)
			case "max_rating":
				max, _ = strconv.ParseFloat(value, 64)
			case "rating_count":
				count, _ = strconv.ParseInt(value, 10, 64)
			}
		}
	}

	// 构建结果
	stats := map[string]float64{
		"avgRating":    avg,
		"minRating":    min,
		"maxRating":    max,
		"countRatings": float64(count),
	}

	// 将结果存入缓存
	Cache.Set(cacheKey, stats)

	return stats, nil
}

// GetMoviesByRatingRange 获取特定评分范围内的电影
func GetMoviesByRatingRange(ctx context.Context, minRating, maxRating float64, limit int64) ([]string, error) {
	// 构建缓存键
	cacheKey := fmt.Sprintf("movies_by_rating:%f:%f:%d", minRating, maxRating, limit)

	// 检查缓存
	if cachedData, found := Cache.Get(cacheKey); found {
		return cachedData.([]string), nil
	}

	// 创建扫描请求，从avg_ratings表获取数据
	scan, err := hrpc.NewScanStr(ctx, "avg_ratings",
		hrpc.Families(map[string][]string{"stats": {"avg_rating"}}))
	if err != nil {
		return nil, err
	}

	// 获取扫描器
	scanner := hbaseClient.Scan(scan)

	// 存储满足条件的电影ID
	var matchedMovieIDs []string

	// 扫描所有电影
	for {
		res, err := scanner.Next()
		if err != nil {
			break // 到达结尾或发生错误，终止循环
		}

		if len(res.Cells) == 0 {
			continue
		}

		// 获取电影ID
		movieID := string(res.Cells[0].Row)
		var avgRating float64

		// 遍历所有单元格
		for _, cell := range res.Cells {
			if string(cell.Family) == "stats" && string(cell.Qualifier) == "avg_rating" {
				avgRating, _ = strconv.ParseFloat(string(cell.Value), 64)
				break
			}
		}

		// 检查评分是否在范围内
		if avgRating >= minRating && avgRating <= maxRating {
			matchedMovieIDs = append(matchedMovieIDs, movieID)

			// 如果结果数量已经达到限制，则停止扫描
			if int64(len(matchedMovieIDs)) >= limit {
				break
			}
		}
	}

	// 将结果存入缓存
	Cache.Set(cacheKey, matchedMovieIDs)

	return matchedMovieIDs, nil
}

// GetMovieWithAllData 获取电影的所有数据，包括基本信息、链接、评分和标签
func GetMovieWithAllData(ctx context.Context, movieID string) (map[string]interface{}, error) {
	// 获取电影的所有数据
	get, err := hrpc.NewGetStr(ctx, "moviedata", movieID)
	if err != nil {
		return nil, err
	}

	result, err := hbaseClient.Get(get)
	if err != nil {
		return nil, err
	}

	if result.Cells == nil || len(result.Cells) == 0 {
		return nil, nil
	}

	// 手动构建结果映射
	resultMap := make(map[string]map[string][]byte)

	for _, cell := range result.Cells {
		family := string(cell.Family)
		qualifier := string(cell.Qualifier)

		if _, ok := resultMap[family]; !ok {
			resultMap[family] = make(map[string][]byte)
		}

		resultMap[family][qualifier] = cell.Value
	}

	return ParseMovieData(movieID, resultMap), nil
}

// EnableCompression 为表启用压缩功能
// 注意：此功能需要HBase管理员权限，通常在初始设置时使用
func EnableCompression(compression string) error {
	// 检查压缩算法是否有效
	validCompressions := map[string]bool{
		"SNAPPY": true,
		"GZ":     true,
		"LZO":    true,
		"NONE":   true,
	}

	if !validCompressions[strings.ToUpper(compression)] {
		return fmt.Errorf("无效的压缩算法: %s. 有效的选项包括: SNAPPY, GZ, LZO, NONE", compression)
	}

	// 压缩设置通常通过HBase Shell执行
	familyList := []string{"movie", "link", "rating", "tag"}
	var commands []string

	for _, family := range familyList {
		cmd := fmt.Sprintf("alter 'moviedata', {NAME => '%s', COMPRESSION => '%s'}",
			family, strings.ToUpper(compression))
		commands = append(commands, cmd)
	}

	// 只记录一条总体提示日志
	logrus.Info("请在HBase Shell中执行压缩命令设置列族压缩")

	return nil
}

// SaveMovieStats 将计算出的电影统计信息保存到avg_ratings表。
func SaveMovieStats(ctx context.Context, movieID string, stats map[string]interface{}) error {
	timestamp := time.Now().Format(time.RFC3339)

	avgRating, _ := stats["avgRating"].(float64)
	ratingCount, _ := stats["count"].(int)
	minRating, _ := stats["minRating"].(float64)
	maxRating, _ := stats["maxRating"].(float64)

	values := map[string]map[string][]byte{
		"stats": {
			"avg_rating":   []byte(fmt.Sprintf("%.2f", avgRating)),
			"rating_count": []byte(strconv.Itoa(ratingCount)),
			"min_rating":   []byte(fmt.Sprintf("%.1f", minRating)),
			"max_rating":   []byte(fmt.Sprintf("%.1f", maxRating)),
			"updated_time": []byte(timestamp),
		},
	}

	putRequest, err := hrpc.NewPutStr(ctx, "avg_ratings", movieID, values)
	if err != nil {
		return fmt.Errorf("为avg_ratings创建Put请求失败: %v", err)
	}

	_, err = hbaseClient.Put(putRequest)
	if err != nil {
		return fmt.Errorf("写入avg_ratings失败: %v", err)
	}

	logrus.Infof(
		"成功存储电影ID %s 的平均评分。统计信息: {平均分: %.2f, 数量: %d, 最低分: %.1f, 最高分: %.1f}",
		movieID,
		avgRating,
		ratingCount,
		minRating,
		maxRating,
	)
	return nil
}

// GetMovieRatings 是获取电影评分的新公共入口点。
// 它取代了旧的GetMovieRatings并包含了缓存逻辑。
func GetMovieRatings(ctx context.Context, movieID string) (map[string]interface{}, error) {
	// 1. 尝试从avg_ratings表（缓存）获取
	get, err := hrpc.NewGetStr(ctx, "avg_ratings", movieID)
	if err == nil {
		result, err := hbaseClient.Get(get)
		if err == nil && result != nil && len(result.Cells) > 0 {
			cachedStats, updatedTime := parseAvgRatings(result)
			if time.Since(updatedTime) < RatingCacheTTL {
				logrus.Infof("电影统计信息缓存命中: %s", movieID)
				rawRatingsList, err := fetchRawRatingsList(ctx, movieID)
				if err != nil {
					logrus.Warnf("获取电影 %s 的原始评分列表失败: %v", movieID, err)
					rawRatingsList = []map[string]interface{}{}
				}
				cachedStats["ratings"] = rawRatingsList
				return cachedStats, nil
			}
			logrus.Infof("电影统计信息缓存已过期: %s", movieID)
		}
	}

	// 2. 缓存未命中或已过期，从头开始计算
	logrus.Infof("电影统计信息缓存未命中，开始计算: %s", movieID)
	fullRatingsData, err := calculateMovieRatings(ctx, movieID)
	if err != nil {
		return nil, fmt.Errorf("计算电影 %s 的评分失败: %v", movieID, err)
	}

	// 3. 异步保存新的统计数据到avg_ratings表
	logrus.Infof("触发异步存储电影ID %s 的平均评分", movieID)
	go func() {
		// 为后台任务创建一个新的上下文以避免被取消。
		bgCtx := context.Background()
		if err := SaveMovieStats(bgCtx, movieID, fullRatingsData); err != nil {
			logrus.Errorf("后台保存电影 %s 的统计信息失败: %v", movieID, err)
		}
	}()

	return fullRatingsData, nil
}

// calculateMovieRatings 通过扫描movie_ratings表来执行实际的计算。
// 此函数包含以前GetMovieRatings的逻辑。
func calculateMovieRatings(ctx context.Context, movieID string) (map[string]interface{}, error) {
	// (旧GetMovieRatings函数的代码放在这里)
	startRow := fmt.Sprintf("%s_", movieID)
	endRow := fmt.Sprintf("%s_z", movieID)

	scanRequest, err := hrpc.NewScanRangeStr(ctx, "movie_ratings", startRow, endRow,
		hrpc.Families(map[string][]string{"data": {"rating", "timestamp"}}))
	if err != nil {
		return nil, err
	}

	scanner := hbaseClient.Scan(scanRequest)

	ratingsList := make([]map[string]interface{}, 0)
	var ratings []float64

	for {
		result, err := scanner.Next()
		if err != nil {
			break
		}

		if len(result.Cells) == 0 {
			continue
		}

		rowKey := string(result.Cells[0].Row)
		parts := strings.Split(rowKey, "_")
		if len(parts) != 2 {
			continue
		}

		userId := parts[1]
		var rating float64
		var timestamp int64

		for _, cell := range result.Cells {
			qualifier := string(cell.Qualifier)
			if qualifier == "rating" {
				rating, _ = strconv.ParseFloat(string(cell.Value), 64)
			} else if qualifier == "timestamp" {
				timestamp, _ = strconv.ParseInt(string(cell.Value), 10, 64)
			}
		}

		if rating > 0 {
			ratings = append(ratings, rating)
			ratingsList = append(ratingsList, map[string]interface{}{
				"userId":    userId,
				"rating":    rating,
				"timestamp": timestamp,
			})
		}
	}

	if len(ratings) == 0 {
		return map[string]interface{}{
			"ratings":   []map[string]interface{}{},
			"count":     0,
			"avgRating": 0.0,
			"minRating": 0.0,
			"maxRating": 0.0,
		}, nil
	}

	var sum, min, max float64
	count := len(ratings)
	min = ratings[0]
	max = ratings[0]
	sum = 0.0

	for _, r := range ratings {
		sum += r
		if r < min {
			min = r
		}
		if r > max {
			max = r
		}
	}

	avg := sum / float64(count)

	return map[string]interface{}{
		"ratings":   ratingsList,
		"count":     count,
		"avgRating": avg,
		"minRating": min,
		"maxRating": max,
	}, nil
}

// fetchRawRatingsList 仅获取评分列表，不计算统计数据。
func fetchRawRatingsList(ctx context.Context, movieID string) ([]map[string]interface{}, error) {
	startRow := fmt.Sprintf("%s_", movieID)
	endRow := fmt.Sprintf("%s_z", movieID)

	scanRequest, err := hrpc.NewScanRangeStr(ctx, "movie_ratings", startRow, endRow,
		hrpc.Families(map[string][]string{"data": {"rating", "timestamp"}}))
	if err != nil {
		return nil, err
	}

	scanner := hbaseClient.Scan(scanRequest)
	ratingsList := make([]map[string]interface{}, 0)

	for {
		result, err := scanner.Next()
		if err != nil {
			break
		}
		if len(result.Cells) == 0 {
			continue
		}

		rowKey := string(result.Cells[0].Row)
		parts := strings.Split(rowKey, "_")
		if len(parts) != 2 {
			continue
		}

		userId := parts[1]
		var rating float64
		var timestamp int64

		for _, cell := range result.Cells {
			qualifier := string(cell.Qualifier)
			if qualifier == "rating" {
				rating, _ = strconv.ParseFloat(string(cell.Value), 64)
			} else if qualifier == "timestamp" {
				timestamp, _ = strconv.ParseInt(string(cell.Value), 10, 64)
			}
		}

		if rating > 0 {
			ratingsList = append(ratingsList, map[string]interface{}{
				"userId":    userId,
				"rating":    rating,
				"timestamp": timestamp,
			})
		}
	}
	return ratingsList, nil
}

// parseAvgRatings 是一个辅助函数，用于从avg_ratings表解析统计信息。
func parseAvgRatings(result *hrpc.Result) (stats map[string]interface{}, updatedTime time.Time) {
	stats = make(map[string]interface{})
	var avg, min, max float64
	var count int

	for _, cell := range result.Cells {
		qualifier := string(cell.Qualifier)
		value := string(cell.Value)

		switch qualifier {
		case "avg_rating":
			avg, _ = strconv.ParseFloat(value, 64)
		case "rating_count":
			count, _ = strconv.Atoi(value)
		case "min_rating":
			min, _ = strconv.ParseFloat(value, 64)
		case "max_rating":
			max, _ = strconv.ParseFloat(value, 64)
		case "updated_time":
			updatedTime, _ = time.Parse(time.RFC3339, value)
		}
	}

	stats["avgRating"] = avg
	stats["count"] = count
	stats["minRating"] = min
	stats["maxRating"] = max
	return stats, updatedTime
}

// GetMovieTags 获取电影的所有标签
func GetMovieTags(ctx context.Context, movieID string) ([]map[string]interface{}, error) {
	// 构建缓存键
	cacheKey := fmt.Sprintf("movie_tags:%s", movieID)

	// 检查缓存
	if cachedData, found := Cache.Get(cacheKey); found {
		return cachedData.([]map[string]interface{}), nil
	}

	// 创建扫描，使用tags表
	// 使用扫描后在应用层过滤
	scan, err := hrpc.NewScanStr(ctx, "tags",
		hrpc.Families(map[string][]string{"data": {"tag"}}))
	if err != nil {
		return nil, err
	}

	scanner := hbaseClient.Scan(scan)

	// 存储标签的结果
	tags := make([]map[string]interface{}, 0)

	// 扫描所有结果
	for {
		result, err := scanner.Next()
		if err != nil {
			break // 扫描结束或发生错误
		}

		if len(result.Cells) == 0 {
			continue
		}

		// 获取行键，格式为 userId_movieId_timestamp
		rowKey := string(result.Cells[0].Row)

		// 检查行键是否包含目标电影ID
		if !strings.Contains(rowKey, "_"+movieID+"_") {
			continue // 跳过不相关的行
		}

		// 解析行键
		parts := strings.Split(rowKey, "_")
		if len(parts) != 3 {
			continue // 跳过格式不正确的行键
		}

		// 提取userId和timestamp
		userId := parts[0]
		timestamp := parts[2]

		var tagContent string

		// 处理每个结果
		for _, cell := range result.Cells {
			if string(cell.Family) == "data" && string(cell.Qualifier) == "tag" {
				tagContent = string(cell.Value)
			}
		}

		// 只有当标签有内容时才添加
		if tagContent != "" {
			tagInfo := map[string]interface{}{
				"userId":    userId,
				"movieId":   movieID,
				"tag":       tagContent,
				"timestamp": timestamp,
			}
			tags = append(tags, tagInfo)
		}
	}

	// 将结果存入缓存
	Cache.Set(cacheKey, tags)

	return tags, nil
}

// GetUserRating 获取特定用户对电影的评分
func GetUserRating(ctx context.Context, movieID string, userID string) (float64, int64, error) {
	// 构建要获取的列列表 - 适配通用格式
	families := map[string][]string{
		"rating": nil,
	}

	// 创建Get请求
	get, err := hrpc.NewGetStr(ctx, "moviedata", movieID, hrpc.Families(families))
	if err != nil {
		return 0, 0, err
	}

	result, err := hbaseClient.Get(get)
	if err != nil {
		return 0, 0, err
	}

	// 如果没有找到电影或该用户没有评分
	if result.Cells == nil || len(result.Cells) == 0 {
		return 0, 0, nil
	}

	var rating float64
	var timestamp int64

	// 构建结果映射
	resultMap := make(map[string]map[string][]byte)
	for _, cell := range result.Cells {
		family := string(cell.Family)
		qualifier := string(cell.Qualifier)

		if _, ok := resultMap[family]; !ok {
			resultMap[family] = make(map[string][]byte)
		}

		resultMap[family][qualifier] = cell.Value
	}

	// 首先尝试获取通用格式的评分
	if ratingData, ok := resultMap["rating"]; ok {
		// 尝试读取通用格式的评分和时间戳
		if ratingValue, ok := ratingData["rating"]; ok {
			rating, _ = strconv.ParseFloat(string(ratingValue), 64)

			if timestampValue, ok := ratingData["timestamp"]; ok {
				timestamp, _ = strconv.ParseInt(string(timestampValue), 10, 64)
			}

			return rating, timestamp, nil
		}

		// 回退到旧的复合列格式
		if ratingValue, ok := ratingData[fmt.Sprintf("rating:%s", userID)]; ok {
			rating, _ = strconv.ParseFloat(string(ratingValue), 64)
		}

		if timestampValue, ok := ratingData[fmt.Sprintf("timestamp:%s", userID)]; ok {
			timestamp, _ = strconv.ParseInt(string(timestampValue), 10, 64)
		}
	}

	return rating, timestamp, nil
}
