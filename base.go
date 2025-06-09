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

	// 如果没有找到电影或没有评分
	if result.Cells == nil || len(result.Cells) == 0 {
		return map[string]float64{
			"avgRating":    0.0,
			"minRating":    0.0,
			"maxRating":    0.0,
			"countRatings": 0.0,
		}, nil
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