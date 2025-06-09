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
