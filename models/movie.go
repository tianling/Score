package models

import (
	"context"
	"fmt"
	"gohbase/utils"
	"math/rand"
	"strconv"
	"strings"
	"time"

	"github.com/tsuna/gohbase/hrpc"
)

// 全局随机数生成器
var rng = rand.New(rand.NewSource(time.Now().UnixNano()))

// Movie 电影模型
type Movie struct {
	MovieID   string   `json:"movieId"`
	Title     string   `json:"title"`
	Genres    []string `json:"genres"`
	Year      int      `json:"year,omitempty"`
	AvgRating float64  `json:"avgRating"`
	Links     Links    `json:"links,omitempty"`
	Tags      []string `json:"tags,omitempty"`
}

// Links 外部链接
type Links struct {
	ImdbID  string `json:"imdbId,omitempty"`
	ImdbURL string `json:"imdbUrl,omitempty"`
	TmdbID  string `json:"tmdbId,omitempty"`
	TmdbURL string `json:"tmdbUrl,omitempty"`
}

// MovieList 电影列表响应
type MovieList struct {
	Movies      []Movie `json:"movies"`
	TotalMovies int     `json:"totalMovies"`
	Page        int     `json:"page"`
	PerPage     int     `json:"perPage"`
	TotalPages  int     `json:"totalPages"`
}

// MovieDetail 电影详情响应
type MovieDetail struct {
	Movie       Movie               `json:"movie"`
	Ratings     []Rating            `json:"ratings,omitempty"`
	TaggedUsers []map[string]string `json:"taggedUsers,omitempty"`
	Stats       map[string]float64  `json:"stats,omitempty"`
}

// Rating 评分
type Rating struct {
	UserID string  `json:"userId"`
	Rating float64 `json:"rating"`
}

// GetMovieByID 根据ID获取电影（带缓存）
func GetMovieByID(movieID string) (*MovieDetail, error) {
	// 构建缓存键
	cacheKey := fmt.Sprintf("movie_detail:%s", movieID)

	// 检查缓存
	if cachedData, found := utils.Cache.Get(cacheKey); found {
		return cachedData.(*MovieDetail), nil
	}

	ctx := context.Background()

	// 从HBase获取电影数据
	data, err := utils.GetMovie(ctx, movieID)
	if err != nil {
		return nil, err
	}

	// 如果电影不存在
	if data == nil {
		return nil, nil
	}

	// 解析电影数据
	movieData := utils.ParseMovieData(movieID, data)

	// 构建电影详情响应
	detail := &MovieDetail{}

	// 设置基本信息
	movie := Movie{
		MovieID: movieID,
	}

	if title, ok := movieData["title"].(string); ok {
		movie.Title = title
		// 尝试从标题中提取年份
		if matches := strings.Split(title, " ("); len(matches) > 1 {
			yearStr := strings.TrimSuffix(matches[len(matches)-1], ")")
			if year, err := strconv.Atoi(yearStr); err == nil {
				movie.Year = year
			}
		}
	}

	if genres, ok := movieData["genres"].([]string); ok {
		movie.Genres = genres
	}

	if avgRating, ok := movieData["avgRating"].(float64); ok {
		movie.AvgRating = avgRating
	}

	// 设置链接
	if links, ok := movieData["links"].(map[string]interface{}); ok {
		linkObj := Links{}

		if imdbId, ok := links["imdbId"].(string); ok {
			linkObj.ImdbID = imdbId
		}
		if imdbUrl, ok := links["imdbUrl"].(string); ok {
			linkObj.ImdbURL = imdbUrl
		}
		if tmdbId, ok := links["tmdbId"].(string); ok {
			linkObj.TmdbID = tmdbId
		}
		if tmdbUrl, ok := links["tmdbUrl"].(string); ok {
			linkObj.TmdbURL = tmdbUrl
		}

		movie.Links = linkObj
	}

	// 设置标签
	if uniqueTags, ok := movieData["uniqueTags"].([]string); ok {
		movie.Tags = uniqueTags
	}

	detail.Movie = movie

	// 设置评分
	if ratings, ok := movieData["ratings"].([]map[string]interface{}); ok {
		detailRatings := []Rating{}

		for _, r := range ratings {
			userId, ok1 := r["userId"].(string)
			rating, ok2 := r["rating"].(float64)

			if ok1 && ok2 {
				detailRatings = append(detailRatings, Rating{
					UserID: userId,
					Rating: rating,
				})
			}
		}

		detail.Ratings = detailRatings
	}

	// 构建统计数据
	detail.Stats = map[string]float64{
		"ratingCount": float64(len(detail.Ratings)),
		"tagCount":    float64(len(movie.Tags)),
	}

	// 将结果存入缓存
	utils.Cache.Set(cacheKey, detail)

	return detail, nil
}

// GetMoviesList 获取电影列表
func GetMoviesList(page, perPage int) (*MovieList, error) {
	ctx := context.Background()

	// 计算分页参数
	startIdx := (page-1)*perPage + 1 // 从1开始
	endIdx := startIdx + perPage

	// 扫描电影范围
	startRow := fmt.Sprintf("%d", startIdx)
	endRow := fmt.Sprintf("%d", endIdx)

	results, err := utils.ScanMovies(ctx, startRow, endRow, int64(perPage))
	if err != nil {
		return nil, err
	}

	// 解析电影列表
	movies := []Movie{}

	for _, result := range results {
		// 获取行键（即movieId）
		var movieID string
		for _, cell := range result.Cells {
			movieID = string(cell.Row)
			break
		}

		if movieID == "" {
			continue
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

		movieData := utils.ParseMovieData(movieID, resultMap)

		movie := Movie{
			MovieID: movieID,
		}

		if title, ok := movieData["title"].(string); ok {
			movie.Title = title
			// 尝试从标题中提取年份
			if matches := strings.Split(title, " ("); len(matches) > 1 {
				yearStr := strings.TrimSuffix(matches[len(matches)-1], ")")
				if year, err := strconv.Atoi(yearStr); err == nil {
					movie.Year = year
				}
			}
		}

		if genres, ok := movieData["genres"].([]string); ok {
			movie.Genres = genres
		}

		if avgRating, ok := movieData["avgRating"].(float64); ok {
			movie.AvgRating = avgRating
		}

		// 添加链接数据
		if links, ok := movieData["links"].(map[string]interface{}); ok {
			linkObj := Links{}

			if imdbId, ok := links["imdbId"].(string); ok {
				linkObj.ImdbID = imdbId
			}
			if imdbUrl, ok := links["imdbUrl"].(string); ok {
				linkObj.ImdbURL = imdbUrl
			}
			if tmdbId, ok := links["tmdbId"].(string); ok {
				linkObj.TmdbID = tmdbId
			}
			if tmdbUrl, ok := links["tmdbUrl"].(string); ok {
				linkObj.TmdbURL = tmdbUrl
			}

			movie.Links = linkObj
		}

		// 添加标签数据
		if uniqueTags, ok := movieData["uniqueTags"].([]string); ok {
			movie.Tags = uniqueTags
		}

		movies = append(movies, movie)
	}

	// 构建响应
	totalMovies := 9742                                 // 从数据库结构文档中获取的总电影数
	totalPages := (totalMovies + perPage - 1) / perPage // 计算总页数

	return &MovieList{
		Movies:      movies,
		TotalMovies: totalMovies,
		Page:        page,
		PerPage:     perPage,
		TotalPages:  totalPages,
	}, nil
}

// GetRandomMovies 获取随机电影（带缓存）
func GetRandomMovies(count int) ([]Movie, error) {
	ctx := context.Background()
	totalMovies := 9742 // 总电影数

	// 构建缓存键 - 这里我们不直接缓存结果，而是缓存seed，确保一段时间内返回相同的"随机"电影
	// 使用当前时间的小时数作为缓存键，这样每小时刷新一次随机结果
	currentHour := time.Now().Hour()
	cacheKey := fmt.Sprintf("random_movies:%d:%d", count, currentHour)

	// 检查缓存中是否有随机电影数据
	if cachedMovies, found := utils.Cache.Get(cacheKey); found {
		return cachedMovies.([]Movie), nil
	}

	// 生成随机ID列表
	randomIDs := generateRandomIDs(totalMovies, count)
	movies := []Movie{}

	// 获取每个随机ID的电影信息
	for _, id := range randomIDs {
		movieID := fmt.Sprintf("%d", id)
		data, err := utils.GetMovie(ctx, movieID)
		if err != nil {
			continue
		}

		if data == nil {
			continue
		}

		movieData := utils.ParseMovieData(movieID, data)

		movie := Movie{
			MovieID: movieID,
		}

		if title, ok := movieData["title"].(string); ok {
			movie.Title = title
			// 尝试从标题中提取年份
			if matches := strings.Split(title, " ("); len(matches) > 1 {
				yearStr := strings.TrimSuffix(matches[len(matches)-1], ")")
				if year, err := strconv.Atoi(yearStr); err == nil {
					movie.Year = year
				}
			}
		}

		if genres, ok := movieData["genres"].([]string); ok {
			movie.Genres = genres
		}

		if avgRating, ok := movieData["avgRating"].(float64); ok {
			movie.AvgRating = avgRating
		}

		// 添加标签
		if tags, ok := movieData["uniqueTags"].([]string); ok {
			movie.Tags = tags
		}

		movies = append(movies, movie)
	}

	// 将结果存入缓存
	utils.Cache.Set(cacheKey, movies)

	return movies, nil
}

// SearchMovies 搜索电影（带缓存）
func SearchMovies(query string, page, perPage int) (*MovieList, error) {
	// 构建缓存键
	cacheKey := fmt.Sprintf("search:%s:%d:%d", query, page, perPage)

	// 检查缓存
	if cachedResults, found := utils.Cache.Get(cacheKey); found {
		return cachedResults.(*MovieList), nil
	}

	ctx := context.Background()

	// 创建扫描 - 使用movies表
	scan, err := hrpc.NewScanStr(ctx, "movies",
		hrpc.Families(map[string][]string{"info": {"title", "genres"}}))
	if err != nil {
		return nil, err
	}

	scanner := utils.GetClient().Scan(scan)
	matchedMovies := []Movie{}

	// 将查询转为小写以进行不区分大小写的匹配
	queryLower := strings.ToLower(query)

	for {
		res, err := scanner.Next()
		if err != nil {
			break // 到达结尾
		}

		if len(res.Cells) == 0 {
			continue
		}

		// 获取行键（即movieId）
		movieID := string(res.Cells[0].Row)
		var title, genres string

		// 从单元格提取数据
		for _, cell := range res.Cells {
			family := string(cell.Family)
			qualifier := string(cell.Qualifier)

			if family == "info" {
				switch qualifier {
				case "title":
					title = string(cell.Value)
				case "genres":
					genres = string(cell.Value)
				}
			}
		}

		// 检查标题是否匹配
		if title != "" && strings.Contains(strings.ToLower(title), queryLower) {
			// 获取完整的电影信息
			movieData, err := utils.GetMovie(ctx, movieID)
			if err != nil {
				continue
			}

			parsedData := utils.ParseMovieData(movieID, movieData)

			movie := Movie{
				MovieID: movieID,
				Title:   title,
			}

			// 尝试从标题中提取年份
			if matches := strings.Split(title, " ("); len(matches) > 1 {
				yearStr := strings.TrimSuffix(matches[len(matches)-1], ")")
				if year, err := strconv.Atoi(yearStr); err == nil {
					movie.Year = year
				}
			}

			if genresArr, ok := parsedData["genres"].([]string); ok {
				movie.Genres = genresArr
			} else if genres != "" {
				movie.Genres = strings.Split(genres, "|")
			}

			if avgRating, ok := parsedData["avgRating"].(float64); ok {
				movie.AvgRating = avgRating
			}

			// 添加链接
			if links, ok := parsedData["links"].(map[string]interface{}); ok {
				movie.Links = Links{
					ImdbID:  links["imdbId"].(string),
					ImdbURL: links["imdbUrl"].(string),
					TmdbID:  links["tmdbId"].(string),
					TmdbURL: links["tmdbUrl"].(string),
				}
			}

			matchedMovies = append(matchedMovies, movie)
			continue
		}

		// 检查类型是否匹配
		if genres != "" {
			genresArr := strings.Split(genres, "|")
			for _, genre := range genresArr {
				if strings.Contains(strings.ToLower(genre), queryLower) {
					// 获取完整的电影信息
					movieData, err := utils.GetMovie(ctx, movieID)
					if err != nil {
						continue
					}

					parsedData := utils.ParseMovieData(movieID, movieData)

					movie := Movie{
						MovieID: movieID,
						Title:   title,
						Genres:  genresArr,
					}

					// 尝试从标题中提取年份
					if matches := strings.Split(title, " ("); len(matches) > 1 {
						yearStr := strings.TrimSuffix(matches[len(matches)-1], ")")
						if year, err := strconv.Atoi(yearStr); err == nil {
							movie.Year = year
						}
					}

					if avgRating, ok := parsedData["avgRating"].(float64); ok {
						movie.AvgRating = avgRating
					}

					// 添加链接
					if links, ok := parsedData["links"].(map[string]interface{}); ok {
						movie.Links = Links{
							ImdbID:  links["imdbId"].(string),
							ImdbURL: links["imdbUrl"].(string),
							TmdbID:  links["tmdbId"].(string),
							TmdbURL: links["tmdbUrl"].(string),
						}
					}

					matchedMovies = append(matchedMovies, movie)
					break
				}
			}
		}
	}

	// 计算分页
	totalMatches := len(matchedMovies)
	totalPages := (totalMatches + perPage - 1) / perPage

	startIdx := (page - 1) * perPage
	endIdx := startIdx + perPage
	if endIdx > totalMatches {
		endIdx = totalMatches
	}

	// 如果没有匹配项
	if startIdx >= totalMatches {
		result := &MovieList{
			Movies:      []Movie{},
			TotalMovies: totalMatches,
			Page:        page,
			PerPage:     perPage,
			TotalPages:  totalPages,
		}

		// 缓存搜索结果
		utils.Cache.Set(cacheKey, result)

		return result, nil
	}

	// 构建结果
	result := &MovieList{
		Movies:      matchedMovies[startIdx:endIdx],
		TotalMovies: totalMatches,
		Page:        page,
		PerPage:     perPage,
		TotalPages:  totalPages,
	}

	// 缓存搜索结果
	utils.Cache.Set(cacheKey, result)

	return result, nil
}

// generateRandomIDs 生成不重复的随机ID列表
func generateRandomIDs(max, count int) []int {
	if count > max {
		count = max
	}

	// 使用map确保唯一性
	idMap := make(map[int]bool)
	for len(idMap) < count {
		id := rng.Intn(max) + 1 // 从1开始
		idMap[id] = true
	}

	// 转换为切片
	ids := make([]int, 0, len(idMap))
	for id := range idMap {
		ids = append(ids, id)
	}

	return ids
}
