# 电影评分系统后端

这是一个基于 Golang 实现的，连接 HBase 数据库的电影评分系统后端。它提供 RESTful API 接口，用于前端获取电影数据。

## 系统要求

- Go 1.16+

## 项目结构

```
backend/
├── config/       # 配置文件
├── controllers/  # 控制器
├── models/       # 数据模型
├── routes/       # 路由定义
├── utils/        # 工具类
├── main.go       # 程序入口
└── README.md     # 说明文档
```

## API 接口

### 电影相关接口

- `GET /api/movies` - 获取电影列表
- `GET /api/movies/{id}` - 获取电影详情
- `GET /api/movies/random` - 获取随机电影
- `POST /api/movies/random` - 获取随机电影（POST方法）
- `GET /api/movies/search` - 搜索电影

### 查询参数

- `page` - 页码，默认为 1
- `per_page` - 每页数量，默认为 12
- `query` - 搜索关键词
- `count` - 随机电影数量

## 开发说明

- 使用 [gin](https://github.com/gin-gonic/gin) 作为 Web 框架
- 使用 [gohbase](https://github.com/tsuna/gohbase) 作为 HBase 客户端 