 'movies': {
        'filename': 'movies.csv',
        'table_name': 'movies',
        'column_family': 'info',
        'row_key_column': 'movieId',
        'columns': ['title', 'genres']
    },
    'ratings': {
        'filename': 'ratings.csv', 
        'table_name': 'ratings',
        'column_family': 'data',
        'row_key_column': 'userId_movieId',  # 修复：使用复合行键避免覆盖
        'row_key_components': ['userId', 'movieId'],  # 行键组成部分
        'columns': ['rating', 'timestamp'],  # 移除movieId，因为已在行键中
        'large_file': True,  # 标记为大文件，支持切割
        'default_chunks': 20  # 默认切割为10份
    }, #双表存储 牺牲储存 增加复杂度 换取性能，因为一个表只能查一个行键，复合行键只能做辅助作用，方便进行查询xx用户的所有评分，xx电影的所有评分，避免全表查询