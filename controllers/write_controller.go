package controllers

import (
	"gohbase/utils"
	"net/http"

	"github.com/gin-gonic/gin"
)

// WriteController 写入控制器
type WriteController struct{}

// StartRandomWrites 开始随机写入操作
func (wc *WriteController) StartRandomWrites(c *gin.Context) {
	utils.WriteManagerInstance.StartRandomWrites()
	c.JSON(http.StatusOK, gin.H{
		"status":  "success",
		"message": "随机写入服务已启动",
	})
}

// StopRandomWrites 停止随机写入操作
func (wc *WriteController) StopRandomWrites(c *gin.Context) {
	utils.WriteManagerInstance.StopRandomWrites()
	c.JSON(http.StatusOK, gin.H{
		"status":  "success",
		"message": "随机写入服务已停止",
	})
}

// GetWriteStatus 获取写入服务状态
func (wc *WriteController) GetWriteStatus(c *gin.Context) {
	isRunning := utils.WriteManagerInstance.IsRunning()
	logs := utils.WriteManagerInstance.GetLogs()

	c.JSON(http.StatusOK, gin.H{
		"status":  "success",
		"running": isRunning,
		"logs":    logs,
	})
}

// GetHotspots 获取热点电影ID
func (wc *WriteController) GetHotspots(c *gin.Context) {
	// 获取最多被写入的10个电影ID
	hotspots := utils.WriteManagerInstance.GetHotspots(10)

	c.JSON(http.StatusOK, gin.H{
		"status":   "success",
		"hotspots": hotspots,
	})
}

// GetWritePanel 获取写入面板HTML
func (wc *WriteController) GetWritePanel(c *gin.Context) {
	html := `
<!DOCTYPE html>
<html lang="zh-CN">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>电影评分随机写入面板</title>
    <style>
        body {
            font-family: Arial, sans-serif;
            margin: 0;
            padding: 20px;
            background-color: #f5f5f5;
        }
        .container {
            max-width: 1200px;
            margin: 0 auto;
            background-color: white;
            border-radius: 8px;
            box-shadow: 0 2px 10px rgba(0,0,0,0.1);
            padding: 20px;
        }
        .header {
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-bottom: 20px;
            padding-bottom: 15px;
            border-bottom: 1px solid #eee;
        }
        h1 {
            margin: 0;
            color: #333;
        }
        .control-panel {
            display: flex;
            gap: 10px;
        }
        button {
            padding: 8px 16px;
            border: none;
            border-radius: 4px;
            cursor: pointer;
            font-weight: bold;
            transition: background-color 0.2s;
        }
        .start-btn {
            background-color: #4caf50;
            color: white;
        }
        .stop-btn {
            background-color: #f44336;
            color: white;
        }
        .refresh-btn {
            background-color: #2196f3;
            color: white;
        }
        button:hover {
            opacity: 0.9;
        }
        .dashboard {
            display: flex;
            gap: 20px;
            margin-top: 20px;
        }
        .panel {
            flex: 1;
            background-color: white;
            border-radius: 8px;
            box-shadow: 0 1px 5px rgba(0,0,0,0.1);
            padding: 15px;
        }
        .panel-header {
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-bottom: 15px;
            padding-bottom: 10px;
            border-bottom: 1px solid #eee;
        }
        .panel-title {
            margin: 0;
            color: #444;
            font-size: 18px;
        }
        .status-badge {
            padding: 5px 10px;
            border-radius: 20px;
            font-size: 14px;
            font-weight: bold;
        }
        .status-running {
            background-color: #e8f5e9;
            color: #4caf50;
        }
        .status-stopped {
            background-color: #ffebee;
            color: #f44336;
        }
        table {
            width: 100%;
            border-collapse: collapse;
            margin-top: 10px;
        }
        th, td {
            padding: 10px;
            text-align: left;
            border-bottom: 1px solid #eee;
        }
        th {
            font-weight: bold;
            color: #555;
            background-color: #f9f9f9;
        }
        tr:hover {
            background-color: #f5f5f5;
        }
        .log-status {
            padding: 3px 6px;
            border-radius: 4px;
            font-size: 12px;
        }
        .status-pending {
            background-color: #fff9c4;
            color: #fbc02d;
        }
        .status-success {
            background-color: #e8f5e9;
            color: #4caf50;
        }
        .status-failed {
            background-color: #ffebee;
            color: #f44336;
        }
        .panel-scroll {
            max-height: 500px;
            overflow-y: auto;
        }
        .no-data {
            text-align: center;
            padding: 20px;
            color: #777;
        }
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>电影评分随机写入面板</h1>
            <div class="control-panel">
                <button id="startBtn" class="start-btn">开始写入</button>
                <button id="stopBtn" class="stop-btn">停止写入</button>
                <button id="refreshBtn" class="refresh-btn">刷新数据</button>
            </div>
        </div>
        
        <div class="dashboard">
            <div class="panel">
                <div class="panel-header">
                    <h2 class="panel-title">写入日志</h2>
                    <div id="statusBadge" class="status-badge status-stopped">已停止</div>
                </div>
                <div class="panel-scroll">
                    <table id="logsTable">
                        <thead>
                            <tr>
                                <th>时间</th>
                                <th>电影ID</th>
                                <th>用户ID</th>
                                <th>评分</th>
                                <th>状态</th>
                            </tr>
                        </thead>
                        <tbody id="logsBody">
                            <tr>
                                <td colspan="5" class="no-data">暂无数据</td>
                            </tr>
                        </tbody>
                    </table>
                </div>
            </div>
            
            <div class="panel">
                <div class="panel-header">
                    <h2 class="panel-title">热点电影</h2>
                    <div>最近10分钟写入最多的电影</div>
                </div>
                <div class="panel-scroll">
                    <table id="hotspotsTable">
                        <thead>
                            <tr>
                                <th>电影ID</th>
                                <th>写入次数</th>
                            </tr>
                        </thead>
                        <tbody id="hotspotsBody">
                            <tr>
                                <td colspan="2" class="no-data">暂无数据</td>
                            </tr>
                        </tbody>
                    </table>
                </div>
            </div>
        </div>
    </div>

    <script>
        // 定义API端点
        const API_ENDPOINTS = {
            start: '/api/write/start',
            stop: '/api/write/stop',
            status: '/api/write/status',
            hotspots: '/api/write/hotspots'
        };

        // DOM元素
        const startBtn = document.getElementById('startBtn');
        const stopBtn = document.getElementById('stopBtn');
        const refreshBtn = document.getElementById('refreshBtn');
        const statusBadge = document.getElementById('statusBadge');
        const logsBody = document.getElementById('logsBody');
        const hotspotsBody = document.getElementById('hotspotsBody');

        // 更新状态和日志
        async function updateStatus() {
            try {
                const response = await fetch(API_ENDPOINTS.status);
                const data = await response.json();
                
                // 更新运行状态
                if (data.running) {
                    statusBadge.textContent = '运行中';
                    statusBadge.className = 'status-badge status-running';
                } else {
                    statusBadge.textContent = '已停止';
                    statusBadge.className = 'status-badge status-stopped';
                }
                
                // 更新日志表格
                if (data.logs && data.logs.length > 0) {
                    logsBody.innerHTML = '';
                    data.logs.forEach(log => {
                        const statusClass = 
                            log.status === 'success' ? 'status-success' : 
                            log.status === 'failed' ? 'status-failed' : 'status-pending';
                        
                        logsBody.innerHTML += '<tr>' +
                            '<td>' + log.timestamp + '</td>' +
                            '<td>' + log.movieId + '</td>' +
                            '<td>' + log.userId + '</td>' +
                            '<td>' + log.rating + '</td>' +
                            '<td><span class="log-status ' + statusClass + '">' + log.status + '</span></td>' +
                            '</tr>';
                    });
                } else {
                    logsBody.innerHTML = '<tr><td colspan="5" class="no-data">暂无数据</td></tr>';
                }
            } catch (error) {
                console.error('获取状态失败:', error);
            }
        }

        // 更新热点数据
        async function updateHotspots() {
            try {
                const response = await fetch(API_ENDPOINTS.hotspots);
                const data = await response.json();
                
                // 更新热点表格
                if (data.hotspots && data.hotspots.length > 0) {
                    hotspotsBody.innerHTML = '';
                    data.hotspots.forEach(hotspot => {
                        hotspotsBody.innerHTML += '<tr>' +
                            '<td>' + hotspot.movieId + '</td>' +
                            '<td>' + hotspot.count + '</td>' +
                            '</tr>';
                    });
                } else {
                    hotspotsBody.innerHTML = '<tr><td colspan="2" class="no-data">暂无数据</td></tr>';
                }
            } catch (error) {
                console.error('获取热点数据失败:', error);
            }
        }

        // 刷新所有数据
        function refreshData() {
            updateStatus();
            updateHotspots();
        }

        // 开始随机写入
        async function startRandomWrites() {
            try {
                const response = await fetch(API_ENDPOINTS.start, { method: 'POST' });
                const data = await response.json();
                console.log('写入服务启动:', data);
                refreshData();
            } catch (error) {
                console.error('启动服务失败:', error);
            }
        }

        // 停止随机写入
        async function stopRandomWrites() {
            try {
                const response = await fetch(API_ENDPOINTS.stop, { method: 'POST' });
                const data = await response.json();
                console.log('写入服务停止:', data);
                refreshData();
            } catch (error) {
                console.error('停止服务失败:', error);
            }
        }

        // 绑定事件
        startBtn.addEventListener('click', startRandomWrites);
        stopBtn.addEventListener('click', stopRandomWrites);
        refreshBtn.addEventListener('click', refreshData);

        // 页面加载时获取初始数据
        refreshData();
        
        // 设置自动刷新 (每5秒)
        setInterval(refreshData, 5000);
    </script>
</body>
</html>
`
	c.Header("Content-Type", "text/html; charset=utf-8")
	c.String(http.StatusOK, html)
}
