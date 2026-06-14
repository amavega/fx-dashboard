# FX Dashboard — GitHub Pages 汇率看板

> 零成本、零运维的 USD/CNY 汇率自动同步 + 可视化看板

## 功能

- **外管局中间价** — 从 SAFE 官网每日自动采集
- **中信银行结汇价 OHLC** — 从中信银行 API 自动采集开盘/收盘/最高/最低
- **企业微信智能表格** — 数据自动写入（可选）
- **交互式 HTML 图表** — GitHub Pages 托管，随时随地访问

## 如何使用

### 1. Fork / 创建仓库

点击 GitHub 右上角 **+** → **New repository** → 命名为 `fx-dashboard`（或其他名字）→ **Create**。

然后把 `gh_deploy/` 目录的内容推到仓库：

```bash
# 克隆你的仓库
git clone https://github.com/amavega/fx-dashboard.git
cd fx-dashboard

# 复制所有文件（Windows 下可以直接拖拽）
# gh_deploy/ 目录下所有文件复制到仓库根目录：
#   sync_all.py
#   data/
#   docs/
#   .github/
#   README.md

git add .
git commit -m "初始化汇率看板"
git push
```

### 2. 可选：配置 Secrets

如果你不想把 Webhook URL 公开，在仓库 **Settings → Secrets and variables → Actions** 添加：

| Secret | 值 |
|---|---|
| `SAFE_WEBHOOK` | 外管局智能表格 Webhook URL |
| `CITIC_WEBHOOK` | 中信银行智能表格 Webhook URL |

> 不配置也能正常采集数据和生成图表，只是不写企微表格。

### 3. 开启 GitHub Pages

仓库 **Settings → Pages**：

- **Source**: `Deploy from a branch`
- **Branch**: `main` / 文件夹选择 `/docs`
- 点击 **Save**

等待 1-2 分钟，访问：
```
https://amavega.github.io/fx-dashboard/
```

### 4. 手动触发首次同步

仓库 **Actions** → **汇率数据每日同步** → **Run workflow**

首次同步会补齐 2025 年至今的全部数据（约 5 分钟）。

之后每天北京时间 11:00 自动运行，只需几秒钟。

## 目录结构

```
fx-dashboard/
├── .github/workflows/daily_sync.yml   # GitHub Actions 定时工作流
├── sync_all.py                         # 统一同步脚本（纯标准库）
├── data/                               # 种子数据 + 自动更新
│   ├── fx_data.json                    # SAFE 中间价数据
│   └── citic_fx_data.json              # CITIC OHLC 数据
├── docs/                               # GitHub Pages 站点
│   ├── index.html                      # 导航首页
│   ├── fx_chart.html                   # SAFE 图表（自动生成）
│   └── citic_fx_chart.html             # CITIC 图表（自动生成）
└── README.md
```

## 完全免费

- GitHub Actions: 2000 分钟/月（每日同步仅用 ~60 分钟/月）
- GitHub Pages: 无限流量 + 1GB 存储
- 无需服务器、无需信用卡、从不欠费

## License

MIT
