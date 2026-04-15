# 🎵 QQ Music Distributed Worker (分机版)

![Version](https://img.shields.io/badge/Edition-Professional-10b981)
![Sync](https://img.shields.io/badge/Sync-Real--time-success)
![Support](https://img.shields.io/badge/技术支持-王润年-blue)

本项目是专门用于分布式采集 QQ 音乐的分机程序，支持高并发、零延迟的数据同步与实时监控。

---

## 📂 项目结构概览

### 1. 核心开发环境 (`/dist_worker`)
- **`worker_gui.py`**: 基于 Flet 的“清新绿”皮肤分机界面源码。
- **`worker_from_api.py`**: 分级采集核心逻辑，负责与 Server 通信、下载、Cookie 刷新等。
- **`worker_config.json`**: 本地开发/运行使用的配置文件。
- **`icon.png`**: 分机 PRO 版的高级感绿色图标。
- **`BUILD.bat`**: 一键打包脚本（打包结果输出至 `/dist`）。

### 2. 部署分发包 (`/dist/DEPLOY_PRO`) —— **部署时仅拷贝此文件夹**
- **`QQMusicWorkerPRO.exe`**: 编译后的单文件可执行程序，双击即用。
- **`worker_config.json`**: 部署专用的配置文件。
- **`一键启动分机.bat`**: 持久化运行脚本。若程序意外退出，它将每隔 5 秒自动重启，实现 24 小时无人值守。

---

## 🚀 快速部署指南

1.  **拷贝**: 将整个 `DEPLOY_PRO` 文件夹复制到分机电脑。
2.  **配置**: 
    - 编辑 `worker_config.json`。
    - `worker_id`: 设置为该机器的唯一 ID（如: `Node-001`）。
    - `api_url`: 填写主控端（Server）的 IP 及端口。
3.  **运行**: 双击运行 `一键启动分机.bat`。

---

## 🛠️ 环境准备 (源码运行)

若不使用打包后的 `.exe`，而是通过 Python 直接启动：

1.  **安装依赖**:
    ```bash
    pip install -r requirements.txt -i https://pypi.tuna.tsinghua.edu.cn/simple
    ```
2.  **安装 Playwright (可选，用于自动刷新 Cookie)**:
    ```bash
    playwright install msedge
    ```
3.  **启动程序**:
    ```bash
    python worker_gui.py
    ```

---

## ⚙️ 配置文件说明 (`worker_config.json`)

- **零延迟渲染**: 采用“数据回调”+“UI 帧同步”架构，彻底解决高负载下的显示积压问题。
- **动态 Cookie 刷新**: 支持在分机本地自动维护登录状态，降低封禁风险。
- **清新绿皮肤**: 专业暗黑森林绿主题，减轻大规模集群管理时的视觉疲劳。

---

## 🛡️ 技术支持

本程序及部署方案由 **王润年** 提供技术支持。

> © 2026 Spider Cluster Discovery System.
