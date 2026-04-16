import flet as ft
import json
import os
import socket
import threading
import time
import logging
from pathlib import Path
from queue import Queue

# Import worker logic
import sys
_project_dir = os.path.dirname(os.path.abspath(__file__))
if _project_dir not in sys.path:
    sys.path.insert(0, _project_dir)

try:
    from worker_from_api import (
        feeder_loop, worker_loop, ResultCollector, CookieHolder,
        set_comment_auth, set_rate_limit, _init_comment_rate_limit, log as worker_log
    )
    import worker_from_api
    from QQMusicSpider.download import set_vkey_rate_limit
except ImportError:
    pass

class WorkerGUI:
    def __init__(self, page: ft.Page):
        self.page = page
        self.config_path = Path(_project_dir) / "worker_config.json"
        self.stop_event = threading.Event()
        self.is_running = False
        self.log_handler = None
        self.logic_thread = None
        
        # UI渲染同步变量
        self.data_done = 0
        self.data_failed = 0
        self.data_rate = 0.0
        self.pending_logs = []
        self._ui_lock = threading.Lock()
        
        self.setup_ui()
        self.load_config()
        
    def setup_ui(self):
        self.page.title = "QQ音乐采集分机 - 清新绿专业版"
        self.page.window.width = 900
        self.page.window.height = 850
        self.page.window.prevent_close = True
        self.page.window.on_event = self.on_window_event
        self.page.theme_mode = ft.ThemeMode.DARK
        self.page.padding = 0
        self.page.bgcolor = "#061a14" 
        
        accent_color = "#10b981"
        primary_light = "#34d399"
        danger_color = "#f43f5e"
        glass_bg = ft.Colors.with_opacity(0.08, ft.Colors.WHITE)
        glass_border = ft.Colors.with_opacity(0.15, ft.Colors.WHITE)
        
        self.card_style = {"bgcolor": glass_bg, "border": ft.Border.all(1, glass_border), "border_radius": 24, "padding": 24, "blur": 20}
        
        header = ft.Container(
            content=ft.Row([
                ft.Column([
                    ft.Text("QQ Music Distributed Discovery", size=12, color=primary_light, weight="bold", style=ft.TextStyle(letter_spacing=1.5)),
                    ft.Text("分布式分机 - 清新绿版", size=32, weight=ft.FontWeight.W_900, color="white"),
                ], spacing=0),
                ft.Icon(ft.Icons.AUTO_AWESOME, size=48, color=accent_color),
            ], alignment=ft.MainAxisAlignment.SPACE_BETWEEN),
            padding=ft.Padding.only(left=40, right=40, top=40, bottom=20),
            gradient=ft.LinearGradient(begin=ft.Alignment(0, -1), end=ft.Alignment(0, 1), colors=[ft.Colors.with_opacity(0.12, accent_color), ft.Colors.TRANSPARENT])
        )
        
        self.stats_done = ft.Text("0", size=38, weight="bold", color=primary_light)
        self.stats_failed = ft.Text("0", size=38, weight="bold", color="#94a3b8")
        self.stats_speed = ft.Text("0.0/分", size=18, color=accent_color, weight="bold")
        
        def create_stat_card(label, icon, control, color):
            return ft.Container(content=ft.Column([ft.Row([ft.Icon(icon, size=18, color=color), ft.Text(label, size=13, color="#a7f3d0")]), control], spacing=8, horizontal_alignment=ft.CrossAxisAlignment.CENTER), **self.card_style, expand=True)

        stats_row = ft.Row([
            create_stat_card("已成功采集", ft.Icons.AUTO_GRAPH, self.stats_done, accent_color),
            create_stat_card("异常统计", ft.Icons.GPP_BAD, self.stats_failed, danger_color),
            create_stat_card("实时速率", ft.Icons.BOLT, self.stats_speed, primary_light),
        ], spacing=15)

        self.worker_id_input = self.styled_input("机器标识", socket.gethostname(), ft.Icons.TERMINAL)
        self.api_url_input = self.styled_input("主控服务器地址", "http://192.168.10.165:8080", ft.Icons.LAN)
        self.cookie_input = self.styled_input("Cookie 凭证", "", ft.Icons.VPN_KEY_OUTLINED, multiline=True)
        self.uin_input = self.styled_input("用户 UIN", "", ft.Icons.ACCOUNT_CIRCLE)
        self.output_dir_input = self.styled_input("歌曲落盘路径", "Y:/QQ音乐", ft.Icons.DRIVE_FILE_RENAME_OUTLINE)
        self.threads_input = ft.Slider(min=1, max=100, divisions=100, label="线程: {value}", value=5, active_color=accent_color, thumb_color=primary_light)
        
        settings_col = ft.Container(content=ft.Column([ft.Text("系统配置参数", size=14, weight="bold", color="#34d399"), ft.Row([self.worker_id_input, self.api_url_input], spacing=15), self.cookie_input, ft.Row([self.uin_input, self.output_dir_input], spacing=15), ft.Row([ft.Text("并行核心线程数", size=14, color="#a7f3d0"), ft.Container(self.threads_input, expand=True)], spacing=20)], spacing=18), **self.card_style)
        self.log_view = ft.ListView(expand=True, spacing=6, auto_scroll=True, padding=12)
        log_container = ft.Container(content=ft.Column([ft.Row([ft.Text("实时工作日志", size=14, weight="bold", color="#34d399"), ft.Container(bgcolor=accent_color, width=8, height=8, border_radius=4)], alignment=ft.MainAxisAlignment.SPACE_BETWEEN), self.log_view]), **self.card_style, height=240)
        
        self.status_dot = ft.Container(width=10, height=10, border_radius=5, bgcolor="#334155")
        self.status_text = ft.Text("等待任务调度", color="#94a3b8", size=14)
        self.start_btn = ft.Container(content=ft.Text("立即开启采集", weight="bold", color="#061a14", size=16), bgcolor=accent_color, padding=ft.Padding.symmetric(horizontal=50, vertical=16), border_radius=32, on_click=self.toggle_worker)
        
        footer = ft.Container(content=ft.Row([ft.Row([self.status_dot, self.status_text], spacing=10), ft.Column([ft.Text("技术支持：王润年", size=13, color=primary_light, weight="bold")], horizontal_alignment=ft.CrossAxisAlignment.END)], alignment=ft.MainAxisAlignment.SPACE_BETWEEN), padding=ft.Padding.symmetric(horizontal=40, vertical=20))
        
        self.page.add(ft.Column([header, ft.Container(content=ft.Column([stats_row, settings_col, log_container], spacing=20), padding=ft.Padding.symmetric(horizontal=40)), ft.Container(expand=True), ft.Row([self.start_btn], alignment=ft.MainAxisAlignment.CENTER), footer], expand=True, spacing=0))

    def styled_input(self, label, value, icon, multiline=False):
        return ft.TextField(label=label, value=value, prefix_icon=icon, multiline=multiline, min_lines=3 if multiline else 1, max_lines=5 if multiline else 1, text_size=14, border_color="#064e3b", focused_border_color=ft.Colors.GREEN, bgcolor="#052e16", border_radius=14, expand=not multiline, color="#ecfdf5")

    def load_config(self):
        if self.config_path.exists():
            try:
                with open(self.config_path, "r", encoding="utf-8") as f:
                    cfg = json.load(f); self.worker_id_input.value = cfg.get("worker_id", socket.gethostname()); self.api_url_input.value = cfg.get("api_url", "http://192.168.10.165:8080"); self.cookie_input.value = cfg.get("qqmusic_cookie", ""); self.uin_input.value = cfg.get("qqmusic_uin", ""); self.output_dir_input.value = cfg.get("output_dir", ""); self.threads_input.value = cfg.get("threads", 5); self.page.update()
            except: pass

    def save_config(self):
        cfg = {"worker_id": self.worker_id_input.value, "api_url": self.api_url_input.value, "qqmusic_cookie": self.cookie_input.value, "qqmusic_uin": self.uin_input.value, "output_dir": self.output_dir_input.value, "threads": int(self.threads_input.value), "download_quality": "flac", "batch_size": 10, "api_qps": 100}
        with open(self.config_path, "w", encoding="utf-8") as f: json.dump(cfg, f, indent=4, ensure_ascii=False)

    def ui_sync_loop(self):
        """高帧率UI同步时钟，解决高并发下的渲染堆积问题"""
        while self.is_running or self.pending_logs or self.data_done > int(self.stats_done.value or 0):
            with self._ui_lock:
                done, failed, rate = self.data_done, self.data_failed, self.data_rate
                new_logs = self.pending_logs[:]; self.pending_logs.clear()
            
            needs_update = False
            if str(done) != self.stats_done.value: self.stats_done.value = str(done); needs_update = True
            if str(failed) != self.stats_failed.value: self.stats_failed.value = str(failed); needs_update = True
            rate_str = f"{rate:.1f}/分"
            if rate_str != self.stats_speed.value: self.stats_speed.value = rate_str; needs_update = True
            
            if new_logs:
                for msg, color in new_logs:
                    if len(self.log_view.controls) > 100: self.log_view.controls.pop(0)
                    self.log_view.controls.append(ft.Text(msg, color=color, size=11, font_family="monospace"))
                needs_update = True
            
            if needs_update: self.page.update()
            time.sleep(0.1) # 10FPS 保证极其丝滑且不占CPU

    def add_log_async(self, message, level="info"):
        colors = {"info": "#a7f3d0", "success": "#34d399", "error": "#f43f5e", "warning": "#fbbf24"}
        t = time.strftime("%H:%M:%S")
        with self._ui_lock:
            self.pending_logs.append((f"[{t}] {level.upper()} » {message}", colors.get(level, "white")))

    def on_stats_update_async(self, done, failed, rate):
        with self._ui_lock:
            self.data_done, self.data_failed, self.data_rate = done, failed, rate

    def on_window_event(self, e):
        if e.data == "close":
            if getattr(self, "_closing", False):
                return
            self._closing = True
            
            if self.is_running:
                self.add_log_async("正在同步数据并安全退出，请勿强制关闭...", "warning")
                self.stop_worker()
                self.status_text.value = "正在同步数据并安全退出，请勿强制关闭..."
                self.page.update()
                
                def wait_and_exit():
                    if self.logic_thread and self.logic_thread.is_alive():
                        self.logic_thread.join()
                    self.page.window.destroy()
                threading.Thread(target=wait_and_exit, daemon=True).start()
            else:
                self.page.window.destroy()

    def toggle_worker(self, e):
        if self.is_running: self.stop_worker()
        else: self.start_worker()

    def start_worker(self):
        self.save_config(); self.is_running = True; self.stop_event.clear()
        self.start_btn.content.value = "停止采集分机"; self.start_btn.bgcolor = "#f43f5e"; self.start_btn.content.color = "white"
        self.status_dot.bgcolor = "#10b981"; self.status_text.value = "运行正常 - 帧同步开启"; self.status_text.color = "#10b981"
        if self.log_handler: worker_log.removeHandler(self.log_handler)
        self.log_handler = LoggingInterceptor(self.add_log_async); worker_log.addHandler(self.log_handler)
        threading.Thread(target=self.ui_sync_loop, daemon=True).start() # 启动UI时钟
        self.logic_thread = threading.Thread(target=self.run_worker_logic, daemon=True)
        self.logic_thread.start()
        self.page.update()

    def stop_worker(self):
        self.is_running = False; self.stop_event.set()
        self.start_btn.content.value = "正在下线..."; self.start_btn.bgcolor = "#475569"
        self.status_text.value = "分机正在退出并保存..."; self.page.update()

    def run_worker_logic(self):
        class Args: pass
        args = Args(); args.worker_id = self.worker_id_input.value; args.api_url = self.api_url_input.value
        args.qqmusic_cookie = self.cookie_input.value; args.qqmusic_uin = self.uin_input.value
        args.output_dir = self.output_dir_input.value; args.threads = int(self.threads_input.value)
        args.batch_size, args.lease_seconds, args.idle_seconds, args.timeout = 10, 3600, 10, 20
        args.api_qps, args.metadata_only, args.download_quality, args.retry_failed = 100, False, "flac", False
        args.flush_interval, args.max_tasks, args.skip_playwright = 1.0, 0, True
        args.comment_fallback_profile_dir, args.comment_fallback_browser_channel = ".playwright_profile", "msedge"
        args.comment_fallback_headful, args.comment_fallback_wait_seconds = False, 8.0
        
        set_rate_limit(args.api_qps); set_comment_auth(args.qqmusic_cookie, args.qqmusic_uin); set_vkey_rate_limit(100); _init_comment_rate_limit(60)
        cookie_holder = CookieHolder(args.qqmusic_cookie, args.qqmusic_uin, args)
        collector = ResultCollector(args.api_url, retry_failed=args.retry_failed, flush_interval=args.flush_interval)
        collector.set_on_update(self.on_stats_update_async); collector.start()
        local_queue = Queue(maxsize=args.batch_size * 2); state = {"lock": threading.Lock(), "claimed": 0}
        
        threading.Thread(target=feeder_loop, args=(args, local_queue, self.stop_event), name="feeder", daemon=True).start()
        workers = []
        for i in range(args.threads):
            w = threading.Thread(target=worker_loop, args=(i, args, local_queue, collector, cookie_holder, state, self.stop_event), daemon=True)
            w.start(); workers.append(w)
        for w in workers: w.join()
        
        self.add_log_async("所有采集任务已同步卸载。", "warning")
        self.start_btn.content.value = "立即开启采集"; self.start_btn.bgcolor = "#10b981"; self.start_btn.content.color = "#061a14"
        self.status_dot.bgcolor = "#334155"; self.status_text.value = "等待任务调度"; collector.stop(); self.page.update()

class LoggingInterceptor(logging.Handler):
    def __init__(self, callback): super().__init__(); self.callback = callback
    def emit(self, record):
        msg = self.format(record); level = "info"
        if record.levelno >= logging.ERROR: level = "error"
        elif record.levelno >= logging.WARNING: level = "warning"
        self.callback(msg, level)

def main(page: ft.Page): WorkerGUI(page)
if __name__ == "__main__": ft.app(target=main)
