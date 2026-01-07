"""
标注数据自动化处理流水线
========================

完整流程：
1. 根据本地 JSON 文件夹中的文件名，从 DataWeave 平台下载对应的 ZIP 文件到本地
2. 上传 ZIP 文件到服务器 /data01/rere_zips
3. 在服务器上解压 ZIP，匹配替换对应的标注 JSON 文件，调整目录结构
4. 下载处理后的数据到本地，进行标注质量检查
5. 将检查通过的数据移动到服务器最终目录

使用方法：
    python pipeline.py --json_dir /path/to/jsons --step all
    python pipeline.py --json_dir /path/to/jsons --step download
    python pipeline.py --json_dir /path/to/jsons --step check
"""

import os
import sys
import yaml
import argparse
import logging
import tempfile
import shutil
import copy
from pathlib import Path
from datetime import datetime

# 添加项目路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# ================= 配置区域 =================
# DataWeave API 配置
API_BASE_URL = "https://dataweave.enableai.cn/api/v4"
API_URL = f"{API_BASE_URL}/file/url"
# Cloudreve v4 登录 API
LOGIN_URL = f"{API_BASE_URL}/session/token"

# DataWeave 登录凭据 (用于自动获取 Token)
# 如果设置了用户名密码，会自动登录获取 Token
DATAWEAVE_USERNAME = "dongshucai@126.com"  # 填写你的用户名
DATAWEAVE_PASSWORD = "dongshucai"  # 填写你的密码

# 多个可能的路径模板 (按优先级顺序，会依次尝试直到找到文件)
DATAWEAVE_PATH_TEMPLATES = [
    "dataweave://my/TO_RERE/盲区数据/{filename}",
    "dataweave://my/TO_RERE/7Lidar_data/{filename}",
    "dataweave://my/TO_RERE/已上传平台/{filename}",
    "dataweave://my/TO_RERE/未上传平台/{filename}",
    "dataweave://my/TO_RERE/剔除非关键帧&重新上传/{filename}",
    "dataweave://my/TO_RERE/12-9/{filename}",
]
# 备用 Token (如果自动登录失败，会使用此 Token)
AUTH_TOKEN = "Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ0b2tlbl90eXBlIjoiYWNjZXNzIiwic3ViIjoidjRCUWlhIiwiZXhwIjoxNzY3Njc3NDMzLCJuYmYiOjE3Njc2NzM4MzN9.F0C1ZkAQxr4uAGVBRIpIMXFJwHW9Ke1x-KshxLMgCs8"

# 服务器配置
SERVER_IP = "222.223.112.212"
SERVER_USER = "user"
SERVER_ZIP_DIR = "/data01/rere_zips"                    # 上传 ZIP 的临时目录
SERVER_PROCESS_DIR = "/data01/processing"  # 处理中的数据目录
SERVER_FINAL_DIR = "/data01/dataset/scenesnew"         # 检查通过后的最终目录

# 处理完成后对原始 ZIP 的操作方式
# "rename": 重命名为 processed_xxx.zip (默认，标记已处理)
# "keep": 保留原始文件不变
# "delete": 删除原始 ZIP 文件
ZIP_AFTER_PROCESS = "rename"

# 本地临时目录 (用于下载 ZIP 和检查数据)
LOCAL_TEMP_DIR = "/media/zgw/T7/test_pipeline_downzips/"

# 是否将 JSON 重命名为 annotations.json
RENAME_JSON = True

# 多线程配置
MAX_WORKERS = 3  # 并发处理的线程数 (建议 2-4，太多会占用服务器资源)

# 配置文件路径
CONFIG_PATH = "configs/user_config.yaml"
# ===========================================

import threading
from concurrent.futures import ThreadPoolExecutor, as_completed

# 简洁的日志格式
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger(__name__)

# 禁用 paramiko 的详细日志
logging.getLogger("paramiko").setLevel(logging.WARNING)

# 线程锁，用于保护共享资源
results_lock = threading.Lock()

# 进度显示工具
class ProgressTracker:
    """简洁的进度追踪器"""
    def __init__(self, total: int, title: str = "处理进度"):
        self.total = total
        self.completed = 0
        self.success = 0
        self.failed = 0
        self.title = title
        self.lock = threading.Lock()
        self.start_time = datetime.now()
    
    def update(self, success: bool = True, name: str = ""):
        with self.lock:
            self.completed += 1
            if success:
                self.success += 1
            else:
                self.failed += 1
            self._display(name, success)
    
    def _display(self, name: str, success: bool):
        percent = self.completed / self.total * 100 if self.total > 0 else 0
        width = 25
        filled = int(width * self.completed / self.total) if self.total > 0 else 0
        bar = '━' * filled + '╸' + '─' * (width - filled - 1) if filled < width else '━' * width
        
        status = "✓" if success else "✗"
        elapsed = (datetime.now() - self.start_time).seconds
        
        # 清除当前行并显示进度
        sys.stdout.write(f'\r\033[K')
        sys.stdout.write(f'[{bar}] {self.completed}/{self.total} ({percent:.0f}%) │ {status} {name[:30]:<30}')
        sys.stdout.flush()
        
        if self.completed >= self.total:
            print()  # 完成后换行
    
    def summary(self):
        elapsed = (datetime.now() - self.start_time).seconds
        mins, secs = divmod(elapsed, 60)
        print(f"\n{'─'*50}")
        print(f"  📊 {self.title} 完成")
        print(f"  ✓ 成功: {self.success}  ✗ 失败: {self.failed}  ⏱ 耗时: {mins}分{secs}秒")
        print(f"{'─'*50}")


class AnnotationPipeline:
    """标注数据处理流水线"""
    
    def __init__(self, json_dir: str, local_zip_dir: str = None):
        self.json_dir = Path(json_dir)
        self.local_zip_dir = Path(local_zip_dir) if local_zip_dir else Path(LOCAL_TEMP_DIR) / "zips"
        self.local_check_dir = Path(LOCAL_TEMP_DIR) / "check_data"
        
        # 确保目录存在
        self.local_zip_dir.mkdir(parents=True, exist_ok=True)
        self.local_check_dir.mkdir(parents=True, exist_ok=True)
        
        # 清理不完整的下载文件（.tmp 临时文件）
        self._cleanup_incomplete_downloads()
        
        # SSH 连接
        self.ssh = None
        self.sftp = None
        
        # 处理结果跟踪
        self.results = {
            'downloaded': [],
            'skipped_server_exists': [],  # 服务器上已存在，跳过下载
            'uploaded': [],
            'processed': [],
            'check_passed': [],
            'check_failed': [],
            'moved_to_final': []
        }
        
        # 错误追踪 (用于追溯失败原因)
        self.errors = {}  # {stem: [(step, error_msg), ...]}
        self.errors_lock = threading.Lock()
        
        # Token 管理
        self._token = None
        self._token_time = None
        self._token_lock = threading.Lock()
        self._token_max_age = 50 * 60  # Token 有效期 50 分钟 (服务端1小时过期)
    
    def _cleanup_incomplete_downloads(self):
        """清理不完整的下载文件（.tmp 临时文件）"""
        tmp_files = list(self.local_zip_dir.glob("*.tmp"))
        if tmp_files:
            logger.info(f"发现 {len(tmp_files)} 个未完成的下载，正在清理...")
            for tmp_file in tmp_files:
                try:
                    tmp_file.unlink()
                    logger.info(f"  已删除: {tmp_file.name}")
                except Exception as e:
                    logger.warning(f"  删除失败 {tmp_file.name}: {e}")
    
    def _is_valid_zip(self, zip_path: Path) -> bool:
        """检查 ZIP 文件是否有效（存在、非空、可读）"""
        if not zip_path.exists():
            return False
        if zip_path.stat().st_size == 0:
            return False
        # 快速检查 ZIP 文件头
        try:
            with open(zip_path, 'rb') as f:
                header = f.read(4)
                # ZIP 文件以 PK\x03\x04 开头
                return header[:2] == b'PK'
        except:
            return False
    
    def _connect_server(self):
        """连接远程服务器"""
        if self.ssh is not None:
            return True
        try:
            import paramiko
            self.ssh = paramiko.SSHClient()
            self.ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            logger.info(f"正在连接服务器 {SERVER_IP}...")
            self.ssh.connect(SERVER_IP, username=SERVER_USER, timeout=10)
            self.sftp = self.ssh.open_sftp()
            logger.info("服务器连接成功")
            return True
        except Exception as e:
            logger.error(f"连接服务器失败: {e}")
            return False
    
    def _close_server(self):
        """关闭服务器连接"""
        if self.sftp:
            self.sftp.close()
        if self.ssh:
            self.ssh.close()
        self.ssh = None
        self.sftp = None
    
    def _exec_remote(self, cmd: str) -> tuple:
        """执行远程命令"""
        stdin, stdout, stderr = self.ssh.exec_command(cmd)
        exit_status = stdout.channel.recv_exit_status()
        out = stdout.read().decode().strip()
        err = stderr.read().decode().strip()
        return exit_status, out, err
    
    def _exec_remote_thread(self, ssh, cmd: str, timeout: int = 60) -> tuple:
        """线程中执行远程命令 (使用传入的 ssh 连接)，带超时控制"""
        try:
            stdin, stdout, stderr = ssh.exec_command(cmd, timeout=timeout)
            exit_status = stdout.channel.recv_exit_status()
            out = stdout.read().decode().strip()
            err = stderr.read().decode().strip()
            return exit_status, out, err
        except Exception as e:
            return -1, "", str(e)
    
    def _get_dataweave_token(self, force_refresh: bool = False) -> str:
        """自动登录 DataWeave (Cloudreve v4) 获取 Token (线程安全，支持自动刷新)"""
        import requests
        import time
        
        with self._token_lock:
            # 检查缓存的 Token 是否有效
            if not force_refresh and self._token and self._token_time:
                elapsed = time.time() - self._token_time
                if elapsed < self._token_max_age:
                    return self._token
                else:
                    logger.info("Token 即将过期，自动刷新...")
            
            # 如果没有配置用户名密码，使用备用 Token
            if not DATAWEAVE_USERNAME or not DATAWEAVE_PASSWORD:
                return AUTH_TOKEN
            
            # 重试获取 Token
            for attempt in range(3):
                try:
                    login_data = {
                        "email": DATAWEAVE_USERNAME,
                        "password": DATAWEAVE_PASSWORD
                    }
                    headers = {
                        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36",
                        "Content-Type": "application/json",
                        "Accept": "application/json",
                        "Origin": "https://dataweave.enableai.cn",
                        "Referer": "https://dataweave.enableai.cn/session",
                    }
                    
                    r = requests.post(LOGIN_URL, json=login_data, headers=headers, timeout=15)
                    data = r.json()
                    
                    if data.get("code") == 0:
                        token_data = data.get("data", {}).get("token", {})
                        access_token = token_data.get("access_token")
                        
                        if access_token:
                            self._token = f"Bearer {access_token}"
                            self._token_time = time.time()
                            if not force_refresh:
                                print("  🔑 Token 获取成功")
                            else:
                                logger.info("🔑 Token 刷新成功")
                            return self._token
                
                except Exception as e:
                    if attempt < 2:
                        time.sleep(1)
                        continue
            
            print("  ⚠ 使用备用 Token")
            return AUTH_TOKEN
    
    # ==================== 步骤 1: 下载 ZIP ====================
    def step1_download_zips(self):
        """从 DataWeave 下载 ZIP 文件"""
        import requests
        
        logger.info("=" * 60)
        logger.info("步骤 1: 从 DataWeave 下载 ZIP 文件")
        logger.info("=" * 60)
        
        json_files = list(self.json_dir.glob("*.json"))
        if not json_files:
            logger.warning(f"未在 {self.json_dir} 找到 JSON 文件")
            return
        
        logger.info(f"找到 {len(json_files)} 个 JSON 文件，准备下载对应的 ZIP")
        
        # 连接服务器获取已存在的ZIP文件列表
        # 存储原始文件名（去掉 processed_ 前缀），便于统一匹配
        server_zip_originals = set()
        if self._connect_server():
            status, out, err = self._exec_remote(f"ls {SERVER_ZIP_DIR}/*.zip 2>/dev/null || true")
            if out:
                for line in out.splitlines():
                    name = Path(line.strip()).name
                    # 去掉 processed_ 前缀，统一存储原始名称
                    if name.startswith("processed_"):
                        original_name = name[len("processed_"):]
                        server_zip_originals.add(original_name)
                    else:
                        server_zip_originals.add(name)
            logger.info(f"服务器上已有 {len(server_zip_originals)} 个 ZIP 文件 (含已处理)")
        
        # 自动获取 Token
        auth_token = self._get_dataweave_token()
        
        headers = {
            "User-Agent": "Mozilla/5.0",
            "Content-Type": "application/json",
            "Authorization": auth_token,
        }
        
        for i, json_file in enumerate(json_files):
            stem = json_file.stem
            zip_name = f"{stem}.zip"
            target_file = self.local_zip_dir / zip_name
            
            logger.info(f"[{i+1}/{len(json_files)}] 处理: {stem}")
            
            # 检查服务器上是否已存在 (统一比较原始文件名)
            if zip_name in server_zip_originals:
                logger.info(f"    服务器上已存在，跳过下载")
                self.results['skipped_server_exists'].append(stem)
                continue
            
            # 检查本地是否已存在且完整
            if self._is_valid_zip(target_file):
                logger.info(f"    本地文件已存在且完整，无需下载")
                self.results['downloaded'].append(stem)
                continue
            
            try:
                # 在多个路径模板中查找文件
                real_url = None
                found_path = None
                
                for path_template in DATAWEAVE_PATH_TEMPLATES:
                    dw_path = path_template.format(filename=zip_name)
                    payload = {"uris": [dw_path]}
                    
                    r = requests.post(API_URL, json=payload, headers=headers, timeout=15)
                    r.raise_for_status()
                    data = r.json()
                    
                    # 检查认证错误
                    if data.get("code") != 0:
                        msg = data.get("msg", "")
                        if "Login required" in msg or data.get("code") == 401:
                            logger.critical("!!! Token 已过期，请更新 AUTH_TOKEN !!!")
                            return
                        # 文件不存在，尝试下一个路径
                        continue
                    
                    # 解析 URL
                    url_data = data.get("data", {})
                    if isinstance(url_data, dict) and "urls" in url_data:
                        urls_list = url_data["urls"]
                        if urls_list and isinstance(urls_list[0], dict):
                            url = urls_list[0].get("url")
                            if url:
                                real_url = url
                                found_path = path_template.split("/")[-2]  # 提取子目录名
                                break
                
                if not real_url:
                    logger.warning(f"    在所有路径中均未找到文件")
                    continue
                
                logger.info(f"    找到文件，路径: {found_path}")
                
                # 下载文件
                logger.info(f"    开始下载...")
                download_headers = {"User-Agent": headers["User-Agent"]}
                with requests.get(real_url, headers=download_headers, stream=True, timeout=300) as r:
                    r.raise_for_status()
                    total_size = int(r.headers.get('content-length', 0))
                    with open(target_file, 'wb') as f:
                        downloaded = 0
                        for chunk in r.iter_content(chunk_size=8192):
                            if chunk:
                                f.write(chunk)
                                downloaded += len(chunk)
                                if total_size > 0:
                                    percent = (downloaded / total_size) * 100
                                    sys.stdout.write(f"\r    下载进度: {percent:.1f}%")
                                    sys.stdout.flush()
                    print()
                
                self.results['downloaded'].append(stem)
                logger.info(f"    下载完成")
                
            except Exception as e:
                logger.error(f"    下载失败: {e}")
                if target_file.exists():
                    target_file.unlink()
        
        logger.info(f"下载阶段完成: 新下载 {len(self.results['downloaded'])} 个, 跳过(服务器已有) {len(self.results['skipped_server_exists'])} 个")
    
    # ==================== 步骤 2: 上传 ZIP 到服务器 ====================
    def step2_upload_zips(self):
        """上传 ZIP 文件到服务器"""
        logger.info("=" * 60)
        logger.info("步骤 2: 上传 ZIP 文件到服务器")
        logger.info("=" * 60)
        
        if not self._connect_server():
            return
        
        # 确保远程目录存在
        self._exec_remote(f"mkdir -p {SERVER_ZIP_DIR}")
        
        zip_files = list(self.local_zip_dir.glob("*.zip"))
        if not zip_files:
            logger.warning("没有找到 ZIP 文件需要上传")
            return
        
        logger.info(f"准备上传 {len(zip_files)} 个 ZIP 文件")
        
        for i, zip_file in enumerate(zip_files):
            remote_path = f"{SERVER_ZIP_DIR}/{zip_file.name}"
            file_size_mb = zip_file.stat().st_size / (1024 * 1024)
            
            logger.info(f"[{i+1}/{len(zip_files)}] 上传: {zip_file.name} ({file_size_mb:.1f} MB)")
            
            try:
                # 检查远程文件是否存在
                try:
                    remote_stat = self.sftp.stat(remote_path)
                    if remote_stat.st_size == zip_file.stat().st_size:
                        logger.info(f"    文件已存在，跳过")
                        self.results['uploaded'].append(zip_file.stem)
                        continue
                except FileNotFoundError:
                    pass
                
                # 上传
                self.sftp.put(str(zip_file), remote_path)
                self.results['uploaded'].append(zip_file.stem)
                logger.info(f"    上传完成")
                
            except Exception as e:
                logger.error(f"    上传失败: {e}")
        
        logger.info(f"上传完成: {len(self.results['uploaded'])} 个文件")
    
    # ==================== 步骤 3: 服务器端解压处理 ====================
    def step3_process_on_server(self):
        """在服务器上解压并处理 ZIP 文件"""
        logger.info("=" * 60)
        logger.info("步骤 3: 服务器端解压处理")
        logger.info("=" * 60)
        
        if not self._connect_server():
            return
        
        # 确保处理目录存在
        self._exec_remote(f"mkdir -p {SERVER_PROCESS_DIR}")
        
        # 部署 worker 脚本
        self._deploy_worker_script()
        
        # 获取服务器上的 ZIP 文件
        status, out, err = self._exec_remote(f"ls {SERVER_ZIP_DIR}/*.zip 2>/dev/null || true")
        if not out:
            logger.warning("服务器上没有找到 ZIP 文件")
            return
        
        # 过滤掉已处理的 ZIP (以 processed_ 开头的)
        all_zips = [Path(f.strip()) for f in out.splitlines() if not Path(f.strip()).name.startswith("processed_")]
        
        # 如果配置为 keep 模式，需要检查输出目录是否已存在来判断是否已处理
        remote_zips = []
        if ZIP_AFTER_PROCESS == "keep":
            for zip_path in all_zips:
                # 检查处理输出目录是否已存在
                check_dir = f"{SERVER_PROCESS_DIR}/{zip_path.stem}"
                status, _, _ = self._exec_remote(f"test -d '{check_dir}' && echo exists")
                if status != 0:
                    remote_zips.append(zip_path)
                else:
                    logger.info(f"跳过已处理: {zip_path.name} (输出目录已存在)")
        else:
            remote_zips = all_zips
        
        logger.info(f"服务器上发现 {len(remote_zips)} 个待处理 ZIP 文件")
        
        for i, zip_path in enumerate(remote_zips):
            zip_stem = zip_path.stem
            logger.info(f"[{i+1}/{len(remote_zips)}] 处理: {zip_path.name}")
            
            # 查找对应的本地 JSON
            local_json = self._find_local_json(zip_stem)
            if not local_json:
                logger.warning(f"    跳过: 未找到对应的 JSON 文件")
                continue
            
            # 上传 JSON 到服务器临时位置
            remote_json_temp = f"/tmp/{local_json.name}"
            try:
                self.sftp.put(str(local_json), remote_json_temp)
                logger.info(f"    已上传 JSON: {local_json.name}")
            except Exception as e:
                logger.error(f"    上传 JSON 失败: {e}")
                continue
            
            # 执行远程处理脚本
            cmd = f"python3 /tmp/zip_worker.py --zip '{zip_path}' --json '{remote_json_temp}' --out '{SERVER_PROCESS_DIR}' --rename_json '{RENAME_JSON}'"
            status, out, err = self._exec_remote(cmd)
            
            if status == 0:
                logger.info(f"    处理成功")
                # 根据配置处理原始 ZIP 文件
                if ZIP_AFTER_PROCESS == "rename":
                    new_name = zip_path.parent / f"processed_{zip_path.name}"
                    self._exec_remote(f"mv '{zip_path}' '{new_name}'")
                    logger.info(f"    原始 ZIP 已重命名为: processed_{zip_path.name}")
                elif ZIP_AFTER_PROCESS == "delete":
                    self._exec_remote(f"rm '{zip_path}'")
                    logger.info(f"    原始 ZIP 已删除")
                else:  # keep
                    logger.info(f"    原始 ZIP 已保留")
                self.results['processed'].append(zip_stem)
            else:
                logger.error(f"    处理失败: {err}")
        
        logger.info(f"处理完成: {len(self.results['processed'])} 个文件")
    
    def _find_local_json(self, zip_stem: str) -> Path:
        """查找本地对应的 JSON 文件"""
        # 精确匹配
        exact = self.json_dir / f"{zip_stem}.json"
        if exact.exists():
            return exact
        
        # 模糊匹配
        for f in self.json_dir.glob("*.json"):
            if zip_stem in f.stem:
                return f
        return None
    
    def _deploy_worker_script(self):
        """部署远程 worker 脚本"""
        worker_code = '''
import os, sys, shutil, zipfile, argparse
from pathlib import Path

def find_data_root(extract_dir):
    required = ["camera_cam_3M_front", "combined_scales", "ins.json", "sample.json"]
    for root, dirs, files in os.walk(extract_dir):
        count = sum(1 for name in dirs + files if name in required)
        if count >= 2:
            return Path(root)
    return None

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--zip", required=True)
    parser.add_argument("--json", required=True)
    parser.add_argument("--out", required=True)
    parser.add_argument("--rename_json", default="False")
    args = parser.parse_args()
    
    zip_path, json_path = Path(args.zip), Path(args.json)
    output_root = Path(args.out)
    rename = args.rename_json.lower() == "true"
    
    final_dir = output_root / zip_path.stem
    temp_dir = output_root / f"temp_{zip_path.stem}"
    
    if temp_dir.exists(): shutil.rmtree(temp_dir)
    temp_dir.mkdir(parents=True, exist_ok=True)
    
    try:
        with zipfile.ZipFile(zip_path, 'r') as zf:
            zf.extractall(temp_dir)
        
        data_root = find_data_root(temp_dir)
        if not data_root:
            raise Exception("未找到数据根目录")
        
        final_dir.mkdir(parents=True, exist_ok=True)
        
        # 移动 JSON
        target_json = "annotations.json" if rename else json_path.name
        shutil.copy(str(json_path), str(final_dir / target_json))
        
        # 移动必需文件
        keep = ["sample.json", "ins.json", "sensor_config_combined_latest.json",
                "combined_scales", "camera_cam_3M_front", "camera_cam_3M_left",
                "camera_cam_3M_right", "camera_cam_3M_rear", "camera_cam_8M_wa_front",
                "iv_points_front_left", "iv_points_front_mid", "iv_points_front_right",
                "iv_points_rear_left", "iv_points_rear_right"]
        
        for item in keep:
            src = data_root / item
            if src.exists():
                dst = final_dir / item
                if dst.exists():
                    if dst.is_dir(): shutil.rmtree(dst)
                    else: dst.unlink()
                if src.is_dir(): shutil.copytree(str(src), str(dst))
                else: shutil.copy(str(src), str(dst))
        
        print("OK")
    finally:
        if temp_dir.exists(): shutil.rmtree(temp_dir)

if __name__ == "__main__":
    main()
'''
        with self.sftp.file("/tmp/zip_worker.py", "w") as f:
            f.write(worker_code)
        logger.info("已部署远程工作脚本")
    
    # ==================== 步骤 4: 检查标注质量 ====================
    def step4_check_annotations(self):
        """在服务器上直接检查标注质量"""
        logger.info("=" * 60)
        logger.info("步骤 4: 检查标注质量 (服务器端执行)")
        logger.info("=" * 60)
        
        if not self._connect_server():
            return
        
        # 获取处理目录中的数据
        status, out, err = self._exec_remote(f"ls -d {SERVER_PROCESS_DIR}/*/ 2>/dev/null || true")
        if not out:
            logger.warning("没有找到待检查的数据")
            return
        
        data_dirs = [d.strip().rstrip('/') for d in out.splitlines() if d.strip()]
        logger.info(f"发现 {len(data_dirs)} 个待检查的数据目录")
        
        # 部署检查脚本到服务器
        self._deploy_checker_script()
        
        # 加载本地配置获取检查规则
        project_root = Path(__file__).parent.parent
        config_path = project_root / CONFIG_PATH
        
        if config_path.exists():
            with open(config_path, 'r') as f:
                base_config = yaml.safe_load(f)
            # 上传配置到服务器
            config_content = yaml.dump(base_config)
            with self.sftp.file("/tmp/check_config.yaml", "w") as f:
                f.write(config_content)
        
        for remote_dir in data_dirs:
            dir_name = Path(remote_dir).name
            
            logger.info(f"检查: {dir_name}")
            
            # 在服务器上执行检查脚本
            report_path = f"/tmp/report_{dir_name}.txt"
            cmd = f"python3 /tmp/annotation_checker.py --data_dir '{remote_dir}' --config '/tmp/check_config.yaml' --report '{report_path}'"
            
            # 执行并实时获取输出
            stdin, stdout, stderr = self.ssh.exec_command(cmd)
            
            # 打印远程输出
            for line in iter(stdout.readline, ""):
                line = line.strip()
                if line:
                    logger.info(f"    [远程] {line}")
            
            status = stdout.channel.recv_exit_status()
            err = stderr.read().decode().strip()
            
            if status == 0:
                # 读取远程报告
                try:
                    with self.sftp.file(report_path, 'r') as f:
                        report_content = f.read().decode() if isinstance(f.read(), bytes) else ""
                    
                    # 重新读取
                    self.sftp.get(report_path, str(self.local_check_dir / f"report_{dir_name}.txt"))
                    report_content = (self.local_check_dir / f"report_{dir_name}.txt").read_text()
                    
                    issue_count = report_content.count("帧:")
                    
                    if issue_count == 0:
                        logger.info(f"    ✓ 检查通过，无问题")
                        self.results['check_passed'].append(dir_name)
                    else:
                        logger.warning(f"    ✗ 发现 {issue_count} 个问题帧")
                        logger.warning(f"      报告已保存: {self.local_check_dir}/report_{dir_name}.txt")
                        self.results['check_failed'].append(dir_name)
                except Exception as e:
                    # 如果没有报告文件，说明检查通过
                    logger.info(f"    ✓ 检查通过")
                    self.results['check_passed'].append(dir_name)
            else:
                logger.error(f"    检查失败: {err}")
                self.results['check_failed'].append(dir_name)
        
        logger.info(f"检查完成: 通过 {len(self.results['check_passed'])}, 失败 {len(self.results['check_failed'])}")
    
    def _deploy_checker_script(self):
        """部署检查脚本到服务器"""
        checker_code = '''
import os
import sys
import json
import math
import argparse
import yaml
import numpy as np
from pathlib import Path

def get_euler_angles(q):
    w, x, y, z = q
    sinr_cosp = 2 * (w * x + y * z)
    cosr_cosp = 1 - 2 * (x * x + y * y)
    roll = math.atan2(sinr_cosp, cosr_cosp)
    sinp = 2 * (w * y - z * x)
    pitch = math.asin(max(-1, min(1, sinp)))
    siny_cosp = 2 * (w * z + x * y)
    cosy_cosp = 1 - 2 * (y * y + z * z)
    yaw = math.atan2(siny_cosp, cosy_cosp)
    return roll, pitch, yaw

def quaternion_to_rotation_matrix(q):
    """四元数转旋转矩阵 (w, x, y, z) 格式"""
    w, x, y, z = q
    return np.array([
        [1 - 2*(y*y + z*z), 2*(x*y - w*z), 2*(x*z + w*y)],
        [2*(x*y + w*z), 1 - 2*(x*x + z*z), 2*(y*z - w*x)],
        [2*(x*z - w*y), 2*(y*z + w*x), 1 - 2*(x*x + y*y)]
    ])

def transform_to_world(pos_ego, ins_entry):
    """将自车坐标系下的位置转换到世界坐标系 (UTM)"""
    ego_utm = np.array([
        ins_entry.get('utm_x', 0),
        ins_entry.get('utm_y', 0),
        ins_entry.get('utm_z', 0)
    ])
    q_ego = [
        ins_entry.get('quaternion_w', 1),
        ins_entry.get('quaternion_x', 0),
        ins_entry.get('quaternion_y', 0),
        ins_entry.get('quaternion_z', 0)
    ]
    R_ego = quaternion_to_rotation_matrix(q_ego)
    pos_world = R_ego @ np.array(pos_ego) + ego_utm
    return pos_world

def check_object(obj, rules, prev_obj=None, next_obj=None, 
                 curr_ins=None, prev_ins=None, next_ins=None):
    issues = []
    size = obj.get('size', [1, 1, 1])
    if len(size) >= 3:
        l, w, h = size[0], size[1], size[2]
    else:
        return issues
    
    obj_class = obj.get('attribute_tokens', {}).get('Class', '').lower()
    
    # 尺寸检查 (仅检查长宽，不检查高度)
    if 'vehicle' in obj_class:
        ranges = rules.get('vehicle', {})
        if ranges:
            lr = ranges.get('length_range', [2, 12])
            wr = ranges.get('width_range', [1, 3])
            if not (lr[0] <= l <= lr[1]): issues.append(f"长度异常: {l:.2f}")
            if not (wr[0] <= w <= wr[1]): issues.append(f"宽度异常: {w:.2f}")
    
    # 四元数检查
    rotation = obj.get('rotation', [])
    if len(rotation) == 4:
        norm = math.sqrt(sum(x*x for x in rotation))
        if abs(norm - 1.0) > 0.01:
            issues.append(f"四元数未归一化: {norm:.4f}")
        
        # 姿态角检查 (仅车辆)
        if 'vehicle' in obj_class:
            roll, pitch, yaw = get_euler_angles(rotation)
            if abs(roll) > 0.5:
                issues.append(f"Roll角异常: {math.degrees(roll):.1f}度")
            if abs(pitch) > 0.5:
                issues.append(f"Pitch角异常: {math.degrees(pitch):.1f}度")
    
    # 朝向与运动方向一致性检查 (车辆) - 使用自车位姿补偿
    if 'vehicle' in obj_class and (prev_obj or next_obj):
        curr_pos_ego = np.array(obj.get('translation', [0, 0, 0]))
        rotation = obj.get('rotation', [])
        
        # 是否使用世界坐标系
        use_world = curr_ins is not None
        
        if len(rotation) == 4 and len(curr_pos_ego) >= 2:
            # 转换当前位置
            if use_world:
                curr_pos = transform_to_world(curr_pos_ego, curr_ins)
            else:
                curr_pos = curr_pos_ego
            
            # 计算运动向量
            motion_vec = None
            if prev_obj and next_obj:
                prev_pos_ego = np.array(prev_obj.get('translation', [0, 0, 0]))
                next_pos_ego = np.array(next_obj.get('translation', [0, 0, 0]))
                if use_world and prev_ins and next_ins:
                    prev_pos = transform_to_world(prev_pos_ego, prev_ins)
                    next_pos = transform_to_world(next_pos_ego, next_ins)
                else:
                    prev_pos, next_pos = prev_pos_ego, next_pos_ego
                motion_vec = next_pos[:2] - prev_pos[:2]
            elif next_obj:
                next_pos_ego = np.array(next_obj.get('translation', [0, 0, 0]))
                if use_world and next_ins:
                    next_pos = transform_to_world(next_pos_ego, next_ins)
                else:
                    next_pos = next_pos_ego
                motion_vec = next_pos[:2] - curr_pos[:2]
            elif prev_obj:
                prev_pos_ego = np.array(prev_obj.get('translation', [0, 0, 0]))
                if use_world and prev_ins:
                    prev_pos = transform_to_world(prev_pos_ego, prev_ins)
                else:
                    prev_pos = prev_pos_ego
                motion_vec = curr_pos[:2] - prev_pos[:2]
            
            if motion_vec is not None:
                dist = np.linalg.norm(motion_vec)
                # 只有位移足够大时才检查朝向一致性 (排除静止物体)
                if dist > 0.5:
                    motion_yaw = math.atan2(motion_vec[1], motion_vec[0])
                    _, _, obj_yaw_ego = get_euler_angles(rotation)
                    
                    # 如果使用世界坐标系，转换标注朝向
                    if use_world and curr_ins:
                        ego_yaw = curr_ins.get('azimuth', 0)
                        obj_yaw = ego_yaw + obj_yaw_ego
                    else:
                        obj_yaw = obj_yaw_ego
                    
                    # 计算角度差
                    diff = motion_yaw - obj_yaw
                    while diff > math.pi:
                        diff -= 2 * math.pi
                    while diff < -math.pi:
                        diff += 2 * math.pi
                    diff = abs(diff)
                    
                    # 允许误差60度，也允许倒车(差值接近180度)
                    is_forward = diff < 1.05  # ~60度
                    is_backward = abs(diff - math.pi) < 1.05
                    
                    if not is_forward and not is_backward:
                        issues.append(f"朝向与运动方向不一致: 差值{math.degrees(diff):.1f}度")
    
    return issues

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--data_dir", required=True)
    parser.add_argument("--config", required=True)
    parser.add_argument("--report", required=True)
    args = parser.parse_args()
    
    data_dir = Path(args.data_dir)
    
    # 加载配置
    rules = {}
    if Path(args.config).exists():
        with open(args.config, 'r') as f:
            config = yaml.safe_load(f)
            rules = config.get('rules', {})
    
    # 加载 INS 数据 (自车位姿)
    ins_data = None
    ins_file = data_dir / 'ins.json'
    if ins_file.exists():
        try:
            with open(ins_file, 'r') as f:
                ins_data = json.load(f)
            print(f"已加载 INS 数据: {len(ins_data)} 条")
        except Exception as e:
            print(f"加载 INS 数据失败: {e}")
    else:
        print("未找到 ins.json，将不进行自车位姿补偿")
    
    # 查找标注文件
    annotation_file = None
    for name in ['annotations.json', 'annotation.json']:
        p = data_dir / name
        if p.exists():
            annotation_file = p
            break
    
    if not annotation_file:
        for f in data_dir.glob('*.json'):
            if f.name not in ['sample.json', 'ins.json', 'sensor_config_combined_latest.json']:
                annotation_file = f
                break
    
    if not annotation_file:
        print("ERROR: 未找到标注文件")
        sys.exit(1)
    
    # 加载标注
    print(f"加载标注文件: {annotation_file.name}")
    with open(annotation_file, 'r') as f:
        data = json.load(f)
    
    # 检查
    issues_by_frame = {}
    total_frames = 0
    total_objects = 0
    issue_objects = 0
    
    frames_to_check = []
    
    if isinstance(data, dict):
        if 'frames' in data:
            for frame in data['frames']:
                frame_id = frame.get('frame_id', frame.get('id', 'unknown'))
                objects = frame.get('objects', [])
                frames_to_check.append((str(frame_id), objects))
        else:
            for frame_id, objects in data.items():
                if isinstance(objects, list):
                    frames_to_check.append((str(frame_id), objects))
    
    try:
        frames_to_check.sort(key=lambda x: int(x[0]))
    except:
        frames_to_check.sort(key=lambda x: x[0])
    
    total_frames = len(frames_to_check)
    print(f"开始检查 {total_frames} 帧...")
    
    # 构建 INS 索引 (按帧索引)
    frame_to_ins = {}
    if ins_data:
        for i in range(min(len(frames_to_check), len(ins_data))):
            frame_to_ins[i] = ins_data[i]
    
    # 构建实例轨迹
    tracks = {}
    for i, (frame_id, objects) in enumerate(frames_to_check):
        for obj in objects:
            inst_id = obj.get('instance_token')
            if inst_id:
                if inst_id not in tracks:
                    tracks[inst_id] = []
                tracks[inst_id].append((i, obj))
    
    for inst_id in tracks:
        tracks[inst_id].sort(key=lambda x: x[0])
    
    for i, (frame_id, objects) in enumerate(frames_to_check):
        if (i + 1) % 20 == 0 or i == total_frames - 1:
            print(f"  进度: {i+1}/{total_frames} ({(i+1)*100//total_frames}%)")
        
        total_objects += len(objects)
        curr_ins = frame_to_ins.get(i)
        
        frame_issues = []
        for obj in objects:
            prev_obj, next_obj = None, None
            prev_ins, next_ins = None, None
            inst_id = obj.get('instance_token')
            if inst_id and inst_id in tracks:
                track = tracks[inst_id]
                for idx, (frame_idx, track_obj) in enumerate(track):
                    if frame_idx == i:
                        if idx > 0:
                            prev_obj = track[idx - 1][1]
                            prev_ins = frame_to_ins.get(track[idx - 1][0])
                        if idx < len(track) - 1:
                            next_obj = track[idx + 1][1]
                            next_ins = frame_to_ins.get(track[idx + 1][0])
                        break
            
            obj_issues = check_object(obj, rules, prev_obj, next_obj,
                                       curr_ins, prev_ins, next_ins)
            if obj_issues:
                issue_objects += 1
                frame_issues.append({
                    'token': obj.get('token', 'unknown'),
                    'class': obj.get('attribute_tokens', {}).get('Class', 'unknown'),
                    'issues': obj_issues
                })
        if frame_issues:
            issues_by_frame[str(frame_id)] = frame_issues
    
    issue_frames = len(issues_by_frame)
    print(f"\\n检查完成!")
    print(f"  总帧数: {total_frames}")
    print(f"  总对象数: {total_objects}")
    print(f"  问题帧数: {issue_frames}")
    print(f"  问题对象数: {issue_objects}")
    
    with open(args.report, 'w') as f:
        f.write(f"检查报告 - {data_dir.name}\\n")
        f.write("=" * 50 + "\\n\\n")
        f.write(f"统计汇总:\\n")
        f.write(f"  总帧数: {total_frames}\\n")
        f.write(f"  总对象数: {total_objects}\\n")
        f.write(f"  问题帧数: {issue_frames}\\n")
        f.write(f"  问题对象数: {issue_objects}\\n")
        f.write(f"  通过率: {(total_frames - issue_frames) * 100 / max(total_frames, 1):.1f}%\\n")
        if ins_data:
            f.write(f"  自车位姿补偿: 已启用 ({len(ins_data)} 条INS数据)\\n")
        else:
            f.write(f"  自车位姿补偿: 未启用\\n")
        f.write("\\n" + "=" * 50 + "\\n\\n")
        
        if not issues_by_frame:
            f.write("恭喜! 所有帧检查通过，未发现问题。\\n")
        else:
            f.write("问题详情:\\n\\n")
            for frame_id, issues in sorted(issues_by_frame.items(), key=lambda x: int(x[0]) if x[0].isdigit() else x[0]):
                f.write(f"帧: {frame_id}\\n")
                for item in issues:
                    f.write(f"  对象: {item['token']} (类别: {item['class']})\\n")
                    for issue in item['issues']:
                        f.write(f"    - {issue}\\n")
                f.write("\\n")
    
    if issue_frames == 0:
        print("RESULT: PASS")
    else:
        print(f"RESULT: FAIL ({issue_frames} frames with issues)")

if __name__ == "__main__":
    main()
'''
        with self.sftp.file("/tmp/annotation_checker.py", "w") as f:
            f.write(checker_code)
        logger.info("已部署检查脚本到服务器")
    
    def _download_dir(self, remote_dir: str, local_dir: Path):
        """递归下载远程目录"""
        for item in self.sftp.listdir_attr(remote_dir):
            remote_path = f"{remote_dir}/{item.filename}"
            local_path = local_dir / item.filename
            
            if item.st_mode & 0o40000:  # 是目录
                local_path.mkdir(exist_ok=True)
                self._download_dir(remote_path, local_path)
            else:
                self.sftp.get(remote_path, str(local_path))
    
    # ==================== 步骤 5: 移动通过的数据到最终目录 ====================
    def step5_move_to_final(self):
        """将检查通过的数据移动到最终目录"""
        logger.info("=" * 60)
        logger.info("步骤 5: 移动通过的数据到最终目录")
        logger.info("=" * 60)
        
        if not self.results['check_passed']:
            logger.warning("没有检查通过的数据需要移动")
            return
        
        if not self._connect_server():
            return
        
        # 确保最终目录存在
        self._exec_remote(f"mkdir -p {SERVER_FINAL_DIR}")
        
        for dir_name in self.results['check_passed']:
            src = f"{SERVER_PROCESS_DIR}/{dir_name}"
            dst = f"{SERVER_FINAL_DIR}/{dir_name}"
            
            logger.info(f"移动: {dir_name}")
            
            # 检查源目录是否存在
            status, _, _ = self._exec_remote(f"test -d '{src}' && echo ok")
            if status != 0:
                logger.warning(f"    源目录不存在，跳过")
                continue
            
            # 安全移动：如果目标已存在，先备份而不是直接删除
            status, out, _ = self._exec_remote(f"test -d '{dst}' && echo exists")
            if out.strip() == 'exists':
                backup_dst = f"{dst}.bak.{datetime.now().strftime('%Y%m%d%H%M%S')}"
                self._exec_remote(f"mv '{dst}' '{backup_dst}'")
                logger.info(f"    已备份旧数据到: {backup_dst}")
            
            # 移动目录
            status, _, err = self._exec_remote(f"mv '{src}' '{dst}'")
            if status == 0:
                logger.info(f"    ✓ 已移动到 {dst}")
                self.results['moved_to_final'].append(dir_name)
            else:
                logger.error(f"    移动失败: {err}")
                self._log_error(dir_name, "移动", f"移动到最终目录失败: {err}")
        
        logger.info(f"移动完成: {len(self.results['moved_to_final'])} 个目录")
        
        logger.info(f"移动完成: {len(self.results['moved_to_final'])} 个目录")
    
    # ==================== 流式处理：下载一个处理一个 ====================
    def run_streaming(self):
        """
        流式处理模式：下载一个文件后立即进行完整处理流程
        每个文件：下载 -> 上传 -> 解压处理 -> 检查 -> 移动到最终目录
        """
        import requests
        
        logger.info("=" * 60)
        logger.info("标注数据自动化处理流水线 (流式处理模式)")
        logger.info(f"JSON 目录: {self.json_dir}")
        logger.info("处理方式: 下载一个文件就立即处理，无需等待全部下载")
        logger.info("=" * 60)
        
        json_files = list(self.json_dir.glob("*.json"))
        if not json_files:
            logger.warning(f"未在 {self.json_dir} 找到 JSON 文件")
            return
        
        logger.info(f"找到 {len(json_files)} 个 JSON 文件需要处理")
        
        try:
            # 连接服务器
            if not self._connect_server():
                logger.error("无法连接服务器，退出")
                return
            
            # 确保远程目录存在
            self._exec_remote(f"mkdir -p {SERVER_ZIP_DIR}")
            self._exec_remote(f"mkdir -p {SERVER_PROCESS_DIR}")
            
            # 部署远程脚本
            self._deploy_worker_script()
            self._deploy_checker_script()
            
            # 上传检查配置
            project_root = Path(__file__).parent.parent
            config_path = project_root / CONFIG_PATH
            if config_path.exists():
                with open(config_path, 'r') as f:
                    base_config = yaml.safe_load(f)
                config_content = yaml.dump(base_config)
                with self.sftp.file("/tmp/check_config.yaml", "w") as f:
                    f.write(config_content)
            
            # 获取服务器上已有的 ZIP 文件（含已处理的）
            server_zip_originals = set()
            status, out, err = self._exec_remote(f"ls {SERVER_ZIP_DIR}/*.zip 2>/dev/null || true")
            if out:
                for line in out.splitlines():
                    name = Path(line.strip()).name
                    if name.startswith("processed_"):
                        server_zip_originals.add(name[len("processed_"):])
                    else:
                        server_zip_originals.add(name)
            logger.info(f"服务器上已有 {len(server_zip_originals)} 个 ZIP 文件")
            
            # 获取已处理完成的目录（仅检查最终目录）
            processed_dirs = set()
            status, out, err = self._exec_remote(f"ls -d {SERVER_FINAL_DIR}/*/ 2>/dev/null || true")
            if out:
                for line in out.splitlines():
                    dir_name = Path(line.strip().rstrip('/')).name
                    processed_dirs.add(dir_name)
            logger.info(f"已处理完成的数据（最终目录）: {len(processed_dirs)} 个")
            
            # 自动获取 Token
            auth_token = self._get_dataweave_token()
            headers = {
                "User-Agent": "Mozilla/5.0",
                "Content-Type": "application/json",
                "Authorization": auth_token,
            }
            
            # 逐个处理每个 JSON 文件
            for i, json_file in enumerate(json_files):
                stem = json_file.stem
                zip_name = f"{stem}.zip"
                
                logger.info("")
                logger.info(f"{'='*60}")
                logger.info(f"[{i+1}/{len(json_files)}] 处理文件: {stem}")
                logger.info(f"{'='*60}")
                
                # 检查是否已完全处理过
                if stem in processed_dirs:
                    logger.info(f"  → 已处理完成，跳过")
                    self.results['check_passed'].append(stem)
                    continue
                
                # ===== 步骤 1: 下载或检查 =====
                local_zip = self.local_zip_dir / zip_name
                remote_zip = f"{SERVER_ZIP_DIR}/{zip_name}"
                need_download = True
                
                # 检查服务器上是否已存在
                if zip_name in server_zip_originals:
                    logger.info(f"  [下载] 服务器上已存在，跳过下载")
                    self.results['skipped_server_exists'].append(stem)
                    need_download = False
                elif self._is_valid_zip(local_zip):
                    logger.info(f"  [下载] 本地已存在且完整: {local_zip}")
                    need_download = False
                
                if need_download:
                    # 从 DataWeave 下载
                    logger.info(f"  [下载] 正在从 DataWeave 下载...")
                    downloaded = self._download_single_zip(stem, zip_name, local_zip, headers)
                    if not downloaded:
                        logger.error(f"  [下载] 下载失败，跳过此文件")
                        continue
                    self.results['downloaded'].append(stem)
                    logger.info(f"  [下载] ✓ 下载完成")
                
                # ===== 步骤 2: 上传到服务器 =====
                if zip_name not in server_zip_originals and local_zip.exists():
                    logger.info(f"  [上传] 正在上传到服务器...")
                    try:
                        self.sftp.put(str(local_zip), remote_zip)
                        self.results['uploaded'].append(stem)
                        logger.info(f"  [上传] ✓ 上传完成")
                    except Exception as e:
                        logger.error(f"  [上传] 上传失败: {e}")
                        continue
                
                # ===== 步骤 3: 服务器端处理 =====
                logger.info(f"  [处理] 正在服务器端解压处理...")
                
                # 上传 JSON 文件
                remote_json_temp = f"/tmp/{json_file.name}"
                try:
                    self.sftp.put(str(json_file), remote_json_temp)
                except Exception as e:
                    logger.error(f"  [处理] 上传 JSON 失败: {e}")
                    continue
                
                # 执行处理脚本
                cmd = f"python3 /tmp/zip_worker.py --zip '{remote_zip}' --json '{remote_json_temp}' --out '{SERVER_PROCESS_DIR}' --rename_json '{RENAME_JSON}'"
                status, out, err = self._exec_remote(cmd)
                
                if status != 0:
                    logger.error(f"  [处理] 处理失败: {err}")
                    continue
                
                # 处理原始 ZIP
                if ZIP_AFTER_PROCESS == "rename":
                    new_name = f"{SERVER_ZIP_DIR}/processed_{zip_name}"
                    self._exec_remote(f"mv '{remote_zip}' '{new_name}'")
                elif ZIP_AFTER_PROCESS == "delete":
                    self._exec_remote(f"rm '{remote_zip}'")
                
                self.results['processed'].append(stem)
                logger.info(f"  [处理] ✓ 处理完成")
                
                # ===== 步骤 4: 检查标注质量 =====
                logger.info(f"  [检查] 正在检查标注质量...")
                
                remote_data_dir = f"{SERVER_PROCESS_DIR}/{stem}"
                report_path = f"/tmp/report_{stem}.txt"
                cmd = f"python3 /tmp/annotation_checker.py --data_dir '{remote_data_dir}' --config '/tmp/check_config.yaml' --report '{report_path}'"
                
                status, out, err = self._exec_remote(cmd)
                
                check_passed = True
                local_report = self.local_check_dir / f"report_{stem}.txt"
                if status == 0:
                    try:
                        # 下载报告检查是否有问题
                        self.sftp.get(report_path, str(local_report))
                        report_content = local_report.read_text()
                        issue_count = report_content.count("帧:")
                        
                        if issue_count > 0:
                            check_passed = False
                            logger.warning(f"  [检查] ✗ 发现 {issue_count} 个问题帧")
                            logger.warning(f"         报告: {local_report}")
                        else:
                            # 检查通过，删除本地报告
                            if local_report.exists():
                                local_report.unlink()
                    except:
                        pass  # 没有报告文件说明通过
                else:
                    check_passed = False
                    logger.error(f"  [检查] 检查执行失败: {err}")
                
                if check_passed:
                    logger.info(f"  [检查] ✓ 检查通过")
                    self.results['check_passed'].append(stem)
                    
                    # ===== 步骤 5: 移动到最终目录 =====
                    logger.info(f"  [移动] 正在移动到最终目录...")
                    src = f"{SERVER_PROCESS_DIR}/{stem}"
                    dst = f"{SERVER_FINAL_DIR}/{stem}"
                    
                    # 安全移动：先检查目标是否存在，存在则备份而不是直接删除
                    status, out, _ = self._exec_remote(f"test -d '{dst}' && echo exists")
                    if out.strip() == 'exists':
                        backup_dst = f"{dst}.bak.{datetime.now().strftime('%Y%m%d%H%M%S')}"
                        self._exec_remote(f"mv '{dst}' '{backup_dst}'")
                        logger.info(f"  [移动] 已备份旧数据到: {backup_dst}")
                    
                    status, _, err = self._exec_remote(f"mv '{src}' '{dst}'")
                    
                    if status == 0:
                        logger.info(f"  [移动] ✓ 已移动到 {dst}")
                        self.results['moved_to_final'].append(stem)
                        # 流水线成功完成，删除本地 ZIP 文件
                        if local_zip.exists():
                            local_zip.unlink()
                            logger.info(f"  [清理] 已删除本地 ZIP: {local_zip.name}")
                    else:
                        logger.error(f"  [移动] 移动失败: {err}")
                        self._log_error(stem, "移动", f"移动到最终目录失败: {err}")
                else:
                    self.results['check_failed'].append(stem)
                    self._log_error(stem, "检查", f"检查未通过")
                
                logger.info(f"  → 文件处理完成")
        
        finally:
            self._close_server()
        
        # 输出汇总
        self._print_summary()
    
    # ==================== 多线程处理模式 ====================
    def run_parallel(self, num_workers: int = None):
        """
        多线程并行处理模式：多个线程同时下载和处理文件
        每个线程独立处理一个文件的完整流程
        """
        import requests
        
        if num_workers is None:
            num_workers = MAX_WORKERS
        
        print()
        print("╔" + "═" * 50 + "╗")
        print("║  📦 标注数据自动化处理流水线 (并行模式)".ljust(51) + "║")
        print("╚" + "═" * 50 + "╝")
        print(f"  📁 JSON目录: {self.json_dir}")
        
        json_files = list(self.json_dir.glob("*.json"))
        if not json_files:
            print(f"  ⚠ 未找到 JSON 文件")
            return
        
        print(f"  📋 共 {len(json_files)} 个文件")
        
        # 先用主连接初始化
        if not self._connect_server():
            print("  ✗ 无法连接服务器")
            return
        
        print(f"  🔗 已连接服务器: {SERVER_IP}")
        
        try:
            # 确保远程目录存在
            self._exec_remote(f"mkdir -p {SERVER_ZIP_DIR}")
            self._exec_remote(f"mkdir -p {SERVER_PROCESS_DIR}")
            
            # 部署远程脚本
            self._deploy_worker_script()
            self._deploy_checker_script()
            
            # 上传检查配置
            project_root = Path(__file__).parent.parent
            config_path = project_root / CONFIG_PATH
            if config_path.exists():
                with open(config_path, 'r') as f:
                    base_config = yaml.safe_load(f)
                config_content = yaml.dump(base_config)
                with self.sftp.file("/tmp/check_config.yaml", "w") as f:
                    f.write(config_content)
            
            # 获取服务器上已有的 ZIP 文件（含已处理的）
            server_zip_originals = set()
            status, out, err = self._exec_remote(f"ls {SERVER_ZIP_DIR}/*.zip 2>/dev/null || true")
            if out:
                for line in out.splitlines():
                    name = Path(line.strip()).name
                    if name.startswith("processed_"):
                        server_zip_originals.add(name[len("processed_"):])
                    else:
                        server_zip_originals.add(name)
            
            # 获取已处理完成的目录（仅检查最终目录）
            processed_dirs = set()
            status, out, err = self._exec_remote(f"ls -d {SERVER_FINAL_DIR}/*/ 2>/dev/null || true")
            if out:
                for line in out.splitlines():
                    dir_name = Path(line.strip().rstrip('/')).name
                    processed_dirs.add(dir_name)
            
            print(f"  📊 服务器状态: {len(server_zip_originals)} ZIPs / {len(processed_dirs)} 已完成")
            
            # 自动获取 Token
            auth_token = self._get_dataweave_token()
            
            # 关闭主连接，让每个线程创建自己的连接
            self._close_server()
            
            # 过滤需要处理的文件
            files_to_process = []
            skipped_count = 0
            for i, json_file in enumerate(json_files):
                stem = json_file.stem
                if stem in processed_dirs:
                    skipped_count += 1
                    with results_lock:
                        self.results['check_passed'].append(stem)
                else:
                    files_to_process.append((i, json_file, stem))
            
            if skipped_count > 0:
                print(f"  ⏭ 跳过已完成: {skipped_count} 个")
            
            if not files_to_process:
                print("  ✓ 所有文件都已处理完成")
                return
            
            print(f"  📦 待处理: {len(files_to_process)} 个文件")
            print(f"  🧵 线程数: {num_workers}")
            print()
            
            # 创建进度追踪器
            progress = ProgressTracker(len(files_to_process), "并行处理")
            
            # 使用线程池并行处理
            with ThreadPoolExecutor(max_workers=num_workers, thread_name_prefix='Worker') as executor:
                futures = {}
                for i, json_file, stem in files_to_process:
                    future = executor.submit(
                        self._process_single_file_threaded,
                        i + 1,
                        len(json_files),
                        json_file,
                        stem,
                        server_zip_originals,
                        auth_token
                    )
                    futures[future] = stem
                
                # 等待所有任务完成，更新进度
                for future in as_completed(futures):
                    stem = futures[future]
                    try:
                        result = future.result()
                        progress.update(success=result, name=stem)
                    except Exception as e:
                        progress.update(success=False, name=f"{stem} (异常)")
            
            # 显示汇总
            progress.summary()
        
        finally:
            self._close_server()
        
        # 输出详细汇总
        self._print_summary()
    
    def _log_error(self, stem: str, step: str, error_msg: str):
        """记录错误信息用于追溯"""
        with self.errors_lock:
            if stem not in self.errors:
                self.errors[stem] = []
            self.errors[stem].append((step, error_msg))
    
    def _process_single_file_threaded(self, idx: int, total: int, json_file: Path, stem: str, 
                                       server_zip_originals: set, auth_token: str) -> bool:
        """
        线程安全的单文件处理函数 (静默模式，不输出日志)
        失败时记录错误到 self.errors 以便追溯
        支持 SSH 断线重连和 Token 自动刷新
        """
        import paramiko
        import requests
        import time
        
        zip_name = f"{stem}.zip"
        local_zip = self.local_zip_dir / zip_name
        remote_zip = f"{SERVER_ZIP_DIR}/{zip_name}"
        
        max_ssh_retries = 3
        
        def connect_ssh():
            """创建 SSH 连接，支持重试"""
            for attempt in range(max_ssh_retries):
                try:
                    _ssh = paramiko.SSHClient()
                    _ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
                    _ssh.connect(SERVER_IP, username=SERVER_USER, timeout=30)
                    _sftp = _ssh.open_sftp()
                    return _ssh, _sftp
                except Exception as e:
                    if attempt < max_ssh_retries - 1:
                        time.sleep(2 * (attempt + 1))
                        continue
                    raise e
            return None, None
        
        ssh = None
        sftp = None
        
        try:
            ssh, sftp = connect_ssh()
            if not ssh:
                self._log_error(stem, "连接", "无法建立 SSH 连接")
                return False
            
            # ===== 步骤 1: 下载 =====
            need_download = True
            if zip_name in server_zip_originals:
                with results_lock:
                    self.results['skipped_server_exists'].append(stem)
                need_download = False
            elif self._is_valid_zip(local_zip):
                need_download = False
            
            if need_download:
                # 获取最新的 Token (会自动刷新过期的 Token)
                current_token = self._get_dataweave_token()
                headers = {
                    "User-Agent": "Mozilla/5.0",
                    "Content-Type": "application/json",
                    "Authorization": current_token,
                }
                downloaded = self._download_single_zip(stem, zip_name, local_zip, headers)
                if not downloaded:
                    self._log_error(stem, "下载", "下载失败，文件在DataWeave中不存在或网络问题")
                    return False
                with results_lock:
                    self.results['downloaded'].append(stem)
            
            # ===== 步骤 2: 上传 =====
            if zip_name not in server_zip_originals and local_zip.exists():
                upload_ok = False
                for upload_attempt in range(3):
                    try:
                        sftp.put(str(local_zip), remote_zip)
                        upload_ok = True
                        break
                    except Exception as e:
                        if upload_attempt < 2:
                            # 尝试重连
                            try:
                                if sftp: sftp.close()
                                if ssh: ssh.close()
                            except: pass
                            import time
                            time.sleep(2)
                            ssh, sftp = connect_ssh()
                            if not ssh:
                                break
                        else:
                            self._log_error(stem, "上传", f"上传到服务器失败: {e}")
                            return False
                if not upload_ok:
                    self._log_error(stem, "上传", "上传失败，无法建立连接")
                    return False
                with results_lock:
                    self.results['uploaded'].append(stem)
            
            # ===== 步骤 3: 服务器处理 =====
            remote_json_temp = f"/tmp/{json_file.name}"
            try:
                sftp.put(str(json_file), remote_json_temp)
            except Exception as e:
                self._log_error(stem, "上传JSON", f"上传JSON文件失败: {e}")
                return False
            
            cmd = f"python3 /tmp/zip_worker.py --zip '{remote_zip}' --json '{remote_json_temp}' --out '{SERVER_PROCESS_DIR}' --rename_json '{RENAME_JSON}'"
            status, _, err_output = self._exec_remote_thread(ssh, cmd, timeout=300)
            
            if status != 0:
                self._log_error(stem, "服务器处理", f"处理脚本返回错误码 {status}: {err_output[:200]}")
                return False
            
            # 处理原始 ZIP
            if ZIP_AFTER_PROCESS == "rename":
                new_name = f"{SERVER_ZIP_DIR}/processed_{zip_name}"
                ssh.exec_command(f"mv '{remote_zip}' '{new_name}'")
            elif ZIP_AFTER_PROCESS == "delete":
                ssh.exec_command(f"rm '{remote_zip}'")
            
            with results_lock:
                self.results['processed'].append(stem)
            
            # ===== 步骤 4: 检查 =====
            remote_data_dir = f"{SERVER_PROCESS_DIR}/{stem}"
            report_path = f"/tmp/report_{stem}.txt"
            cmd = f"python3 /tmp/annotation_checker.py --data_dir '{remote_data_dir}' --config '/tmp/check_config.yaml' --report '{report_path}'"
            
            status, _, check_err = self._exec_remote_thread(ssh, cmd, timeout=120)
            
            check_passed = True
            local_report = self.local_check_dir / f"report_{stem}.txt"
            
            if status == 0:
                try:
                    sftp.get(report_path, str(local_report))
                    report_content = local_report.read_text()
                    issue_count = report_content.count("帧:")
                    if issue_count > 0:
                        check_passed = False
                        self._log_error(stem, "检查", f"发现 {issue_count} 个问题帧，详见报告: {local_report}")
                    else:
                        # 检查通过，删除本地报告
                        if local_report.exists():
                            local_report.unlink()
                except Exception as e:
                    self._log_error(stem, "检查", f"获取报告失败: {e}")
            else:
                check_passed = False
                self._log_error(stem, "检查", f"检查脚本执行失败，错误码 {status}: {check_err[:200]}")
            
            if check_passed:
                with results_lock:
                    self.results['check_passed'].append(stem)
                
                # ===== 步骤 5: 移动 =====
                src = f"{SERVER_PROCESS_DIR}/{stem}"
                dst = f"{SERVER_FINAL_DIR}/{stem}"
                
                # 安全移动：先检查目标是否存在，存在则备份而不是直接删除
                status, out, _ = self._exec_remote_thread(ssh, f"test -d '{dst}' && echo exists")
                if out.strip() == 'exists':
                    # 目标已存在，先备份旧数据
                    backup_dst = f"{dst}.bak.{datetime.now().strftime('%Y%m%d%H%M%S')}"
                    ssh.exec_command(f"mv '{dst}' '{backup_dst}'")
                
                status, _, move_err = self._exec_remote_thread(ssh, f"mv '{src}' '{dst}'")
                
                if status == 0:
                    with results_lock:
                        self.results['moved_to_final'].append(stem)
                    # 流水线成功完成，删除本地 ZIP 文件
                    if local_zip.exists():
                        local_zip.unlink()
                else:
                    self._log_error(stem, "移动", f"移动到最终目录失败: {move_err}")
            else:
                with results_lock:
                    self.results['check_failed'].append(stem)
            
            return check_passed
            
        except Exception as e:
            self._log_error(stem, "异常", f"{type(e).__name__}: {str(e)}")
            return False
        
        finally:
            try:
                if sftp:
                    sftp.close()
                if ssh:
                    ssh.close()
            except:
                pass

    def _download_single_zip(self, stem: str, zip_name: str, target_file: Path, headers: dict, 
                              retry_token: bool = True) -> bool:
        """下载单个 ZIP 文件，使用临时文件避免下载中断导致的不完整文件
        
        支持 Token 过期自动刷新和下载重试
        """
        import requests
        import time
        
        # 临时文件路径
        temp_file = target_file.with_suffix('.zip.tmp')
        
        max_retries = 3
        
        for attempt in range(max_retries):
            try:
                # 在多个路径模板中查找文件
                real_url = None
                found_path = None
                token_expired = False
                
                for path_template in DATAWEAVE_PATH_TEMPLATES:
                    dw_path = path_template.format(filename=zip_name)
                    payload = {"uris": [dw_path]}
                    
                    r = requests.post(API_URL, json=payload, headers=headers, timeout=15)
                    r.raise_for_status()
                    data = r.json()
                    
                    if data.get("code") != 0:
                        msg = data.get("msg", "")
                        if "Login required" in msg or data.get("code") == 401:
                            token_expired = True
                            break
                        continue
                    
                    url_data = data.get("data", {})
                    if isinstance(url_data, dict) and "urls" in url_data:
                        urls_list = url_data["urls"]
                        if urls_list and isinstance(urls_list[0], dict):
                            url = urls_list[0].get("url")
                            if url:
                                real_url = url
                                found_path = path_template.split("/")[-2]
                                break
                
                # Token 过期，尝试刷新
                if token_expired:
                    if retry_token and attempt < max_retries - 1:
                        logger.critical("!!! Token 已过期 !!!")
                        new_token = self._get_dataweave_token(force_refresh=True)
                        headers = dict(headers)
                        headers["Authorization"] = new_token
                        continue
                    else:
                        return False
                
                if not real_url:
                    # 文件不存在，无需重试
                    return False
                
                logger.info(f"    找到文件，路径: {found_path}")
                
                # 下载到临时文件 (增加超时)
                download_headers = {"User-Agent": headers["User-Agent"]}
                with requests.get(real_url, headers=download_headers, stream=True, timeout=600) as r:
                    r.raise_for_status()
                    total_size = int(r.headers.get('content-length', 0))
                    with open(temp_file, 'wb') as f:
                        downloaded = 0
                        for chunk in r.iter_content(chunk_size=8192):
                            if chunk:
                                f.write(chunk)
                                downloaded += len(chunk)
                                if total_size > 0:
                                    percent = (downloaded / total_size) * 100
                                    sys.stdout.write(f"\r    下载进度: {percent:.1f}%")
                                    sys.stdout.flush()
                    print()
                
                # 验证下载完整性
                if total_size > 0:
                    actual_size = temp_file.stat().st_size
                    if actual_size != total_size:
                        logger.error(f"    下载不完整: 预期 {total_size} 字节，实际 {actual_size} 字节")
                        if temp_file.exists():
                            temp_file.unlink()
                        if attempt < max_retries - 1:
                            logger.info(f"    重试下载 ({attempt + 2}/{max_retries})...")
                            time.sleep(2)
                            continue
                        return False
                
                # 下载完成，重命名为正式文件
                if target_file.exists():
                    target_file.unlink()
                temp_file.rename(target_file)
                
                return True
                
            except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as e:
                logger.warning(f"    网络错误: {e}")
                if temp_file.exists():
                    temp_file.unlink()
                if attempt < max_retries - 1:
                    logger.info(f"    重试下载 ({attempt + 2}/{max_retries})...")
                    time.sleep(2 * (attempt + 1))
                    continue
                return False
            except Exception as e:
                logger.error(f"    下载失败: {e}")
                if temp_file.exists():
                    temp_file.unlink()
                return False
        
        return False
    
    # ==================== 运行流水线 ====================
    def run(self, steps: list = None):
        """运行流水线"""
        all_steps = ['download', 'upload', 'process', 'check', 'move']
        
        if steps is None or 'all' in steps:
            steps = all_steps
        
        logger.info("=" * 60)
        logger.info("标注数据自动化处理流水线")
        logger.info(f"JSON 目录: {self.json_dir}")
        logger.info(f"执行步骤: {', '.join(steps)}")
        logger.info("=" * 60)
        
        try:
            if 'download' in steps:
                self.step1_download_zips()
            
            if 'upload' in steps:
                self.step2_upload_zips()
            
            if 'process' in steps:
                self.step3_process_on_server()
            
            if 'check' in steps:
                self.step4_check_annotations()
            
            if 'move' in steps:
                self.step5_move_to_final()
            
        finally:
            self._close_server()
        
        # 输出汇总
        self._print_summary()
    
    def _print_summary(self):
        """打印执行汇总"""
        print()
        print("╔" + "═" * 50 + "╗")
        print("║  📊 执行汇总".ljust(51) + "║")
        print("╠" + "═" * 50 + "╣")
        
        stats = [
            ("⏭ 跳过(已存在)", len(self.results['skipped_server_exists'])),
            ("⬇ 下载成功", len(self.results['downloaded'])),
            ("⬆ 上传成功", len(self.results['uploaded'])),
            ("⚙ 处理成功", len(self.results['processed'])),
            ("✓ 检查通过", len(self.results['check_passed'])),
            ("✗ 检查失败", len(self.results['check_failed'])),
            ("📁 已移动", len(self.results['moved_to_final'])),
        ]
        
        for label, count in stats:
            line = f"║  {label}: {count}"
            print(line.ljust(51) + "║")
        
        print("╚" + "═" * 50 + "╝")
        
        if self.results['check_failed']:
            print()
            print("  ⚠ 检查未通过的数据:")
            for name in self.results['check_failed']:
                report = self.local_check_dir / f"report_{name}.txt"
                print(f"    • {name}")
                if report.exists():
                    print(f"      报告: {report}")
        
        # 显示错误追溯信息
        if self.errors:
            print()
            print("  ❌ 失败详情 (可追溯):")
            for stem, error_list in self.errors.items():
                print(f"    ┌─ {stem}")
                for step, msg in error_list:
                    # 截断过长的错误信息
                    display_msg = msg[:80] + "..." if len(msg) > 80 else msg
                    print(f"    │  [{step}] {display_msg}")
                print(f"    └─")


def main():
    parser = argparse.ArgumentParser(description="标注数据自动化处理流水线")
    parser.add_argument('--json_dir', type=str, required=True,
                        help='本地 JSON 文件夹路径')
    parser.add_argument('--zip_dir', type=str, default=None,
                        help='本地 ZIP 文件存储路径 (可选，默认使用临时目录)')
    parser.add_argument('--step', type=str, nargs='+', 
                        default=['all'],
                        choices=['all', 'download', 'upload', 'process', 'check', 'move'],
                        help='执行的步骤 (批量模式)')
    parser.add_argument('--streaming', '-s', action='store_true',
                        help='流式处理模式: 下载一个文件就立即处理，无需等待全部下载完成')
    parser.add_argument('--parallel', '-p', action='store_true',
                        help='多线程并行模式: 多个文件同时下载和处理 (推荐)')
    parser.add_argument('--workers', '-w', type=int, default=None,
                        help=f'并行线程数 (默认 {MAX_WORKERS})')
    
    args = parser.parse_args()
    
    if not Path(args.json_dir).exists():
        logger.error(f"JSON 目录不存在: {args.json_dir}")
        return
    
    pipeline = AnnotationPipeline(args.json_dir, args.zip_dir)
    
    if args.parallel:
        # 多线程并行模式
        pipeline.run_parallel(args.workers)
    elif args.streaming:
        # 流式处理模式
        pipeline.run_streaming()
    else:
        # 批量处理模式
        pipeline.run(args.step)


if __name__ == "__main__":
    main()
