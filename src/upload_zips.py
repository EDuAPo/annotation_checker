"""
功能说明：
此脚本用于将本地指定目录下的所有 ZIP 文件批量上传到远程服务器的指定目录。
支持自动创建远程目录、显示上传进度以及跳过已存在且大小一致的文件。
"""

import os
import paramiko
import logging
from pathlib import Path
import sys

# ================= 配置区域 =================
# 本地存放 ZIP 文件的目录 (请修改为你的实际路径)
LOCAL_ZIP_DIR = "/media/zgw/T7/from_rere/1230_lines/"

# 远程服务器配置
SERVER_IP = "222.223.112.212"
SERVER_USER = "user"
# 远程服务器目标路径
SERVER_TARGET_DIR = "/data01/rere_zips"
# ===========================================

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger(__name__)

def upload_files():
    local_path = Path(LOCAL_ZIP_DIR)
    
    if not local_path.exists():
        logger.error(f"本地目录不存在: {LOCAL_ZIP_DIR}")
        return

    # 获取所有 .zip 文件
    zip_files = list(local_path.glob("*.zip"))
    if not zip_files:
        logger.warning(f"在 {LOCAL_ZIP_DIR} 未找到 .zip 文件")
        return

    logger.info(f"找到 {len(zip_files)} 个 ZIP 文件，准备上传到 {SERVER_IP}:{SERVER_TARGET_DIR}")

    ssh = None
    sftp = None

    try:
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        
        logger.info(f"正在连接服务器 {SERVER_IP}...")
        ssh.connect(SERVER_IP, username=SERVER_USER, timeout=10)
        sftp = ssh.open_sftp()
        
        # 确保远程目录存在
        logger.info(f"检查并创建远程目录: {SERVER_TARGET_DIR}")
        stdin, stdout, stderr = ssh.exec_command(f"mkdir -p {SERVER_TARGET_DIR}")
        exit_status = stdout.channel.recv_exit_status()
        if exit_status != 0:
            logger.error(f"无法创建远程目录: {stderr.read().decode()}")
            return

        # 开始上传
        for i, zip_file in enumerate(zip_files):
            remote_path = f"{SERVER_TARGET_DIR}/{zip_file.name}"
            file_size = zip_file.stat().st_size
            file_size_mb = file_size / (1024 * 1024)
            
            logger.info(f"[{i+1}/{len(zip_files)}] 上传: {zip_file.name} ({file_size_mb:.1f} MB)")
            
            # 进度回调
            def progress(transferred, total):
                percent = (transferred / total) * 100
                sys.stdout.write(f"\r  -> 进度: {percent:.1f}%")
                sys.stdout.flush()

            try:
                # 检查远程文件是否已存在且大小一致（可选，简单的断点续传检查）
                try:
                    remote_stat = sftp.stat(remote_path)
                    if remote_stat.st_size == file_size:
                        print()
                        logger.info(f"  -> 文件已存在且大小一致，跳过")
                        continue
                except FileNotFoundError:
                    pass

                sftp.put(str(zip_file), remote_path, callback=progress)
                print() # 换行
            except Exception as e:
                print()
                logger.error(f"  -> 上传失败: {e}")
                continue

        logger.info("所有文件上传完成")

    except Exception as e:
        logger.error(f"发生错误: {e}")
    finally:
        if sftp: sftp.close()
        if ssh: ssh.close()

if __name__ == "__main__":
    # 运行前请确保已安装 paramiko: pip install paramiko
    upload_files()
