import os
import paramiko
import logging
from pathlib import Path
import sys

# ================= 配置区域 =================
# 本地存放 JSON 文件的目录
LOCAL_JSON_DIR = "/home/zgw/Downloads/12.30导出/线段/"

# 远程服务器配置
SERVER_IP = "222.223.112.212"
SERVER_USER = "user"
# 远程服务器基础路径 (在此路径下寻找与 JSON 文件名匹配的文件夹)
SERVER_BASE_DIR = "/data01/seg_dataset/jsons"

# 目标一级子目录名称 (例如: "annotations" 或 "json_data")
# 如果为空字符串 ""，则直接放在匹配的文件夹根目录下
TARGET_SUBDIR_NAME = "" 
# ===========================================

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger(__name__)

def upload_json_files():
    local_path = Path(LOCAL_JSON_DIR)
    
    if not local_path.exists():
        logger.error(f"本地目录不存在: {LOCAL_JSON_DIR}")
        return

    # 获取所有 .json 文件
    json_files = list(local_path.glob("*.json"))
    if not json_files:
        logger.warning(f"在 {LOCAL_JSON_DIR} 未找到 .json 文件")
        return

    logger.info(f"找到 {len(json_files)} 个 JSON 文件，准备处理...")

    ssh = None
    sftp = None

    try:
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        
        logger.info(f"正在连接服务器 {SERVER_IP}...")
        # 如果有密钥或密码，请在此处添加 password='...' 或 key_filename='...'
        ssh.connect(SERVER_IP, username=SERVER_USER, timeout=10)
        sftp = ssh.open_sftp()
        
        success_count = 0
        skip_count = 0
        
        for json_file in json_files:
            # 获取文件名（不含扩展名），用于匹配远程文件夹
            folder_name = json_file.stem
            
            # 构建远程匹配文件夹路径
            remote_matched_dir = os.path.join(SERVER_BASE_DIR, folder_name)
            
            # 检查远程匹配文件夹是否存在
            try:
                sftp.stat(remote_matched_dir)
            except FileNotFoundError:
                logger.warning(f"跳过: 远程未找到匹配文件夹 {remote_matched_dir} (对应文件: {json_file.name})")
                skip_count += 1
                continue
            
            # 构建最终目标路径 (包含一级子目录)
            if TARGET_SUBDIR_NAME:
                remote_target_dir = os.path.join(remote_matched_dir, TARGET_SUBDIR_NAME)
                # 检查子目录是否存在，不存在则创建
                try:
                    sftp.stat(remote_target_dir)
                except FileNotFoundError:
                    try:
                        sftp.mkdir(remote_target_dir)
                        logger.info(f"创建远程子目录: {remote_target_dir}")
                    except OSError as e:
                        logger.error(f"无法创建远程子目录 {remote_target_dir}: {e}")
                        skip_count += 1
                        continue
            else:
                remote_target_dir = remote_matched_dir
            
            remote_file_path = os.path.join(remote_target_dir, json_file.name)
            
            try:
                sftp.put(str(json_file), remote_file_path)
                logger.info(f"成功上传: {json_file.name} -> {remote_file_path}")
                success_count += 1
            except Exception as e:
                logger.error(f"上传失败 {json_file.name}: {e}")
                skip_count += 1

        logger.info(f"处理完成。成功: {success_count}, 跳过/失败: {skip_count}")

    except Exception as e:
        logger.error(f"连接或执行过程中发生错误: {e}")
    finally:
        if sftp: sftp.close()
        if ssh: ssh.close()

if __name__ == "__main__":
    upload_json_files()
