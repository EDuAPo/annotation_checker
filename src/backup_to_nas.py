"""
本地数据备份到群晖NAS脚本
==========================

将本地指定文件夹的数据备份到群晖NAS。

使用方法：
    python backup_to_nas.py --source /path/to/source
"""

import os
import sys
import logging
import argparse
from pathlib import Path

# 添加项目路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# NAS 配置
NAS_IP = "192.168.2.41"
NAS_SHARE = "public"  # 共享名
NAS_SUBDIR = "from_rere"  # 子目录
NAS_USER = "SYSC"
NAS_PASS = "Nas123456"
NAS_MOUNT_POINT = "/mnt/nas_backup"

# 日志配置
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

class NASBackup:
    """NAS备份工具"""
    
    def __init__(self, source_dir: str):
        self.source_dir = Path(source_dir)
        if not self.source_dir.exists():
            raise ValueError(f"源目录不存在: {self.source_dir}")
    
    def backup_to_nas(self):
        """执行备份"""
        import subprocess
        
        try:
            logger.info("开始备份本地数据到群晖NAS...")
            
            # 1. 创建挂载点
            logger.info("创建NAS挂载点...")
            result = subprocess.run(["sudo", "mkdir", "-p", NAS_MOUNT_POINT], 
                                  capture_output=True, text=True)
            if result.returncode != 0:
                logger.error(f"创建挂载点失败: {result.stderr}")
                return False
            
            # 2. 挂载NAS
            logger.info("挂载NAS共享...")
            mount_cmd = ["sudo", "mount", "-t", "cifs", 
                        f"//{NAS_IP}/{NAS_SHARE}", NAS_MOUNT_POINT, 
                        "-o", f"username={NAS_USER},password={NAS_PASS},vers=3.0"]
            result = subprocess.run(mount_cmd, capture_output=True, text=True, timeout=30)
            if result.returncode != 0:
                logger.error(f"挂载NAS失败: {result.stderr}")
                # 尝试其他版本
                mount_cmd[7] = f"username={NAS_USER},password={NAS_PASS},vers=1.0"
                result = subprocess.run(mount_cmd, capture_output=True, text=True, timeout=30)
                if result.returncode != 0:
                    logger.error(f"挂载NAS失败 (vers=1.0): {result.stderr}")
                    return False
            
            # 3. 检查挂载是否成功
            result = subprocess.run(["mountpoint", NAS_MOUNT_POINT], 
                                  capture_output=True, text=True)
            if result.returncode != 0:
                logger.error(f"NAS挂载检查失败: {result.stderr}")
                return False
            
            # 4. 创建备份子目录
            logger.info(f"创建备份子目录 {NAS_SUBDIR}...")
            result = subprocess.run(["sudo", "mkdir", "-p", f"{NAS_MOUNT_POINT}/{NAS_SUBDIR}"], 
                                  capture_output=True, text=True)
            if result.returncode != 0:
                logger.error(f"创建子目录失败: {result.stderr}")
                return False
            
            # 5. 执行rsync备份
            logger.info(f"开始rsync备份 {self.source_dir} -> {NAS_MOUNT_POINT}/{NAS_SUBDIR}")
            rsync_cmd = ["sudo", "rsync", "-av", "--delete", 
                        str(self.source_dir) + "/", f"{NAS_MOUNT_POINT}/{NAS_SUBDIR}/"]
            result = subprocess.run(rsync_cmd, capture_output=True, text=True)
            if result.returncode != 0:
                logger.error(f"rsync备份失败: {result.stderr}")
                return False
            
            logger.info("备份完成！")
            
            # 6. 卸载NAS
            logger.info("卸载NAS...")
            result = subprocess.run(["sudo", "umount", NAS_MOUNT_POINT], 
                                  capture_output=True, text=True)
            if result.returncode != 0:
                logger.warning(f"卸载NAS失败: {result.stderr}")
            
            return True
            
        except subprocess.TimeoutExpired:
            logger.error("挂载操作超时")
            return False
        except Exception as e:
            logger.error(f"备份过程中出错: {e}")
            return False

def main():
    """主函数"""
    parser = argparse.ArgumentParser(description="备份本地数据到群晖NAS")
    parser.add_argument("--source", required=True, help="要备份的本地源目录")
    parser.add_argument("--dry-run", action="store_true", help="仅显示将执行的命令，不实际执行")
    args = parser.parse_args()
    
    try:
        backup = NASBackup(args.source)
    except ValueError as e:
        logger.error(str(e))
        sys.exit(1)
    
    if args.dry_run:
        logger.info("干运行模式 - 显示将执行的命令:")
        logger.info(f"1. mkdir -p {NAS_MOUNT_POINT}")
        logger.info(f"2. mount -t cifs //{NAS_IP}/{NAS_SHARE} {NAS_MOUNT_POINT} -o username={NAS_USER},password=***")
        logger.info(f"3. mkdir -p {NAS_MOUNT_POINT}/{NAS_SUBDIR}")
        logger.info(f"4. rsync -av --delete {args.source}/ {NAS_MOUNT_POINT}/{NAS_SUBDIR}/")
        logger.info(f"5. umount {NAS_MOUNT_POINT}")
        return
    
    success = backup.backup_to_nas()
    if success:
        logger.info("✓ 备份成功完成")
        sys.exit(0)
    else:
        logger.error("✗ 备份失败")
        sys.exit(1)

if __name__ == "__main__":
    main()