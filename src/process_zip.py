"""
功能说明：
此脚本用于自动化处理远程服务器上的 ZIP 数据包。
主要流程：
1. 连接远程服务器，扫描待处理的 ZIP 文件。
2. 在本地查找与 ZIP 文件名匹配的 JSON 标注文件，并上传到服务器。
3. 在服务器上解压 ZIP，并将上传的 JSON 文件整合进去。
4. 根据配置决定是否将 JSON文件 重命名为 annotations.json。
5. 将整理好的数据移动到目标输出目录，并标记原 ZIP 为已处理。
"""

import os
import paramiko
import logging
from pathlib import Path
import time

# ================= 配置区域 =================
# 本地 JSON 文件夹路径
LOCAL_JSON_DIR = "/home/zgw/Downloads/12.30导出/线段/"

# 远程服务器配置
SERVER_IP = "222.223.112.212"
SERVER_USER = "user"
# 远程服务器上存放 ZIP 文件的目录
SERVER_ZIP_DIR = "/data01/rere_zips"
# 远程服务器上处理结果的输出目录 (通常和 ZIP 目录一样，或者不同)
SERVER_OUTPUT_DIR = "/data01/seg_dataset/jsons"

# 是否将上传的 JSON 文件重命名为 annotations.json
# True: 重命名为 annotations.json (默认)
# False: 保持原文件名
RENAME_JSON = False

# ===========================================

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger(__name__)

# 嵌入的远程工作脚本代码
# 这个脚本会被上传到服务器并执行
REMOTE_WORKER_CODE = """
import os
import sys
import shutil
import zipfile
import logging
import argparse
from pathlib import Path

logging.basicConfig(level=logging.INFO, format='[Remote] %(message)s')
logger = logging.getLogger(__name__)

def find_common_directory_for_all_items(extract_dir):
    other_required_items = [
        "camera_cam_3M_front", "camera_cam_3M_left", "camera_cam_3M_right", 
        "camera_cam_3M_rear", "camera_cam_8M_wa_front", "combined_scales", 
        "ins.json", "iv_points_front_left", "iv_points_front_mid", 
        "iv_points_front_right", "iv_points_rear_left", "iv_points_rear_right", 
        "sensor_config_combined_latest.json", "sample.json"
    ]
    
    directory_counts = {}
    for root, dirs, files in os.walk(extract_dir):
        current_dir = Path(root)
        count = 0
        for name in dirs + files:
            if name in other_required_items:
                count += 1
        if count > 0:
            directory_counts[current_dir] = count
            
    if not directory_counts:
        return None
    return max(directory_counts.items(), key=lambda x: x[1])[0]

def cleanup_and_organize(extract_dir, common_dir, target_dir, json_path, rename_json):
    # 1. 创建目标目录
    target_dir.mkdir(parents=True, exist_ok=True)
    
    # 2. 移动 JSON 并重命名
    if rename_json:
        target_json_name = "annotations.json"
    else:
        target_json_name = json_path.name
        
    shutil.move(str(json_path), str(target_dir / target_json_name))
    
    # 3. 移动必需项目
    keep_items = [
        "annotations.json", "sample.json", "ins.json", "sensor_config_combined_latest.json",
        "combined_scales", "camera_cam_3M_front", "camera_cam_3M_left", 
        "camera_cam_3M_right", "camera_cam_3M_rear", "camera_cam_8M_wa_front", 
        "iv_points_front_left", "iv_points_front_mid", "iv_points_front_right", 
        "iv_points_rear_left", "iv_points_rear_right"
    ]
    
    # 如果不重命名，需要确保原文件名也在保留列表中，否则会被清理掉
    if not rename_json and target_json_name not in keep_items:
        keep_items.append(target_json_name)
    
    # 从 common_dir 移动到 target_dir
    # 注意：如果 common_dir 就是 target_dir (原地操作)，则不需要移动，只需要删除多余的
    # 但为了安全，我们通常建议解压到 temp，然后移动到 final
    
    # 这里简化逻辑：从 common_dir 移动所有在白名单里的东西到 target_dir
    for item_name in keep_items:
        if item_name == target_json_name: continue # 已经处理过
        
        src = common_dir / item_name
        dst = target_dir / item_name
        
        if src.exists():
            if dst.exists():
                if dst.is_dir(): shutil.rmtree(dst)
                else: dst.unlink()
            shutil.move(str(src), str(dst))
        else:
            # 尝试在子文件夹搜索 (兼容旧逻辑)
            pass 

    # 4. 再次检查并清理 target_dir 中多余的文件
    for item in target_dir.iterdir():
        if item.name not in keep_items:
            if item.is_dir(): shutil.rmtree(item)
            else: item.unlink()

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--zip", required=True)
    parser.add_argument("--json", required=True)
    parser.add_argument("--out", required=True)
    parser.add_argument("--rename_json", type=str, default="True")
    args = parser.parse_args()
    
    zip_path = Path(args.zip)
    json_path = Path(args.json)
    output_root = Path(args.out)
    do_rename = args.rename_json.lower() == "true"
    
    zip_stem = zip_path.stem
    final_output_dir = output_root / zip_stem
    
    # 临时解压目录
    temp_extract_dir = output_root / f"temp_{zip_stem}"
    if temp_extract_dir.exists():
        shutil.rmtree(temp_extract_dir)
    temp_extract_dir.mkdir(parents=True, exist_ok=True)
    
    try:
        logger.info(f"解压 {zip_path.name} ...")
        with zipfile.ZipFile(zip_path, 'r') as zf:
            zf.extractall(temp_extract_dir)
            
        logger.info("寻找数据根目录...")
        common_dir = find_common_directory_for_all_items(temp_extract_dir)
        if not common_dir:
            raise Exception("未找到包含必需文件的目录")
            
        logger.info(f"清洗并移动数据到 {final_output_dir} ...")
        cleanup_and_organize(temp_extract_dir, common_dir, final_output_dir, json_path, do_rename)
        
        logger.info("完成。")
        
    except Exception as e:
        logger.error(f"错误: {str(e)}")
        sys.exit(1)
    finally:
        # 清理临时解压目录
        if temp_extract_dir.exists():
            shutil.rmtree(temp_extract_dir)

if __name__ == "__main__":
    main()
"""

class RemoteOrchestrator:
    def __init__(self):
        self.ssh = None
        self.sftp = None
        self.local_json_dir = Path(LOCAL_JSON_DIR)
        
    def connect(self):
        try:
            self.ssh = paramiko.SSHClient()
            self.ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            logger.info(f"正在连接服务器 {SERVER_IP}...")
            self.ssh.connect(SERVER_IP, username=SERVER_USER, timeout=10)
            self.sftp = self.ssh.open_sftp()
            logger.info("服务器连接成功")
            return True
        except Exception as e:
            logger.error(f"连接失败: {e}")
            return False

    def deploy_worker_script(self):
        """将 worker 脚本写入服务器临时文件"""
        try:
            with self.sftp.file("/tmp/zip_worker.py", "w") as f:
                f.write(REMOTE_WORKER_CODE)
            logger.info("已部署远程工作脚本")
        except Exception as e:
            logger.error(f"部署脚本失败: {e}")
            raise

    def get_remote_zips(self):
        """获取远程未处理的 ZIP 文件列表"""
        # 查找所有 .zip
        stdin, stdout, stderr = self.ssh.exec_command(f"ls {SERVER_ZIP_DIR}/*.zip")
        files = stdout.read().decode().splitlines()
        
        zips = []
        for f in files:
            path = Path(f.strip())
            # 排除已处理的 (以 processed_ 开头)
            if not path.name.startswith("processed_"):
                zips.append(path)
        return zips

    def find_local_json(self, zip_stem):
        """在本地查找对应的 JSON"""
        # 1. 精确匹配
        exact = self.local_json_dir / f"{zip_stem}.json"
        if exact.exists():
            return exact
        
        # 2. 模糊匹配
        for f in self.local_json_dir.glob("*.json"):
            if zip_stem in f.stem:
                return f
        return None

    def process(self):
        if not self.connect():
            return

        self.deploy_worker_script()
        
        remote_zips = self.get_remote_zips()
        logger.info(f"服务器上发现 {len(remote_zips)} 个待处理 ZIP 文件")
        
        for i, zip_path in enumerate(remote_zips):
            zip_name = zip_path.name
            zip_stem = zip_path.stem
            logger.info(f"[{i+1}/{len(remote_zips)}] 处理: {zip_name}")
            
            # 1. 查找本地 JSON
            local_json = self.find_local_json(zip_stem)
            if not local_json:
                logger.warning(f"  -> 跳过: 本地未找到对应的 JSON 文件")
                continue
            
            # 2. 上传 JSON 到服务器临时位置
            remote_json_temp = f"/tmp/{local_json.name}"
            try:
                self.sftp.put(str(local_json), remote_json_temp)
                logger.info(f"  -> 已上传 JSON: {local_json.name}")
            except Exception as e:
                logger.error(f"  -> 上传 JSON 失败: {e}")
                continue
                
            # 3. 执行远程处理
            # python3 /tmp/zip_worker.py --zip <zip> --json <json> --out <out_dir> --rename_json <True/False>
            cmd = f"python3 /tmp/zip_worker.py --zip '{zip_path}' --json '{remote_json_temp}' --out '{SERVER_OUTPUT_DIR}' --rename_json '{RENAME_JSON}'"
            
            stdin, stdout, stderr = self.ssh.exec_command(cmd)
            
            # 实时打印远程输出
            exit_status = stdout.channel.recv_exit_status()
            out_log = stdout.read().decode().strip()
            err_log = stderr.read().decode().strip()
            
            if exit_status == 0:
                logger.info(f"  -> 远程处理成功")
                # 4. 重命名原始 ZIP 以标记为已处理 (避免重复处理)
                # 也可以选择删除: rm_cmd = f"rm '{zip_path}'"
                new_name = zip_path.parent / f"processed_{zip_name}"
                rename_cmd = f"mv '{zip_path}' '{new_name}'"
                self.ssh.exec_command(rename_cmd)
                logger.info(f"  -> 原始文件已重命名为: processed_{zip_name}")
            else:
                logger.error(f"  -> 远程处理失败")
                logger.error(f"  -> 错误日志: {err_log}")
                if out_log: logger.error(f"  -> 输出日志: {out_log}")

        self.ssh.close()
        logger.info("所有任务完成")

if __name__ == "__main__":
    # 确保本地 JSON 目录存在
    if not Path(LOCAL_JSON_DIR).exists():
        logger.error(f"本地 JSON 目录不存在: {LOCAL_JSON_DIR}")
    else:
        orchestrator = RemoteOrchestrator()
        orchestrator.process()
