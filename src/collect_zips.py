"""
功能说明：
此脚本用于搜索给定路径（包括二级子目录）下的 ZIP 文件，
并复制/移动到指定的目标文件夹中。

使用方法：
1. 修改配置区域的路径
2. 运行脚本: python3 upload_zips_to_web.py

依赖: 无额外依赖
"""

import os
import sys
import shutil
import logging
from pathlib import Path

# ================= 配置区域 =================
# 本地搜索 ZIP 文件的根目录 (支持二级子目录搜索)
LOCAL_SEARCH_DIR = "/media/zgw/T71/0105out/"

# 搜索深度: 1=仅当前目录, 2=包含一级子目录, 3=包含二级子目录
SEARCH_DEPTH = 3

# 目标文件夹 (ZIP 文件将被复制/移动到这里)
TARGET_DIR = "/media/zgw/T71/zip_collection/"

# 操作模式: "copy" = 复制, "move" = 移动
MODE = "copy"

# 是否跳过已存在的文件
SKIP_EXISTING = True
# ===========================================

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger(__name__)


def find_zip_files(search_dir: str, max_depth: int = 3) -> list:
    """
    在指定目录下搜索 ZIP 文件，支持多级子目录
    """
    search_path = Path(search_dir)
    if not search_path.exists():
        logger.error(f"搜索目录不存在: {search_dir}")
        return []
    
    zip_files = []
    patterns = ["*.zip"]
    if max_depth >= 2:
        patterns.append("*/*.zip")
    if max_depth >= 3:
        patterns.append("*/*/*.zip")
    
    for pattern in patterns:
        zip_files.extend(search_path.glob(pattern))
    
    return sorted(set(zip_files))


def copy_single_file(file_path: Path, target_dir: Path) -> bool:
    """
    复制/移动单个文件到目标目录
    移动模式采用"先复制后删除"策略，确保不损坏原始文件
    """
    filename = file_path.name
    file_size = file_path.stat().st_size
    file_size_mb = file_size / (1024 * 1024)
    target_path = target_dir / filename
    
    logger.info(f"  文件: {filename} ({file_size_mb:.1f} MB)")
    
    # 检查是否已存在
    if SKIP_EXISTING and target_path.exists():
        if target_path.stat().st_size == file_size:
            logger.info(f"    文件已存在且大小一致，跳过")
            return True
    
    try:
        # 无论是 copy 还是 move 模式，都先复制文件
        logger.info(f"    复制中...")
        shutil.copy2(str(file_path), str(target_path))
        
        # 验证复制成功：检查目标文件大小
        if not target_path.exists():
            logger.error(f"    复制失败: 目标文件不存在")
            return False
        
        target_size = target_path.stat().st_size
        if target_size != file_size:
            logger.error(f"    复制失败: 文件大小不匹配 (源: {file_size}, 目标: {target_size})")
            # 删除不完整的目标文件
            target_path.unlink()
            return False
        
        # 如果是移动模式，验证成功后删除原文件
        if MODE == "move":
            logger.info(f"    验证成功，删除原文件...")
            file_path.unlink()
            logger.info(f"    移动完成!")
        else:
            logger.info(f"    复制完成!")
        
        return True
    except Exception as e:
        logger.error(f"    操作失败: {e}")
        return False


def main():
    """主函数"""
    logger.info("=" * 50)
    logger.info("ZIP 文件收集工具")
    logger.info("=" * 50)
    
    # 检查本地目录
    if not Path(LOCAL_SEARCH_DIR).exists():
        logger.error(f"搜索目录不存在: {LOCAL_SEARCH_DIR}")
        return
    
    # 创建目标目录
    target_path = Path(TARGET_DIR)
    target_path.mkdir(parents=True, exist_ok=True)
    logger.info(f"目标目录: {TARGET_DIR}")
    
    # 搜索 ZIP 文件
    logger.info(f"搜索目录: {LOCAL_SEARCH_DIR}")
    logger.info(f"搜索深度: {SEARCH_DEPTH} 级")
    logger.info(f"操作模式: {MODE}")
    
    zip_files = find_zip_files(LOCAL_SEARCH_DIR, SEARCH_DEPTH)
    
    if not zip_files:
        logger.warning("未找到 ZIP 文件")
        return
    
    # 计算总大小
    total_size = sum(f.stat().st_size for f in zip_files)
    total_size_gb = total_size / (1024 * 1024 * 1024)
    
    logger.info(f"找到 {len(zip_files)} 个 ZIP 文件，总大小: {total_size_gb:.2f} GB")
    logger.info("-" * 50)
    
    # 统计
    success_count = 0
    fail_count = 0
    
    # 处理文件
    for i, zip_file in enumerate(zip_files):
        logger.info(f"[{i+1}/{len(zip_files)}] 处理: {zip_file.relative_to(LOCAL_SEARCH_DIR)}")
        
        result = copy_single_file(zip_file, target_path)
        
        if result:
            success_count += 1
        else:
            fail_count += 1
    
    # 打印结果
    logger.info("=" * 50)
    logger.info("任务完成")
    logger.info(f"  成功: {success_count}")
    logger.info(f"  失败: {fail_count}")
    logger.info("=" * 50)


if __name__ == "__main__":
    main()
