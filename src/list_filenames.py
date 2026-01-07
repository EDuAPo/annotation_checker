import os
from pathlib import Path

# ================= 配置区域 =================
# 目标文件夹路径
TARGET_DIR = "/home/zgw/Downloads/12.30导出/线段/"

# 要去除的后缀
SUFFIX = ".json"
# ===========================================

def list_filenames_without_suffix():
    folder_path = Path(TARGET_DIR)
    
    if not folder_path.exists():
        print(f"错误: 文件夹不存在 - {TARGET_DIR}")
        return

    # 获取所有文件
    files = list(folder_path.glob(f"*{SUFFIX}"))
    
    if not files:
        print(f"未找到以 {SUFFIX} 结尾的文件")
        return

    print(f"--- 开始打印文件名 (已去除 {SUFFIX} 后缀) ---")
    
    count = 0
    for file_path in files:
        # 获取文件名
        name = file_path.name
        
        # 去除后缀打印
        if name.endswith(SUFFIX):
            name_without_suffix = name[:-len(SUFFIX)]
            print(name_without_suffix)
            count += 1
            
    print(f"--- 打印结束，共 {count} 个文件 ---")

if __name__ == "__main__":
    list_filenames_without_suffix()
