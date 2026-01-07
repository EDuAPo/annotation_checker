import os
from pathlib import Path

# ================= 配置区域 =================
# 1. 目标文件夹路径
TARGET_DIR = "/media/zgw/WD/from_rere/盲区json/json/"

# 2. 要去除的后缀 (注意包含点)
# 如果您的文件后缀真的是 .josn (拼写错误)，请改为 ".josn"
SUFFIX_TO_REMOVE = ".json" 
# ===========================================

def batch_rename():
    folder_path = Path(TARGET_DIR)
    
    if not folder_path.exists():
        print(f"错误: 文件夹不存在 - {TARGET_DIR}")
        return

    # 查找所有以指定后缀结尾的文件
    files = list(folder_path.glob(f"*{SUFFIX_TO_REMOVE}"))
    
    if not files:
        print(f"在 {TARGET_DIR} 未找到以 {SUFFIX_TO_REMOVE} 结尾的文件")
        return

    print(f"找到 {len(files)} 个文件，准备去除后缀...")

    success_count = 0
    
    for file_path in files:
        # 获取旧文件名
        old_name = file_path.name
        
        # 获取新文件名 (去除后缀)
        # stem 属性会自动去除最后一个后缀 (例如 abc.json -> abc)
        # 如果文件名是 abc.json.bak，stem 会变成 abc.json，这可能不是你想要的
        # 所以这里用 replace 确保只去除指定的后缀
        if file_path.name.endswith(SUFFIX_TO_REMOVE):
            new_name = file_path.name[:-len(SUFFIX_TO_REMOVE)]
        else:
            continue

        new_path = file_path.with_name(new_name)
        
        try:
            if new_path.exists():
                print(f"[跳过] 目标文件已存在: {new_name}")
                continue
                
            # 执行重命名
            file_path.rename(new_path)
            print(f"[成功] {old_name} -> {new_name}")
            success_count += 1
            
        except Exception as e:
            print(f"[失败] {old_name}: {e}")

    print(f"\n处理完成。共重命名 {success_count} 个文件。")

if __name__ == "__main__":
    batch_rename()
