import os
from datetime import datetime

def write_txt_report(records, output_path='local_report.txt'):
    """
    将数据处理结果写入本地 TXT 文件。
    records: List[Dict]，每个 dict 包含一条数据的统计信息。
    """
    if not records:
        return
    # 获取所有字段名
    all_keys = set()
    for rec in records:
        all_keys.update(rec.keys())
    all_keys = list(all_keys)
    # 写入
    with open(output_path, 'w', encoding='utf-8') as f:
        f.write(f"数据统计报告 生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write("\t".join(all_keys) + "\n")
        for rec in records:
            row = [str(rec.get(k, '')) for k in all_keys]
            f.write("\t".join(row) + "\n")

# 示例用法：
if __name__ == '__main__':
    sample = [
        {"数据包名称": "1209_134548_134748", "关键帧数": 194, "拉框": 1, "盲区": 1},
        {"数据包名称": "1209_135248_135448", "关键帧数": 187, "拉框": 1, "盲区": 1},
    ]
    write_txt_report(sample)
    print("已写入 local_report.txt")
