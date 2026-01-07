import yaml
import argparse
import sys
import os
import copy
from pathlib import Path

# 添加项目根目录到系统路径，以便可以导入 src 包
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.data_loader import CustomJsonLoader
from src.rules_checker import RuleChecker
from src.visualizer import Visualizer
from src.batch_processor import BatchProcessor

def load_config(config_path: str):
    with open(config_path, 'r') as f:
        config = yaml.safe_load(f)
    return config

def main():
    parser = argparse.ArgumentParser(description="3D标注检查工具")
    parser.add_argument('--config', type=str, default='configs/default.yaml', help='配置文件路径')
    parser.add_argument('--mode', type=str, choices=['visualize', 'batch', 'single'], 
                        default='visualize', help='运行模式')
    parser.add_argument('--frame_id', type=str, help='帧ID（用于single模式）')
    parser.add_argument('--input_dir', type=str, help='输入目录（用于batch模式）。如果是父目录包含多个序列子目录，将批量处理所有子序列。')
    args = parser.parse_args()
    
    config = load_config(args.config)
    
    if args.mode == 'batch':
        if args.input_dir:
            # 简单的检查，提示用户不支持直接的 SSH/SCP 路径
            if ":" in args.input_dir and "@" in args.input_dir:
                print(f"Error: 只有本地路径才被支持。检测到远程路径格式: {args.input_dir}")
                print("Suggestion: 请使用 'sshfs' 将远程目录挂载到本地，或在远程服务器上直接运行此工具。")
                return

            input_path = Path(args.input_dir)
            if not input_path.exists():
                print(f"Error: Path {input_path} does not exist.")
                return

            # 检查是否包含子目录
            # 如果 input_path 下直接有 json 文件，视为单个序列。
            # 否则，如果有子目录，视为多个序列的集合。
            has_json = list(input_path.glob("*.json"))
            subdirs = [d for d in input_path.iterdir() if d.is_dir()]
            
            is_single_sequence = len(has_json) > 0

            if is_single_sequence or not subdirs:
                print(f"Processing as single sequence: {input_path}")
                config['data']['annotation_path'] = str(input_path)
                config['data']['pointcloud_path'] = str(input_path)
                
                processor = BatchProcessor(config)
                processor.process_all(config['batch_processing']['output_report_path'])
            else:
                print(f"Detected multiple sequences in {input_path}")
                base_report_path = Path(config['batch_processing']['output_report_path'])
                
                for seq_dir in sorted(subdirs):
                    print(f"--- Processing sequence: {seq_dir.name} ---")
                    seq_config = copy.deepcopy(config)
                    seq_config['data']['annotation_path'] = str(seq_dir)
                    seq_config['data']['pointcloud_path'] = str(seq_dir)
                    
                    if base_report_path.suffix:
                        new_name = f"{base_report_path.stem}_{seq_dir.name}{base_report_path.suffix}"
                        seq_report_path = base_report_path.parent / new_name
                    else:
                         seq_report_path = base_report_path / f"report_{seq_dir.name}.txt"

                    seq_report_path.parent.mkdir(parents=True, exist_ok=True)
                    
                    try:
                        processor = BatchProcessor(seq_config)
                        processor.process_all(str(seq_report_path))
                        print(f"Report generated: {seq_report_path}")
                    except Exception as e:
                        print(f"Error processing {seq_dir.name}: {e}")
        else:
            processor = BatchProcessor(config)
            processor.process_all(config['batch_processing']['output_report_path'])
    elif args.mode == 'single' and args.frame_id:
        data_loader = CustomJsonLoader(
            config['data']['annotation_path'],
            config['data']['pointcloud_path'],
            config
        )
        objects = data_loader.load_annotation(args.frame_id)
        pointcloud = data_loader.load_pointcloud(args.frame_id)
        visualizer = Visualizer(config)
        visualizer.visualize_frame(pointcloud, objects)
    elif args.mode == 'visualize':
        data_loader = CustomJsonLoader(
            config['data']['annotation_path'],
            config['data']['pointcloud_path'],
            config
        )
        frame_ids = data_loader.get_all_frame_ids()
        if not frame_ids:
            print("未找到任何帧。")
            return
        
        print(f"找到 {len(frame_ids)} 帧。默认显示第一帧: {frame_ids[0]}")
        # 如果指定了frame_id，则使用指定的，否则使用第一个
        target_frame = args.frame_id if args.frame_id else frame_ids[0]
        
        objects = data_loader.load_annotation(target_frame)
        pointcloud = data_loader.load_pointcloud(target_frame)
        visualizer = Visualizer(config)
        visualizer.visualize_frame(pointcloud, objects)
    else:
        # 交互式可视化模式
        print("请使用visualize模式或batch模式。")

if __name__ == '__main__':
    main()