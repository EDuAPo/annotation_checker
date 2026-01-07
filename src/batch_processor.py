import json
from pathlib import Path
from typing import Dict, List, Optional
from .data_loader import CustomJsonLoader
from .rules_checker import RuleChecker

class BatchProcessor:
    def __init__(self, config: Dict, data_dir: Path = None):
        self.config = config
        self.data_dir = data_dir
        self.data_loader = CustomJsonLoader(
            config['data']['annotation_path'],
            config['data']['pointcloud_path'],
            config
        )
        
        # 加载 ins.json 数据
        self.ins_data = self._load_ins_data()
        
        # 初始化规则检查器，传入 INS 数据
        self.rule_checker = RuleChecker(config, self.ins_data)
        
    def _load_ins_data(self) -> Optional[List[Dict]]:
        """加载 ins.json 自车位姿数据"""
        ins_paths = []
        
        # 尝试多个可能的路径
        if self.data_dir:
            ins_paths.append(self.data_dir / "ins.json")
        
        ann_path = Path(self.config['data']['annotation_path'])
        if ann_path.is_file():
            ins_paths.append(ann_path.parent / "ins.json")
        else:
            ins_paths.append(ann_path / "ins.json")
            
        pc_path = Path(self.config['data']['pointcloud_path'])
        ins_paths.append(pc_path / "ins.json")
        ins_paths.append(pc_path.parent / "ins.json")
        
        for ins_path in ins_paths:
            if ins_path.exists():
                try:
                    with open(ins_path, 'r') as f:
                        data = json.load(f)
                    print(f"已加载 INS 数据: {ins_path} ({len(data)} 条)")
                    return data
                except Exception as e:
                    print(f"加载 INS 数据失败: {e}")
        
        print("未找到 ins.json，将不进行自车位姿补偿")
        return None
    
    def _get_ins_by_frame_id(self, frame_id: str) -> Optional[Dict]:
        """根据帧ID获取对应的 INS 数据"""
        if not self.ins_data:
            return None
        
        # 尝试从 sample.json 获取帧的时间戳
        # 或者根据 frame_id 匹配 ins 数据的 timestamp_desc
        for ins in self.ins_data:
            ts_desc = ins.get('timestamp_desc', '')
            # timestamp_desc 格式如 "20251127_102238_738"
            # frame_id 可能是数字索引，需要根据实际情况匹配
            if frame_id in ts_desc or str(frame_id) == str(ins.get('id', '')):
                return ins
        
        # 如果 frame_id 是索引，直接用索引访问
        try:
            idx = int(frame_id)
            if 0 <= idx < len(self.ins_data):
                return self.ins_data[idx]
        except ValueError:
            pass
        
        return None
        
    def process_all(self, output_report: str):
        """处理所有标注，生成报告"""
        issues_by_frame = {}
        
        # 1. 获取所有数据并构建轨迹
        # 假设 data_loader 已经缓存了数据
        all_data = self.data_loader.get_all_annotations()
        
        if all_data is None:
            # 如果是文件夹模式，未预加载所有数据，这里手动加载以构建轨迹信息用于一致性检查
            # 注意：这可能会消耗较多内存和时间
            all_data = {}
            temp_ids = self.data_loader.get_all_frame_ids()
            for fid in temp_ids:
                all_data[fid] = self.data_loader.load_annotation(fid)
        
        # 如果是字典 {frame_id: [objs]}
        # 如果是列表 [obj, obj, ...] (NuScenes style)
        
        tracks = {} # instance_token -> list of (frame_id, obj)
        
        if isinstance(all_data, dict):
            # Frame-based dict
            for frame_id, objs in all_data.items():
                for obj in objs:
                    inst_id = obj.get('instance_token')
                    if inst_id:
                        if inst_id not in tracks: tracks[inst_id] = []
                        tracks[inst_id].append((frame_id, obj))
        elif isinstance(all_data, list):
            # Flat list
            for obj in all_data:
                inst_id = obj.get('instance_token')
                frame_id = obj.get('frame_id', 'unknown') # 假设有frame_id
                if inst_id:
                    if inst_id not in tracks: tracks[inst_id] = []
                    tracks[inst_id].append((frame_id, obj))
        
        # 对每个轨迹按时间排序 (假设frame_id是可排序的字符串或数字，或者依赖obj中的timestamp)
        # 这里简单按frame_id排序
        for inst_id in tracks:
            tracks[inst_id].sort(key=lambda x: x[0])
            
        # 2. 逐帧检查
        frame_ids = self.data_loader.get_all_frame_ids()
        # 排序frame_ids以保证报告顺序
        try:
            frame_ids.sort(key=lambda x: int(x))
        except:
            frame_ids.sort()
        
        # 构建帧ID到INS数据的映射
        frame_to_ins = {}
        if self.ins_data:
            for i, fid in enumerate(frame_ids):
                # 尝试按索引匹配
                if i < len(self.ins_data):
                    frame_to_ins[fid] = self.ins_data[i]
            
        for frame_id in frame_ids:
            objects = self.data_loader.load_annotation(frame_id)
            curr_ins = frame_to_ins.get(frame_id)
            
            frame_issues = []
            for obj in objects:
                # 基础检查
                obj_issues = self.rule_checker.check_object(obj)
                
                # 运动一致性检查
                inst_id = obj.get('instance_token')
                if inst_id and inst_id in tracks:
                    track = tracks[inst_id]
                    # 找到当前对象在轨迹中的索引
                    idx = -1
                    for i, (fid, o) in enumerate(track):
                        if o is obj:  # 引用比较
                            idx = i
                            break
                    
                    if idx != -1:
                        prev_obj = track[idx-1][1] if idx > 0 else None
                        next_obj = track[idx+1][1] if idx < len(track)-1 else None
                        
                        # 获取前后帧的 INS 数据
                        prev_fid = track[idx-1][0] if idx > 0 else None
                        next_fid = track[idx+1][0] if idx < len(track)-1 else None
                        prev_ins = frame_to_ins.get(prev_fid) if prev_fid else None
                        next_ins = frame_to_ins.get(next_fid) if next_fid else None
                        
                        motion_issues = self.rule_checker.check_motion_alignment(
                            obj, prev_obj, next_obj,
                            curr_ins, prev_ins, next_ins
                        )
                        obj_issues.extend(motion_issues)
                
                if obj_issues:
                    frame_issues.append({
                        'object_token': obj.get('token', 'unknown'),
                        'class_name': obj.get('attribute_tokens', {}).get('Class', 'unknown'),
                        'issues': obj_issues
                    })
            
            if frame_issues:
                issues_by_frame[frame_id] = frame_issues
        
        # 生成报告
        self.generate_report(issues_by_frame, output_report)
        
    def generate_report(self, issues_by_frame: Dict, output_path: str):
        with open(output_path, 'w') as f:
            if issues_by_frame:
                f.write("标注质量检查报告\n")
                f.write("=================\n\n")
                for frame_id, frame_issues in issues_by_frame.items():
                    f.write(f"帧: {frame_id}\n")
                    for obj_issue in frame_issues:
                        f.write(f"  对象: {obj_issue['object_token']} (类别: {obj_issue.get('class_name', 'unknown')})\n")
                        for issue in obj_issue['issues']:
                            f.write(f"    - {issue}\n")
                    f.write("\n")
            else:
                f.write("未发现任何问题。\n")