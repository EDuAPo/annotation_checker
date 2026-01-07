import json
import numpy as np
from pathlib import Path
from typing import Dict, List, Any
from src.utils import transform_points

class CustomJsonLoader:
    def __init__(self, annotation_path: str, pointcloud_path: str, config: Dict = None):
        self.annotation_path = Path(annotation_path)
        self.pointcloud_path = Path(pointcloud_path)
        self.config = config or {}
        self.frame_map = {}
        self.sensor_config = {}
        self.cached_data = None  # Cache for single file annotations
        self._load_sample_map()
        self._load_extrinsics()
        self._preload_annotations()

    def _preload_annotations(self):
        """If annotation path is a file, load it once."""
        target_file = None
        if self.annotation_path.is_file():
            target_file = self.annotation_path
        elif self.annotation_path.is_dir():
            # 尝试查找常见的合并标注文件
            for name in ['annotations.json', 'labels.json', 'all_annotations.json']:
                candidate = self.annotation_path / name
                if candidate.exists():
                    target_file = candidate
                    break
        
        if target_file:
            try:
                with open(target_file, 'r') as f:
                    self.cached_data = json.load(f)
                print(f"Loaded annotations from {target_file}")
            except Exception as e:
                print(f"Failed to preload annotations: {e}")

    def get_all_annotations(self) -> Dict:
        """Return all annotations if available."""
        return self.cached_data
        
    def _load_extrinsics(self):
        """尝试加载传感器外参配置"""
        # 尝试在不同位置寻找配置文件
        candidates = [
            self.pointcloud_path / "sensor_config_combined_latest.json",
            self.annotation_path.parent / "sensor_config_combined_latest.json",
            self.pointcloud_path.parent / "sensor_config_combined_latest.json"
        ]
        
        for config_path in candidates:
            if config_path.exists():
                try:
                    with open(config_path, 'r') as f:
                        self.sensor_config = json.load(f)
                    print(f"已加载传感器配置: {config_path}")
                    break
                except Exception as e:
                    print(f"加载传感器配置失败: {e}")

    def _get_sensor_extrinsics(self, sensor_name: str):
        """获取指定传感器的外参 (rotation, translation)"""
        if not self.sensor_config:
            return None, None
            
        # 如果配置是列表
        if isinstance(self.sensor_config, list):
            for cfg in self.sensor_config:
                # 假设列表中的项有 'sensor_name' 或 'name' 字段
                name = cfg.get('sensor_name') or cfg.get('name') or cfg.get('sensor_id') or cfg.get('sensor_token')
                if name == sensor_name or name == f"{sensor_name}_config":
                    if 'rotation' in cfg and 'translation' in cfg:
                        return cfg['rotation'], cfg['translation']
                    elif 'extrinsic_parameters' in cfg:
                        return cfg['extrinsic_parameters']['rotation'], cfg['extrinsic_parameters']['translation']
            return None, None

        # 尝试直接查找 (如果是字典)
        if sensor_name in self.sensor_config:
            cfg = self.sensor_config[sensor_name]
            # 假设结构可能是直接的，或者嵌套的
            # 这里做一个简单的递归搜索或者尝试常见结构
            if 'rotation' in cfg and 'translation' in cfg:
                return cfg['rotation'], cfg['translation']
            elif 'extrinsic_parameters' in cfg:
                return cfg['extrinsic_parameters']['rotation'], cfg['extrinsic_parameters']['translation']
        
        # 如果没找到，尝试遍历查找包含该sensor_name的键
        # 这是一个简化的处理，实际情况可能更复杂
        return None, None

    def _load_sample_map(self):
        """加载sample.json映射文件"""
        # 假设sample.json在annotation_path同级目录或pointcloud_path下
        sample_file = self.pointcloud_path / "sample.json"
        if not sample_file.exists():
            if self.annotation_path.is_file():
                sample_file = self.annotation_path.parent / "sample.json"
        
        if sample_file.exists():
            with open(sample_file, 'r') as f:
                samples = json.load(f)
                for sample in samples:
                    # 建立 frame_id (str) -> pointcloud_filename 的映射
                    # 优先使用 iv_points_front_mid
                    fid = str(sample['id'])
                    pc_file = sample.get('iv_points_front_mid')
                    if pc_file:
                        self.frame_map[fid] = pc_file

    def get_all_frame_ids(self) -> List[str]:
        """获取所有帧ID"""
        if self.cached_data:
            data = self.cached_data
            if isinstance(data, dict):
                return list(data.keys())
            elif isinstance(data, list):
                return list(set([obj.get('frame_id') for obj in data if 'frame_id' in obj]))
        elif self.annotation_path.is_file():
            # Fallback if cache failed but file exists (unlikely)
            with open(self.annotation_path, 'r') as f:
                data = json.load(f)
            if isinstance(data, dict):
                return list(data.keys())
            elif isinstance(data, list):
                return list(set([obj.get('frame_id') for obj in data if 'frame_id' in obj]))
        else:
            # 文件夹模式
            excluded_stems = {'sample', 'annotations', 'labels', 'sensor_config'}
            frame_ids = []
            for f in self.annotation_path.glob('*.json'):
                if f.stem in excluded_stems or 'sensor_config' in f.stem:
                    continue
                frame_ids.append(f.stem)
            return frame_ids

    def load_annotation(self, frame_id: str) -> List[Dict]:
        """加载指定帧的标注"""
        if self.cached_data:
            data = self.cached_data
            if isinstance(data, dict) and frame_id in data:
                return data[frame_id]
            elif isinstance(data, list):
                filtered = [obj for obj in data if obj.get('frame_id') == frame_id]
                if filtered:
                    return filtered
                return data # Fallback
            else:
                print(f"Warning: Frame ID {frame_id} not found in cached data")
                return []

        
        ann_file = self.annotation_path / f"{frame_id}.json"
        if not ann_file.exists():
            # 如果文件夹内是单个大JSON文件，则尝试加载并提取对应帧
            # 这里根据实际情况调整
            pass
        with open(ann_file, 'r') as f:
            data = json.load(f)
        # 假设JSON文件直接是一个列表，或者根据您的结构调整
        # 这里假设整个JSON是一个字典，键为帧ID，值为对象列表
        # 根据您的示例，JSON的顶层键是帧ID（字符串），值是该帧的对象列表
        # 如果传入的frame_id在顶层键中，则返回对应的列表
        if isinstance(data, dict) and frame_id in data:
            return data[frame_id]
        elif isinstance(data, list):
            return data
        else:
            raise ValueError(f"无法解析标注文件 {ann_file}")
    
    def load_pointcloud(self, frame_id: str) -> np.ndarray:
        """加载指定帧的点云"""
        pc_file = None
        
        # 1. 尝试从映射中获取
        if frame_id in self.frame_map:
            filename = self.frame_map[frame_id]
            # 针对特定目录结构的尝试
            candidates = [
                self.pointcloud_path / "iv_points_front_mid" / "pcd_binary" / filename,
                self.pointcloud_path / filename,
            ]
            for p in candidates:
                if p.exists():
                    pc_file = p
                    break
        
        # 2. 如果映射没找到或文件不存在，尝试默认命名规则
        if pc_file is None or not pc_file.exists():
            pc_file = self.pointcloud_path / f"{frame_id}.bin"
            if not pc_file.exists():
                # 尝试其他格式
                pc_file = self.pointcloud_path / f"{frame_id}.pcd"
        
        if pc_file.exists():
            if pc_file.suffix == '.pcd':
                import open3d as o3d
                # Open3D 读取 PCD
                pcd = o3d.io.read_point_cloud(str(pc_file))
                points = np.asarray(pcd.points)
            else:
                # 假设是二进制 float32 x,y,z,i
                points = np.fromfile(str(pc_file), dtype=np.float32).reshape(-1, 4)[:, :3]
            
            # Check coordinate system config
            coord_sys = self.config.get('coordinate_system', {})
            target_frame = coord_sys.get('frame', 'vehicle')
            sensor_height = coord_sys.get('sensor_height', 0.0)

            # 尝试应用外参转换 (Lidar -> Vehicle/World)
            # 默认假设使用 iv_points_front_mid
            sensor_name = "iv_points_front_mid"
            rot, trans = self._get_sensor_extrinsics(sensor_name)
            
            if target_frame == 'vehicle':
                if rot and trans:
                    print(f"Applying extrinsics for {sensor_name}: R={rot}, T={trans}")
                    # 假设配置文件中的四元数是 [w, x, y, z] 格式
                    # utils.transform_points 现在也期望 [w, x, y, z]
                    if len(rot) == 4:
                        points = transform_points(points, rot, trans)
                else:
                    # Fallback: use sensor_height if available
                    if sensor_height != 0.0:
                        print(f"Warning: No extrinsics found. Approximating Vehicle frame by shifting Z by {sensor_height}")
                        points[:, 2] += sensor_height
                    else:
                        available = []
                        if isinstance(self.sensor_config, dict):
                            available = list(self.sensor_config.keys())
                        elif isinstance(self.sensor_config, list):
                            if self.sensor_config:
                                 import sys
                            available = [cfg.get('sensor_name') or cfg.get('name') or cfg.get('sensor_token') for cfg in self.sensor_config]
                        print(f"Warning: No extrinsics found for {sensor_name} and no sensor_height configured. Points remain in Lidar frame. Available sensors: {available}")
            elif target_frame == 'lidar':
                print("Target frame is Lidar. Skipping transformation.")
            
            return points
        else:
            raise FileNotFoundError(f"点云文件不存在: {pc_file} (Frame ID: {frame_id})")