import math
import numpy as np
from typing import Dict, List, Tuple, Optional

class RuleChecker:
    def __init__(self, config: Dict, ins_data: List[Dict] = None):
        self.config = config
        self.rules = config['rules']
        self.ins_data = ins_data  # ins.json 数据，用于自车位姿补偿
        self._ins_index = {}  # timestamp -> ins entry 快速索引
        if ins_data:
            self._build_ins_index()
    
    def _build_ins_index(self):
        """构建 ins 数据的时间戳索引"""
        for entry in self.ins_data:
            ts = entry.get('timestamp_nanosec')
            if ts:
                self._ins_index[ts] = entry
    
    def get_ins_by_timestamp(self, timestamp: int) -> Optional[Dict]:
        """根据时间戳获取最近的 ins 数据"""
        if not self._ins_index:
            return None
        # 精确匹配
        if timestamp in self._ins_index:
            return self._ins_index[timestamp]
        # 找最近的
        timestamps = list(self._ins_index.keys())
        if not timestamps:
            return None
        closest = min(timestamps, key=lambda t: abs(t - timestamp))
        # 时间差超过 100ms (100000000 ns) 则认为无效
        if abs(closest - timestamp) > 100000000:
            return None
        return self._ins_index[closest]
    
    def quaternion_to_rotation_matrix(self, q: List[float]) -> np.ndarray:
        """四元数转旋转矩阵 (w, x, y, z) 格式"""
        w, x, y, z = q
        return np.array([
            [1 - 2*(y*y + z*z), 2*(x*y - w*z), 2*(x*z + w*y)],
            [2*(x*y + w*z), 1 - 2*(x*x + z*z), 2*(y*z - w*x)],
            [2*(x*z - w*y), 2*(y*z + w*x), 1 - 2*(x*x + y*y)]
        ])
    
    def transform_to_world(self, pos_ego: np.ndarray, ins_entry: Dict) -> np.ndarray:
        """将自车坐标系下的位置转换到世界坐标系 (UTM)
        
        Args:
            pos_ego: 自车坐标系下的位置 [x, y, z]
            ins_entry: ins.json 中的一条记录
            
        Returns:
            世界坐标系 (UTM) 下的位置 [x, y, z]
        """
        # 自车在世界坐标系中的位置
        ego_utm = np.array([
            ins_entry.get('utm_x', 0),
            ins_entry.get('utm_y', 0),
            ins_entry.get('utm_z', 0)
        ])
        
        # 自车姿态 (ins.json 格式为 x, y, z, w)
        q_ego = [
            ins_entry.get('quaternion_w', 1),
            ins_entry.get('quaternion_x', 0),
            ins_entry.get('quaternion_y', 0),
            ins_entry.get('quaternion_z', 0)
        ]
        
        # 旋转矩阵
        R_ego = self.quaternion_to_rotation_matrix(q_ego)
        
        # 转换: world_pos = ego_pos @ R_ego.T + ego_utm
        pos_world = R_ego @ pos_ego + ego_utm
        
        return pos_world
        
    def get_euler_angles(self, q):
        # 假设输入四元数格式为 [w, x, y, z]
        w, x, y, z = q
        # Roll (x-axis rotation)
        sinr_cosp = 2 * (w * x + y * z)
        cosr_cosp = 1 - 2 * (x * x + y * y)
        roll = math.atan2(sinr_cosp, cosr_cosp)

        # Pitch (y-axis rotation)
        sinp = 2 * (w * y - z * x)
        if abs(sinp) >= 1:
            pitch = math.copysign(math.pi / 2, sinp)
        else:
            pitch = math.asin(sinp)

        # Yaw (z-axis rotation)
        siny_cosp = 2 * (w * z + x * y)
        cosy_cosp = 1 - 2 * (y * y + z * z)
        yaw = math.atan2(siny_cosp, cosy_cosp)

        return roll, pitch, yaw

    def check_motion_alignment(self, obj: Dict, prev_obj: Dict, next_obj: Dict,
                                curr_ins: Dict = None, prev_ins: Dict = None, next_ins: Dict = None) -> List[str]:
        """
        检查车辆朝向与运动轨迹的一致性
        Industrial Method: Motion Consistency Check
        
        Args:
            obj: 当前帧的目标对象
            prev_obj: 上一帧的同一目标
            next_obj: 下一帧的同一目标  
            curr_ins: 当前帧的自车 INS 数据 (用于坐标转换)
            prev_ins: 上一帧的自车 INS 数据
            next_ins: 下一帧的自车 INS 数据
        """
        issues = []
        obj_class = obj.get('attribute_tokens', {}).get('Class', 'unknown').lower()
        
        # 仅检查车辆
        if 'vehicle' not in obj_class:
            return issues
        
        # 获取当前位置 (自车坐标系)
        curr_pos_ego = np.array(obj.get('translation', [0,0,0]))
        
        # 如果有 INS 数据，转换到世界坐标系
        use_world_coords = curr_ins is not None
        
        if use_world_coords:
            curr_pos = self.transform_to_world(curr_pos_ego, curr_ins)
        else:
            curr_pos = curr_pos_ego
        
        # 计算运动向量
        motion_vec = None
        
        # 优先使用前后帧计算
        if prev_obj and next_obj:
            prev_pos_ego = np.array(prev_obj.get('translation', [0,0,0]))
            next_pos_ego = np.array(next_obj.get('translation', [0,0,0]))
            
            if use_world_coords and prev_ins and next_ins:
                prev_pos = self.transform_to_world(prev_pos_ego, prev_ins)
                next_pos = self.transform_to_world(next_pos_ego, next_ins)
            else:
                prev_pos = prev_pos_ego
                next_pos = next_pos_ego
                
            motion_vec = next_pos - prev_pos
        elif next_obj:
            next_pos_ego = np.array(next_obj.get('translation', [0,0,0]))
            
            if use_world_coords and next_ins:
                next_pos = self.transform_to_world(next_pos_ego, next_ins)
            else:
                next_pos = next_pos_ego
                
            motion_vec = next_pos - curr_pos
        elif prev_obj:
            prev_pos_ego = np.array(prev_obj.get('translation', [0,0,0]))
            
            if use_world_coords and prev_ins:
                prev_pos = self.transform_to_world(prev_pos_ego, prev_ins)
            else:
                prev_pos = prev_pos_ego
                
            motion_vec = curr_pos - prev_pos
            
        if motion_vec is None:
            return issues
            
        # 计算速度模长 (假设帧间隔恒定，这里只看位移大小)
        # 如果位移太小，认为是静止或噪声，不进行朝向检查
        dist = np.linalg.norm(motion_vec[:2])  # 只看XY平面
        if dist < 0.5:  # 阈值可调，例如0.5米
            return issues
        
        # 计算运动方向 (Yaw) - 在世界坐标系中
        motion_yaw_world = math.atan2(motion_vec[1], motion_vec[0])
        
        # 获取标注朝向 (Yaw) - 在自车坐标系中
        rotation = obj.get('rotation', [])
        if len(rotation) == 4:
            _, _, obj_yaw_ego = self.get_euler_angles(rotation)
            
            # 如果使用世界坐标系，需要将标注朝向也转换到世界坐标系
            if use_world_coords and curr_ins:
                # 获取自车朝向 (azimuth 或从四元数计算)
                ego_yaw = curr_ins.get('azimuth', 0)
                # 标注朝向在世界坐标系中 = 自车朝向 + 标注朝向(相对自车)
                obj_yaw_world = ego_yaw + obj_yaw_ego
            else:
                obj_yaw_world = obj_yaw_ego
            
            # 计算角度差 (考虑周期性)
            diff = motion_yaw_world - obj_yaw_world
            # 归一化到 [-pi, pi]
            while diff > math.pi:
                diff -= 2 * math.pi
            while diff < -math.pi:
                diff += 2 * math.pi
            diff = abs(diff)
            
            # 检查一致性
            # 允许误差: 30度 (约0.52弧度)
            # 考虑倒车情况: 差值接近 PI
            is_forward = diff < 0.52
            is_backward = abs(diff - math.pi) < 0.52
            
            if not is_forward and not is_backward:
                issues.append(f"朝向与运动方向不一致: 差值{math.degrees(diff):.1f}度")
            elif is_backward:
                # 倒车是合法的，但可以标记一下，或者如果大部分时间是倒车可能需要确认
                # issues.append(f"Info: 车辆正在倒车")
                pass
                
        return issues

    def check_object(self, obj: Dict) -> List[str]:
        """检查单个对象，返回问题列表"""
        issues = []
        obj_token = obj.get('token', 'unknown')
        size = obj.get('size', [])
        center = obj.get('translation', [])
        rotation = obj.get('rotation', [])
        num_pts = obj.get('num_lidar_pts', 0)
        obj_class = obj.get('attribute_tokens', {}).get('Class', 'unknown').lower()
        
        # 检查点云数量
        if num_pts < self.rules['min_lidar_points']:
            issues.append(f"点云数量过少: {num_pts}")
        
        # 检查尺寸
        if len(size) == 3:
            l, w, h = size
            # 根据类别选择规则
            if 'vehicle' in obj_class:
                rule = self.rules['vehicle']
            elif 'pedestrian' in obj_class:
                rule = self.rules['pedestrian']
            elif 'cone' in obj_class:
                rule = self.rules['cone']
            elif 'sign' in obj_class:
                rule = self.rules['sign']
            else:
                rule = None
                
            if rule:
                if not (rule['length_range'][0] <= l <= rule['length_range'][1]):
                    issues.append(f"长度异常: {l}")
                if not (rule['width_range'][0] <= w <= rule['width_range'][1]):
                    issues.append(f"宽度异常: {w}")
                if not (rule['height_range'][0] <= h <= rule['height_range'][1]):
                    issues.append(f"高度异常: {h}")
        
        # 检查高度位置
        # if len(center) == 3:
        #     z = center[2]
        #     # 简单检查：物体应该在地面附近，这里假设地面z=0，允许上下浮动2米
        #     if abs(z) > 2.0:  # 根据实际情况调整
        #         issues.append(f"高度异常: {z}")
        
        # 检查四元数归一化
        if len(rotation) == 4:
            norm = math.sqrt(sum([r*r for r in rotation]))
            if abs(norm - 1.0) > 0.01:
                issues.append(f"四元数未归一化: {norm}")
            
            # 检查车辆的Roll和Pitch
            if 'vehicle' in obj_class:
                roll, pitch, yaw = self.get_euler_angles(rotation)
                # 阈值设为 0.5 弧度 (约28度)，防止车辆翻车或严重倾斜
                if abs(roll) > 0.5:
                    issues.append(f"车辆Roll异常: {roll:.2f}")
                if abs(pitch) > 0.5:
                    issues.append(f"车辆Pitch异常: {pitch:.2f}")
        
        return issues