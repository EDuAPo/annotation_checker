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
        
        # 低速无人车优化参数
        self.low_speed_threshold = 0.5  # m/s，低速阈值 (1.8 km/h)
        self.static_threshold = 0.1     # m/s，静止阈值
        self.angle_tolerance = 0.5236   # 弧度，角度容限 (约30度) - 比原来更严格
        self.min_track_length = 3       # 最短轨迹长度进行一致性检查
        self.smooth_window = 3          # 平滑窗口大小
    
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
    
    def get_ins_interpolated(self, timestamp: int) -> Optional[Dict]:
        """根据时间戳获取插值的 ins 数据，提高时间同步精度"""
        if not self._ins_index:
            return None
        
        timestamps = sorted(self._ins_index.keys())
        if not timestamps:
            return None
        
        # 精确匹配
        if timestamp in self._ins_index:
            return self._ins_index[timestamp]
        
        # 找到前后两个时间戳用于插值
        prev_ts = None
        next_ts = None
        
        for ts in timestamps:
            if ts < timestamp:
                prev_ts = ts
            elif ts > timestamp:
                next_ts = ts
                break
        
        # 如果只有一个时间戳，返回最近的
        if prev_ts is None:
            if abs(next_ts - timestamp) > 100000000:  # 100ms
                return None
            return self._ins_index[next_ts]
        elif next_ts is None:
            if abs(timestamp - prev_ts) > 100000000:
                return None
            return self._ins_index[prev_ts]
        
        # 时间差太大，不进行插值
        if abs(timestamp - prev_ts) > 100000000 or abs(next_ts - timestamp) > 100000000:
            return None
        
        # 线性插值
        prev_ins = self._ins_index[prev_ts]
        next_ins = self._ins_index[next_ts]
        
        # 时间比例
        total_time = next_ts - prev_ts
        ratio = (timestamp - prev_ts) / total_time if total_time > 0 else 0
        
        # 插值位置和姿态
        interpolated = {}
        
        # UTM坐标线性插值
        for key in ['utm_x', 'utm_y', 'utm_z']:
            prev_val = prev_ins.get(key, 0)
            next_val = next_ins.get(key, 0)
            interpolated[key] = prev_val + ratio * (next_val - prev_val)
        
        # 四元数球面线性插值 (slerp)
        prev_quat = [
            prev_ins.get('quaternion_w', 1),
            prev_ins.get('quaternion_x', 0),
            prev_ins.get('quaternion_y', 0),
            prev_ins.get('quaternion_z', 0)
        ]
        next_quat = [
            next_ins.get('quaternion_w', 1),
            next_ins.get('quaternion_x', 0),
            next_ins.get('quaternion_y', 0),
            next_ins.get('quaternion_z', 0)
        ]
        
        # 简单的四元数插值 (可以进一步优化为slerp)
        interpolated_quat = []
        for i in range(4):
            interpolated_quat.append(prev_quat[i] + ratio * (next_quat[i] - prev_quat[i]))
        
        # 归一化四元数
        norm = math.sqrt(sum(q*q for q in interpolated_quat))
        if norm > 0:
            interpolated_quat = [q/norm for q in interpolated_quat]
        
        interpolated.update({
            'quaternion_w': interpolated_quat[0],
            'quaternion_x': interpolated_quat[1],
            'quaternion_y': interpolated_quat[2],
            'quaternion_z': interpolated_quat[3],
            'timestamp_nanosec': timestamp
        })
        
        return interpolated

    def classify_motion_state(self, velocity: float) -> str:
        """
        根据速度分类运动状态 (针对低速无人车优化)
        
        Args:
            velocity: 速度 m/s
            
        Returns:
            运动状态: 'static', 'low_speed', 'normal_speed'
        """
        if velocity < self.static_threshold:
            return 'static'
        elif velocity < self.low_speed_threshold:
            return 'low_speed'
        else:
            return 'normal_speed'
    
    def check_trajectory_consistency(self, track: List[Tuple[int, Dict]], 
                                   frame_to_ins: Dict[int, Dict] = None) -> List[str]:
        """
        轨迹一致性检查 (低速无人车优化版)
        考虑低速运动特性，适应频繁转向和停车场景
        
        Args:
            track: 轨迹数据 [(frame_idx, obj), ...] 按时间排序
            frame_to_ins: 帧索引到INS数据的映射
            
        Returns:
            问题列表
        """
        issues = []
        if len(track) < self.min_track_length:
            return issues  # 轨迹太短，无法进行一致性检查
        
        # 提取位置序列和时间戳
        positions = []
        timestamps = []
        velocities = []
        
        for frame_idx, obj in track:
            pos_ego = np.array(obj.get('translation', [0, 0, 0]))
            
            # 使用插值INS数据进行坐标转换
            if frame_to_ins and frame_idx in frame_to_ins:
                ins_data = frame_to_ins[frame_idx]
                pos_world = self.transform_to_world(pos_ego, ins_data)
            else:
                pos_world = pos_ego
            
            positions.append(pos_world)
            
            # 提取时间戳 (假设10Hz采样，如果没有真实时间戳)
            ts = obj.get('timestamp', frame_idx * 100000000)  # 100ms间隔
            timestamps.append(ts)
        
        # 计算速度和加速度
        for i in range(1, len(positions)):
            dt = (timestamps[i] - timestamps[i-1]) / 1e9  # 转换为秒
            if dt > 0:
                vel = np.linalg.norm(positions[i][:2] - positions[i-1][:2]) / dt
                velocities.append(vel)
        
        # 1. 低速场景下的轨迹平滑性检查
        if len(velocities) > 1:
            # 计算速度变化率 (针对低速场景优化)
            speed_changes = []
            for i in range(1, len(velocities)):
                dt = (timestamps[i+1] - timestamps[i]) / 1e9 if i+1 < len(timestamps) else 0.1
                if dt > 0:
                    acc = (velocities[i] - velocities[i-1]) / dt
                    speed_changes.append(abs(acc))
            
            # 低速场景下允许更大的加速度变化，但要检测异常
            max_reasonable_acc = 3.0  # m/s²，低速无人车最大合理加速度
            for i, acc in enumerate(speed_changes):
                if acc > max_reasonable_acc:
                    issues.append(f"轨迹加速度异常: 第{i+1}段加速度{acc:.1f}m/s² (超过{max_reasonable_acc:.1f}m/s²)")
        
        # 2. 运动状态一致性检查
        motion_states = [self.classify_motion_state(v) for v in velocities]
        
        # 检测频繁的状态切换 (可能表示标注不稳定)
        state_transitions = 0
        for i in range(1, len(motion_states)):
            if motion_states[i] != motion_states[i-1]:
                state_transitions += 1
        
        # 低速场景下允许更多的状态切换，但要检测异常
        max_reasonable_transitions = len(velocities) * 0.6  # 允许60%的状态切换
        if state_transitions > max_reasonable_transitions:
            issues.append(f"运动状态频繁切换: {state_transitions}次切换 (轨迹长度{len(velocities)})")
        
        # 3. 轨迹连续性检查 (低速优化)
        for i in range(1, len(positions)):
            dist = np.linalg.norm(positions[i] - positions[i-1])
            dt = (timestamps[i] - timestamps[i-1]) / 1e9
            
            if dt > 0:
                speed = dist / dt
                # 低速无人车最大速度限制 (考虑可能的异常)
                max_reasonable_speed = 8.0  # m/s ≈ 28 km/h
                if speed > max_reasonable_speed:
                    issues.append(f"轨迹不连续: 第{i}帧间速度{speed:.1f}m/s过高")
                
                # 检测静止状态下的位置跳跃
                if i < len(velocities) and velocities[i-1] < self.static_threshold and dist > 0.5:
                    issues.append(f"静止状态下位置跳跃: 第{i}帧移动{dist:.2f}米")
        
        # 4. 轨迹合理性检查 (低速无人车特性)
        if len(velocities) > 2:
            # 计算运动方向变化
            directions = []
            for i in range(1, len(positions)):
                vec = positions[i][:2] - positions[i-1][:2]
                if np.linalg.norm(vec) > 0.01:  # 避免零向量
                    direction = math.atan2(vec[1], vec[0])
                    directions.append(direction)
            
            # 检测急转弯 (低速场景下更常见)
            if len(directions) > 1:
                for i in range(1, len(directions)):
                    angle_diff = abs(directions[i] - directions[i-1])
                    angle_diff = min(angle_diff, 2*math.pi - angle_diff)  # 考虑周期性
                    
                    # 低速场景下允许更大的转向角度，但要检测异常
                    max_turn_rate = math.pi / 2  # 90度/帧
                    if angle_diff > max_turn_rate:
                        issues.append(f"轨迹方向突变: 第{i}帧处转向{math.degrees(angle_diff):.1f}度")
        
        return issues
    
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

    def check_motion_alignment(self, obj: Dict, track: List[Tuple[int, Dict]] = None,
                                frame_idx: int = 0, frame_to_ins: Dict[int, Dict] = None) -> List[str]:
        """
        检查车辆朝向与运动轨迹的一致性 (低速无人车优化版)
        根据运动状态动态调整检查策略
        
        Args:
            obj: 当前帧的目标对象
            track: 完整轨迹数据 [(frame_idx, obj), ...] 按时间排序
            frame_idx: 当前对象在轨迹中的索引
            frame_to_ins: 帧索引到INS数据的映射
        """
        issues = []
        obj_class = obj.get('attribute_tokens', {}).get('Class', 'unknown').lower()
        
        # 仅检查车辆 (支持多种车辆类别)
        vehicle_classes = ['vehicle', 'car', 'truck', 'bus', 'motorcycle', 'bicycle']
        is_vehicle = any(vc in obj_class for vc in vehicle_classes)
        
        if not is_vehicle:
            return issues
        
        # 获取当前位置 (自车坐标系)
        curr_pos_ego = np.array(obj.get('translation', [0, 0, 0]))
        
        # 使用插值INS数据进行坐标转换
        curr_ins = frame_to_ins.get(frame_idx) if frame_to_ins else None
        use_world_coords = curr_ins is not None
        
        # 改进的运动向量计算：使用多帧数据进行更准确的估计
        motion_vec = self._estimate_motion_vector(track, frame_idx, frame_to_ins)
        
        if motion_vec is None:
            return issues
        
        # 计算速度模长和运动状态
        dist = np.linalg.norm(motion_vec[:2])  # 只看XY平面
        motion_speed = dist / 0.1  # 假设帧间隔0.1秒
        motion_state = self.classify_motion_state(motion_speed)
        
        # 根据运动状态调整检测阈值
        if motion_state == 'static':
            # 静止状态：更宽松的角度检查，更严格的位置一致性
            min_motion_threshold = 0.05  # 5cm
            angle_tolerance = self.angle_tolerance * 1.5  # 更宽松的角度容限
        elif motion_state == 'low_speed':
            # 低速运动：平衡检查
            min_motion_threshold = 0.1  # 10cm
            angle_tolerance = self.angle_tolerance
        else:
            # 正常速度：更严格的检查
            min_motion_threshold = 0.2  # 20cm
            angle_tolerance = self.angle_tolerance * 0.8  # 更严格的角度容限
        
        if dist < min_motion_threshold:
            return issues
        
        # 计算运动方向 (Yaw) - 在世界坐标系中
        motion_yaw_world = math.atan2(motion_vec[1], motion_vec[0])
        
        # 获取标注朝向 (Yaw) - 在自车坐标系中
        rotation = obj.get('rotation', [])
        if len(rotation) == 4:
            _, _, obj_yaw_ego = self.get_euler_angles(rotation)
            
            # 如果使用世界坐标系，需要将标注朝向也转换到世界坐标系
            if use_world_coords and curr_ins:
                # 从四元数计算自车朝向
                ego_quat = [
                    curr_ins.get('quaternion_w', 1),
                    curr_ins.get('quaternion_x', 0),
                    curr_ins.get('quaternion_y', 0),
                    curr_ins.get('quaternion_z', 0)
                ]
                _, _, ego_yaw = self.get_euler_angles(ego_quat)
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
            
            # 基于运动状态的动态角度一致性检查
            is_forward = diff < angle_tolerance
            is_backward = abs(diff - math.pi) < angle_tolerance
            
            if not is_forward and not is_backward:
                # 根据运动状态提供不同的诊断信息
                state_desc = {
                    'static': '静止',
                    'low_speed': '低速',
                    'normal_speed': '正常速度'
                }.get(motion_state, '未知')
                
                issues.append(f"[{state_desc}]朝向与运动方向不一致: 差值{math.degrees(diff):.1f}度, "
                            f"运动速度{motion_speed:.2f}m/s")
            elif is_backward and motion_speed > self.static_threshold:
                # 只有在明显运动时才报告倒车
                issues.append(f"车辆正在倒车: 速度{motion_speed:.2f}m/s")
                
        return issues
    
    def _estimate_motion_vector(self, track: List[Tuple[int, Dict]], frame_idx: int, 
                               frame_to_ins: Dict[int, Dict] = None) -> Optional[np.ndarray]:
        """
        使用多帧数据估计运动向量，提高准确性
        
        Args:
            track: 完整轨迹 [(frame_idx, obj), ...]
            frame_idx: 当前帧在轨迹中的索引
            frame_to_ins: 帧到INS数据的映射
            
        Returns:
            运动向量 [dx, dy, dz] 或 None
        """
        if not track or frame_idx >= len(track):
            return None
        
        # 使用前后3帧进行加权平均 (如果可用)
        positions = []
        weights = []
        
        # 当前帧
        curr_frame_idx, curr_obj = track[frame_idx]
        curr_pos_ego = np.array(curr_obj.get('translation', [0, 0, 0]))
        curr_ins = frame_to_ins.get(curr_frame_idx) if frame_to_ins else None
        
        if curr_ins:
            curr_pos = self.transform_to_world(curr_pos_ego, curr_ins)
        else:
            curr_pos = curr_pos_ego
        
        positions.append(curr_pos)
        weights.append(1.0)  # 当前帧权重最高
        
        # 前帧
        for offset in [-1, -2]:
            if frame_idx + offset >= 0:
                prev_frame_idx, prev_obj = track[frame_idx + offset]
                prev_pos_ego = np.array(prev_obj.get('translation', [0, 0, 0]))
                prev_ins = frame_to_ins.get(prev_frame_idx) if frame_to_ins else None
                
                if prev_ins:
                    prev_pos = self.transform_to_world(prev_pos_ego, prev_ins)
                else:
                    prev_pos = prev_pos_ego
                
                positions.append(prev_pos)
                weights.append(0.5 ** abs(offset))  # 距离越远权重越低
        
        # 后帧
        for offset in [1, 2]:
            if frame_idx + offset < len(track):
                next_frame_idx, next_obj = track[frame_idx + offset]
                next_pos_ego = np.array(next_obj.get('translation', [0, 0, 0]))
                next_ins = frame_to_ins.get(next_frame_idx) if frame_to_ins else None
                
                if next_ins:
                    next_pos = self.transform_to_world(next_pos_ego, next_ins)
                else:
                    next_pos = next_pos_ego
                
                positions.append(next_pos)
                weights.append(0.5 ** abs(offset))  # 距离越远权重越低
        
        if len(positions) < 2:
            return None
        
        # 计算加权平均位置
        weighted_pos = np.zeros(3)
        total_weight = sum(weights)
        
        for pos, weight in zip(positions, weights):
            weighted_pos += pos * weight
        
        weighted_pos /= total_weight
        
        # 估计运动向量：使用前后位置差
        if frame_idx > 0 and frame_idx < len(track) - 1:
            # 使用对称差分
            prev_pos = positions[1] if len(positions) > 1 else weighted_pos
            next_pos = positions[-1] if len(positions) > 1 else weighted_pos
            motion_vec = next_pos - prev_pos
        elif frame_idx > 0:
            # 只有前帧
            prev_pos = positions[1] if len(positions) > 1 else weighted_pos
            motion_vec = curr_pos - prev_pos
        elif frame_idx < len(track) - 1:
            # 只有后帧
            next_pos = positions[-1] if len(positions) > 1 else weighted_pos
            motion_vec = next_pos - curr_pos
        else:
            return None
        
        return motion_vec

    def check_object(self, obj: Dict) -> List[str]:
        """检查单个对象，返回问题列表"""
        issues = []
        obj_token = obj.get('token', 'unknown')
        size = obj.get('size', [])
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

    def check_low_speed_vehicle_rules(self, obj: Dict, track: List[Tuple[int, Dict]] = None,
                                    frame_idx: int = 0, frame_to_ins: Dict[int, Dict] = None) -> List[str]:
        """
        低速无人车专用检查规则
        
        Args:
            obj: 当前帧的目标对象
            track: 完整轨迹数据
            frame_idx: 当前帧索引
            frame_to_ins: INS数据映射
            
        Returns:
            问题列表
        """
        issues = []
        obj_class = obj.get('attribute_tokens', {}).get('Class', 'unknown').lower()
        
        # 仅检查车辆
        vehicle_classes = ['vehicle', 'car', 'truck', 'bus', 'motorcycle', 'bicycle']
        if not any(vc in obj_class for vc in vehicle_classes):
            return issues
        
        # 1. 检查尺寸合理性 (低速场景优化)
        size = obj.get('size', [])
        if len(size) >= 3:
            l, w, h = size
            # 低速无人车场景下的尺寸范围 (可能包含小型车辆)
            size_rules = {
                'car': {'l': (2, 6), 'w': (1.4, 2.2), 'h': (1.2, 2.0)},
                'truck': {'l': (4, 12), 'w': (1.8, 2.6), 'h': (2.0, 4.0)},
                'bus': {'l': (8, 18), 'w': (2.0, 2.8), 'h': (3.0, 4.5)},
                'motorcycle': {'l': (1.5, 2.5), 'w': (0.6, 1.0), 'h': (1.0, 1.5)},
                'bicycle': {'l': (1.2, 2.0), 'w': (0.5, 0.8), 'h': (0.8, 1.2)}
            }
            
            # 尝试匹配类别
            matched_rule = None
            for category, rules in size_rules.items():
                if category in obj_class:
                    matched_rule = rules
                    break
            
            if matched_rule:
                if not (matched_rule['l'][0] <= l <= matched_rule['l'][1]):
                    issues.append(f"车辆长度异常: {l:.2f}m (期望{matched_rule['l'][0]:.1f}-{matched_rule['l'][1]:.1f}m)")
                if not (matched_rule['w'][0] <= w <= matched_rule['w'][1]):
                    issues.append(f"车辆宽度异常: {w:.2f}m (期望{matched_rule['w'][0]:.1f}-{matched_rule['w'][1]:.1f}m)")
                if not (matched_rule['h'][0] <= h <= matched_rule['h'][1]):
                    issues.append(f"车辆高度异常: {h:.2f}m (期望{matched_rule['h'][0]:.1f}-{matched_rule['h'][1]:.1f}m)")
        
        # 2. 检查姿态角 (低速场景下的合理性)
        rotation = obj.get('rotation', [])
        if len(rotation) == 4:
            roll, pitch, yaw = self.get_euler_angles(rotation)
            
            # 低速场景下放宽姿态角限制，但仍要检测明显异常
            if abs(roll) > 0.7:  # 约40度
                issues.append(f"车辆侧倾异常: {math.degrees(roll):.1f}度")
            if abs(pitch) > 0.5:  # 约28度
                issues.append(f"车辆俯仰异常: {math.degrees(pitch):.1f}度")
        
        # 3. 检查轨迹的低速特性 (如果有轨迹数据)
        if track and len(track) >= 3:
            # 计算平均速度
            positions = []
            timestamps = []
            
            for i, (fid, t_obj) in enumerate(track):
                pos_ego = np.array(t_obj.get('translation', [0, 0, 0]))
                ins = frame_to_ins.get(fid) if frame_to_ins else None
                
                if ins:
                    pos = self.transform_to_world(pos_ego, ins)
                else:
                    pos = pos_ego
                
                positions.append(pos)
                ts = t_obj.get('timestamp', fid * 100000000)
                timestamps.append(ts)
            
            # 计算平均速度
            total_distance = 0
            total_time = (timestamps[-1] - timestamps[0]) / 1e9
            
            for i in range(1, len(positions)):
                dist = np.linalg.norm(positions[i][:2] - positions[i-1][:2])
                total_distance += dist
            
            if total_time > 0:
                avg_speed = total_distance / total_time
                
                # 低速无人车场景：平均速度不应过高
                if avg_speed > 5.0:  # 18 km/h
                    issues.append(f"轨迹平均速度过高: {avg_speed:.2f}m/s (可能不是低速场景数据)")
                
                # 检查速度稳定性 (低速场景应较为平稳)
                velocities = []
                for i in range(1, len(positions)):
                    dt = (timestamps[i] - timestamps[i-1]) / 1e9
                    if dt > 0:
                        vel = np.linalg.norm(positions[i][:2] - positions[i-1][:2]) / dt
                        velocities.append(vel)
                
                if velocities:
                    speed_std = np.std(velocities)
                    avg_vel = np.mean(velocities)
                    
                    # 速度标准差不应过大 (相对于平均速度)
                    if avg_vel > 0 and speed_std / avg_vel > 0.8:
                        issues.append(f"速度波动过大: 标准差{speed_std:.2f}m/s (平均{avg_vel:.2f}m/s)")
        
        return issues