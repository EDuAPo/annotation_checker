"""
飞书多维表格数据追踪模块
========================

用于将数据处理情况统计到飞书表格中。
根据 json_dir 路径自动识别数据属性（拉框、线段、贴边等），并在表格中标记对应列。

飞书多维表格 API 文档：
https://open.feishu.cn/document/server-docs/docs/bitable-v1/bitable-overview

使用前需要配置：
1. 在飞书开放平台创建应用
2. 获取 App ID 和 App Secret
3. 在 configs/feishu_config.yaml 中配置凭证和表格信息
"""

import time
import logging
import requests
from pathlib import Path
from typing import Optional, Dict, List, Any
import yaml

logger = logging.getLogger(__name__)


# 配置文件路径
FEISHU_CONFIG_PATH = "configs/feishu_config.yaml"

# 数据属性关键词映射
# 根据 json_dir 路径中的关键词识别数据属性
ATTRIBUTE_KEYWORDS = {
    "拉框": ["拉框", "框标注", "box", "bbox"],
    "线段": ["线段", "line", "polyline", "划线"],
    "贴边": ["贴边", "edge", "边缘", "boundary"],
    "盲区": ["盲区", "blind", "遮挡"],
    "关键帧": ["关键帧", "keyframe", "key_frame"],
}


class FeishuTracker:
    """飞书表格数据追踪器"""
    
    def __init__(self, config_path: str = None):
        """
        初始化飞书追踪器
        
        Args:
            config_path: 配置文件路径，默认使用 configs/feishu_config.yaml
        """
        self.config_path = config_path or FEISHU_CONFIG_PATH
        self.config = self._load_config()
        
        # 飞书 API 配置
        self.app_id = self.config.get("app_id", "")
        self.app_secret = self.config.get("app_secret", "")
        
        # 表格配置
        # 从链接 https://ai.feishu.cn/wiki/QrMxwkT9NiBBSRkQ4Vlc8mkSn3g 提取
        self.wiki_token = self.config.get("wiki_token", "QrMxwkT9NiBBSRkQ4Vlc8mkSn3g")
        self.app_token = self.config.get("app_token", "")  # 多维表格的 app_token
        self.table_id = self.config.get("table_id", "")    # 表格 ID
        
        # 列名映射 (字段名 -> 字段 ID)
        self.field_mapping = self.config.get("field_mapping", {})
        # 字段类型缓存 (字段名 -> 字段类型和属性)
        self.field_types = {}  # {field_name: {"type": int, "property": dict}}
        # 字段名标准化映射（支持常用别名自动适配）
        self._field_alias = {
            "名称": ["名称", "数据包名称", "name"],
            "关键帧数": ["关键帧数量", "关键帧数", "keyframe", "key_frame"],
            "标注情况": ["标注情况"],
            "拉框属性": ["拉框", "拉框属性"],
            "线段属性": ["线段", "线段属性"],
            "贴边属性": ["贴边", "贴边属性"],
            "盲区属性": ["盲区", "盲区属性"],
            "更新时间": ["更新时间"],
        }
        
        # Token 缓存
        self._tenant_access_token = None
        self._token_expires_at = 0
        
        # API 基础 URL
        self.api_base = "https://open.feishu.cn/open-apis"
    
    def _load_config(self) -> dict:
        """加载配置文件"""
        # 尝试从项目根目录加载
        config_paths = [
            self.config_path,
            Path(__file__).parent.parent / self.config_path,
            Path.cwd() / self.config_path,
        ]
        
        for path in config_paths:
            path = Path(path)
            if path.exists():
                with open(path, 'r', encoding='utf-8') as f:
                    config = yaml.safe_load(f) or {}
                logger.info(f"已加载飞书配置: {path}")
                return config
        
        logger.warning(f"未找到飞书配置文件，将使用默认配置")
        return {}
    
    def _get_tenant_access_token(self) -> str:
        """
        获取 tenant_access_token
        
        飞书 API 认证需要使用 tenant_access_token，有效期 2 小时
        """
        # 检查缓存是否有效
        if self._tenant_access_token and time.time() < self._token_expires_at - 300:
            return self._tenant_access_token
        
        if not self.app_id or not self.app_secret:
            raise ValueError("请在 configs/feishu_config.yaml 中配置 app_id 和 app_secret")
        
        url = f"{self.api_base}/auth/v3/tenant_access_token/internal"
        payload = {
            "app_id": self.app_id,
            "app_secret": self.app_secret
        }
        
        try:
            response = requests.post(url, json=payload, timeout=10)
            data = response.json()
            
            if data.get("code") == 0:
                # 处理不同的响应格式
                # 格式1: {"code":0, "data": {"tenant_access_token": "...", "expire": 7200}}
                # 格式2: {"code":0, "tenant_access_token": "...", "expire": 7200, "msg": "ok"}
                token = None
                expire = 7200
                
                # 首先尝试格式1（嵌套在data中）
                if isinstance(data.get("data"), dict):
                    token = data.get("data", {}).get("tenant_access_token")
                    expire = data.get("data", {}).get("expire", expire)
                
                # 如果格式1失败，尝试格式2（直接在根层级）
                if not token:
                    token = data.get("tenant_access_token")
                    expire = data.get("expire", expire)
                
                if not token:
                    raise Exception(f"获取 Token 失败，响应缺少 tenant_access_token: {data}")
                
                self._tenant_access_token = token
                self._token_expires_at = time.time() + int(expire)
                logger.info("飞书 Token 获取成功")
                return self._tenant_access_token
            else:
                raise Exception(f"获取 Token 失败: {data}")
        except requests.RequestException as e:
            raise Exception(f"请求飞书 API 失败: {e}")
    
    def _get_headers(self) -> dict:
        """获取 API 请求头"""
        token = self._get_tenant_access_token()
        return {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json"
        }
    
    def detect_attributes(self, json_dir: str) -> List[str]:
        """
        根据 json_dir 路径检测数据属性
        
        Args:
            json_dir: JSON 文件夹路径，如 "/media/zgw/T7/1.6线拉框导出/盲区数据/"
            
        Returns:
            检测到的属性列表，如 ["拉框", "盲区"]
        """
        json_dir_str = str(json_dir).lower()
        detected = []
        
        for attr_name, keywords in ATTRIBUTE_KEYWORDS.items():
            for keyword in keywords:
                if keyword.lower() in json_dir_str:
                    if attr_name not in detected:
                        detected.append(attr_name)
                    break
        
        logger.info(f"从路径 '{json_dir}' 检测到属性: {detected}")
        return detected
    
    def get_table_info(self) -> dict:
        """
        获取多维表格信息
        
        Returns:
            表格信息字典
        """
        if not self.app_token:
            raise ValueError("请在配置中设置 app_token (多维表格的 app_token)")
        
        url = f"{self.api_base}/bitable/v1/apps/{self.app_token}"
        
        try:
            response = requests.get(url, headers=self._get_headers(), timeout=10)
            data = response.json()
            
            if data.get("code") == 0:
                return data.get("data", {})
            else:
                raise Exception(f"获取表格信息失败: {data}")
        except requests.RequestException as e:
            raise Exception(f"请求失败: {e}")
    
    def get_tables(self) -> List[dict]:
        """
        获取多维表格中的所有数据表
        
        Returns:
            数据表列表
        """
        if not self.app_token:
            raise ValueError("请在配置中设置 app_token")
        
        url = f"{self.api_base}/bitable/v1/apps/{self.app_token}/tables"
        
        try:
            response = requests.get(url, headers=self._get_headers(), timeout=10)
            data = response.json()
            
            if data.get("code") == 0:
                return data.get("data", {}).get("items", [])
            else:
                raise Exception(f"获取数据表列表失败: {data}")
        except requests.RequestException as e:
            raise Exception(f"请求失败: {e}")
    
    def get_fields(self, table_id: str = None) -> List[dict]:
        """
        获取表格的所有字段 (列)
        
        Args:
            table_id: 数据表 ID，默认使用配置中的 table_id
            
        Returns:
            字段列表
        """
        table_id = table_id or self.table_id
        if not self.app_token or not table_id:
            raise ValueError("请在配置中设置 app_token 和 table_id")
        
        url = f"{self.api_base}/bitable/v1/apps/{self.app_token}/tables/{table_id}/fields"
        
        try:
            response = requests.get(url, headers=self._get_headers(), timeout=10)
            data = response.json()
            
            if data.get("code") == 0:
                fields = data.get("data", {}).get("items", [])
                # 更新字段映射和类型缓存
                for field in fields:
                    field_name = field.get("field_name")
                    field_id = field.get("field_id")
                    field_type = field.get("type")
                    field_property = field.get("property", {})
                    
                    if field_name and field_id:
                        self.field_mapping[field_name] = field_id
                        self.field_types[field_name] = {
                            "type": field_type,
                            "property": field_property
                        }
                return fields
            else:
                raise Exception(f"获取字段列表失败: {data}")
        except requests.RequestException as e:
            raise Exception(f"请求失败: {e}")
    
    def search_record(self, name: str, table_id: str = None) -> Optional[dict]:
        """
        按名称搜索记录
        
        Args:
            name: 数据名称 (用于搜索)
            table_id: 数据表 ID
            
        Returns:
            找到的记录，未找到则返回 None
        """
        table_id = table_id or self.table_id
        if not self.app_token or not table_id:
            raise ValueError("请在配置中设置 app_token 和 table_id")
        
        # 确保有字段映射
        if not self.field_mapping:
            self.get_fields(table_id)
        
        # 获取名称字段
        name_field_name = None
        name_field_id = None
        for field_name in self.field_mapping:
            if field_name in ["名称", "数据包名称", "name", "Name"]:
                name_field_name = field_name
                name_field_id = self.field_mapping[field_name]
                break
        if not name_field_id:
            # 尝试使用第一个文本字段
            logger.warning("未找到名称字段，将尝试获取所有记录进行匹配")
        
        url = f"{self.api_base}/bitable/v1/apps/{self.app_token}/tables/{table_id}/records/search"
        
        # 构建筛选条件
        payload = {
            "page_size": 100,
        }
        
        if name_field_id:
            payload["filter"] = {
                "conjunction": "and",
                "conditions": [
                    {
                        "field_name": name_field_name,  # 使用正确的字段名
                        "operator": "is",
                        "value": [name]
                    }
                ]
            }
        
        try:
            response = requests.post(url, headers=self._get_headers(), json=payload, timeout=10)
            data = response.json()
            
            if data.get("code") == 0:
                items = data.get("data", {}).get("items", [])
                if items:
                    return items[0]
                return None
            else:
                # 如果搜索失败，尝试列出所有记录手动匹配
                logger.warning(f"搜索失败: {data.get('msg')}, 尝试手动匹配")
                return self._find_record_by_name(name, table_id)
        except requests.RequestException as e:
            logger.error(f"搜索记录失败: {e}")
            return None
    
    def _find_record_by_name(self, name: str, table_id: str) -> Optional[dict]:
        """手动遍历查找记录"""
        url = f"{self.api_base}/bitable/v1/apps/{self.app_token}/tables/{table_id}/records"
        params = {"page_size": 500}
        
        try:
            response = requests.get(url, headers=self._get_headers(), params=params, timeout=15)
            data = response.json()
            
            if data.get("code") == 0:
                items = data.get("data", {}).get("items", [])
                for item in items:
                    fields = item.get("fields", {})
                    # 检查各种可能的名称字段
                    for field_name in self.field_mapping:
                        if field_name in ["名称", "数据包名称", "name", "Name"]:
                            field_value = fields.get(field_name)
                            if field_value:
                                # 处理不同类型的字段值
                                if isinstance(field_value, str) and name in field_value:
                                    return item
                                elif isinstance(field_value, list) and any(name in str(v) for v in field_value):
                                    return item
                return None
        except Exception as e:
            logger.error(f"遍历查找失败: {e}")
            return None
    
    def _get_record_by_id(self, record_id: str, table_id: str = None) -> Optional[dict]:
        """
        根据记录ID获取记录详情
        
        Args:
            record_id: 记录ID
            table_id: 数据表ID
            
        Returns:
            记录数据
        """
        table_id = table_id or self.table_id
        if not self.app_token or not table_id:
            return None
        
        url = f"{self.api_base}/bitable/v1/apps/{self.app_token}/tables/{table_id}/records/{record_id}"
        
        try:
            response = requests.get(url, headers=self._get_headers(), timeout=10)
            data = response.json()
            
            if data.get("code") == 0:
                return data.get("data", {}).get("record")
            else:
                logger.warning(f"获取记录失败: {data}")
                return None
        except requests.RequestException as e:
            logger.error(f"获取记录请求失败: {e}")
            return None
    
    def create_record(self, name: str, attributes: List[str], table_id: str = None, extra_fields: Dict[str, Any] = None) -> Optional[str]:
        """
        创建新记录
        
        Args:
            name: 数据名称
            attributes: 要标记的属性列表，如 ["拉框", "盲区"]
            table_id: 数据表 ID
            extra_fields: 额外字段，如 {"关键帧数量": 100}
            
        Returns:
            创建的记录 ID
        """
        table_id = table_id or self.table_id
        if not self.app_token or not table_id:
            raise ValueError("请在配置中设置 app_token 和 table_id")
        
        # 确保有字段映射
        if not self.field_mapping:
            self.get_fields(table_id)
        
        url = f"{self.api_base}/bitable/v1/apps/{self.app_token}/tables/{table_id}/records"
        
        # 构建记录数据，全部用 field_name 作为 key
        fields = {}
        # 名称
        name_field = self._get_field_name_by_alias("名称")
        if name_field:
            fields[name_field] = name
        # 更新时间（毫秒级时间戳，用于日期字段）
        update_field = self._get_field_name_by_alias("更新时间")
        if update_field:
            fields[update_field] = int(time.time() * 1000)
        # 属性复选框
        for attr in attributes:
            attr_field = self._get_field_name_by_alias(attr)
            if attr_field:
                fields[attr_field] = True
        # 额外字段（如关键帧数量）
        if extra_fields:
            for k, v in extra_fields.items():
                k_field = self._get_field_name_by_alias(k)
                if k_field:
                    fields[k_field] = self._convert_field_value(k_field, v)
        payload = {"fields": fields}

        try:
            response = requests.post(url, headers=self._get_headers(), json=payload, timeout=10)
            data = response.json()
            
            if data.get("code") == 0:
                record_id = data.get("data", {}).get("record", {}).get("record_id")
                logger.info(f"创建记录成功: {name} (ID: {record_id})")
                return record_id
            else:
                logger.error(f"创建记录失败: {data}")
                return None
        except Exception as e:
            logger.error(f"创建记录请求失败: {e}")
            return None
    def _convert_field_value(self, field_name: str, value: Any) -> Any:
        """
        根据字段类型转换字段值
        
        Args:
            field_name: 字段名
            value: 原始值
            
        Returns:
            转换后的值
        """
        if field_name not in self.field_types:
            # 如果没有类型信息，尝试获取
            try:
                self.get_fields()
            except Exception:
                pass
        
        field_info = self.field_types.get(field_name, {})
        field_type = field_info.get("type")
        field_property = field_info.get("property", {})
        
        # 根据字段类型转换值
        if field_type == 4:  # 多选字段
            return self._convert_multi_select_value(field_name, value, field_property)
        elif field_type == 7:  # 复选框字段
            return bool(value)
        elif field_type == 5:  # 日期字段
            if isinstance(value, (int, float)):
                return int(value)  # 毫秒级时间戳
            return int(time.time() * 1000)
        elif field_type == 3:  # 数字字段
            try:
                return float(value) if '.' in str(value) else int(value)
            except (ValueError, TypeError):
                return value
        elif field_type in [1, 2]:  # 文本字段
            return str(value)
        else:
            # 未知类型，直接返回原值
            return value
    
    def _convert_multi_select_value(self, field_name: str, value: Any, field_property: dict) -> List[str]:
        """
        转换多选字段的值为选项ID列表
        
        Args:
            field_name: 字段名
            value: 原始值 (可以是字符串、列表等)
            field_property: 字段属性，包含选项信息
            
        Returns:
            选项ID列表
        """
        options = field_property.get("options", [])
        if not options:
            return []
        
        # 创建选项名到ID的映射
        name_to_id = {opt.get("name"): opt.get("id") for opt in options if isinstance(opt, dict)}
        
        # 处理不同的输入格式
        if isinstance(value, str):
            # 单个选项名
            option_id = name_to_id.get(value.strip())
            return [option_id] if option_id else []
        elif isinstance(value, list):
            # 多个选项名列表
            result = []
            for item in value:
                if isinstance(item, str):
                    option_id = name_to_id.get(item.strip())
                    if option_id:
                        result.append(option_id)
                elif isinstance(item, dict) and "name" in item:
                    option_id = name_to_id.get(item["name"])
                    if option_id:
                        result.append(option_id)
            return result
        else:
            return []
    
    def _get_field_name_by_alias(self, name: str) -> str:
        """
        根据字段名或别名获取实际存在的字段名
        """
        for std, aliases in self._field_alias.items():
            if name == std or name in aliases:
                for alias in aliases:
                    if alias in self.field_mapping:
                        return alias
        # 直接查找
        for field_name in self.field_mapping:
            if field_name == name:
                return field_name
        return ""
        
    def update_record(self, record_id: str, attributes: List[str], table_id: str = None, extra_fields: Dict[str, Any] = None) -> bool:
        """
        更新记录的属性字段
        
        Args:
            record_id: 记录 ID
            attributes: 要标记的属性列表
            table_id: 数据表 ID
            extra_fields: 额外字段，如 {"关键帧数量": 100}
            
        Returns:
            是否更新成功
        """
        table_id = table_id or self.table_id
        if not self.app_token or not table_id:
            raise ValueError("请在配置中设置 app_token 和 table_id")
        
        # 首先获取现有记录的数据，避免覆盖其他属性
        existing_record = self._get_record_by_id(record_id, table_id)
        existing_fields = existing_record.get("fields", {}) if existing_record else {}
        
        url = f"{self.api_base}/bitable/v1/apps/{self.app_token}/tables/{table_id}/records/{record_id}"
        fields = {}
        
        # 更新时间（毫秒级时间戳，用于日期字段）
        update_field = self._get_field_name_by_alias("更新时间")
        if update_field:
            fields[update_field] = int(time.time() * 1000)
        
        # 设置属性字段为勾选状态（保留现有属性，只更新指定的）
        for attr in attributes:
            attr_field = self._get_field_name_by_alias(attr)
            if attr_field:
                fields[attr_field] = True
        
        # 添加额外字段（如关键帧数量）
        if extra_fields:
            for k, v in extra_fields.items():
                k_field = self._get_field_name_by_alias(k)
                if k_field:
                    fields[k_field] = self._convert_field_value(k_field, v)
        
        payload = {"fields": fields}
        
        try:
            response = requests.put(url, headers=self._get_headers(), json=payload, timeout=10)
            data = response.json()
            
            if data.get("code") == 0:
                logger.info(f"更新记录成功: {record_id}, 属性: {attributes}")
                return True
            else:
                logger.error(f"更新记录失败: {data}")
                return False
        except requests.RequestException as e:
            logger.error(f"更新记录请求失败: {e}")
            return False
    
    def track_data(self, names: List[str], json_dir: str, table_id: str = None, 
                   data_info: Dict[str, Dict[str, Any]] = None) -> dict:
        """
        追踪数据处理情况 (核心方法)
        
        根据 json_dir 路径检测数据属性，然后在表格中记录每个数据名称，并标记对应的属性列。
        
        Args:
            names: 数据名称列表 (通常是 ZIP 文件名或数据集名称)
            json_dir: JSON 文件夹路径，用于检测数据属性
            table_id: 数据表 ID
            data_info: 每个数据的额外信息，格式 {name: {"关键帧数量": 100, ...}}
            
        Returns:
            处理结果统计
        """
        result = {
            "created": [],
            "updated": [],
            "failed": [],
            "attributes": [],
            "total_keyframes": 0,
        }
        
        data_info = data_info or {}
        
        # 检测数据属性
        attributes = self.detect_attributes(json_dir)
        result["attributes"] = attributes
        
        if not attributes and not data_info:
            logger.warning("未检测到任何数据属性，跳过飞书表格更新")
            return result
        
        logger.info(f"开始更新飞书表格，共 {len(names)} 条数据，属性: {attributes}")
        
        for name in names:
            try:
                # 去掉 .zip 后缀
                name_clean = name.replace(".zip", "").replace(".json", "")
                
                # 获取该数据的额外信息
                extra_fields = data_info.get(name_clean, {}) or data_info.get(name, {})
                
                # 统计关键帧总数
                if "关键帧数量" in extra_fields:
                    result["total_keyframes"] += extra_fields["关键帧数量"]
                
                # 搜索是否已存在
                record = self.search_record(name_clean, table_id)
                
                if record:
                    # 更新现有记录
                    record_id = record.get("record_id")
                    if self.update_record(record_id, attributes, table_id, extra_fields):
                        result["updated"].append(name_clean)
                    else:
                        result["failed"].append(name_clean)
                else:
                    # 创建新记录
                    record_id = self.create_record(name_clean, attributes, table_id, extra_fields)
                    if record_id:
                        result["created"].append(name_clean)
                    else:
                        result["failed"].append(name_clean)
                        
            except Exception as e:
                logger.error(f"处理 {name} 失败: {e}")
                result["failed"].append(name)
        
        # 输出统计
        keyframes_info = f", 总关键帧: {result['total_keyframes']}" if result['total_keyframes'] > 0 else ""
        logger.info(f"飞书表格更新完成: 创建 {len(result['created'])} 条, "
                   f"更新 {len(result['updated'])} 条, 失败 {len(result['failed'])} 条{keyframes_info}")
        
        return result
    
    def batch_create_records(self, records: List[dict], table_id: str = None) -> dict:
        """
        批量创建记录
        
        Args:
            records: 记录列表，每条记录是一个字典
            table_id: 数据表 ID
            
        Returns:
            处理结果
        """
        table_id = table_id or self.table_id
        if not self.app_token or not table_id:
            raise ValueError("请在配置中设置 app_token 和 table_id")
        
        url = f"{self.api_base}/bitable/v1/apps/{self.app_token}/tables/{table_id}/records/batch_create"
        
        payload = {
            "records": [{"fields": r} for r in records]
        }
        
        try:
            response = requests.post(url, headers=self._get_headers(), json=payload, timeout=30)
            data = response.json()
            
            if data.get("code") == 0:
                created = data.get("data", {}).get("records", [])
                logger.info(f"批量创建成功: {len(created)} 条记录")
                return {"success": True, "created": len(created)}
            else:
                logger.error(f"批量创建失败: {data}")
                return {"success": False, "error": data.get("msg")}
        except requests.RequestException as e:
            logger.error(f"批量创建请求失败: {e}")
            return {"success": False, "error": str(e)}


def create_default_config():
    """创建默认配置文件"""
    config_template = """# 飞书多维表格配置
# ===================

# 飞书开放平台应用凭证
# 请在 https://open.feishu.cn/ 创建应用并获取
app_id: ""
app_secret: ""

# 多维表格配置
# 从表格 URL 中获取: https://ai.feishu.cn/wiki/QrMxwkT9NiBBSRkQ4Vlc8mkSn3g
wiki_token: "QrMxwkT9NiBBSRkQ4Vlc8mkSn3g"

# 多维表格的 app_token (需要从表格页面获取，或者使用 API 获取)
# 如果知识库中嵌入了多维表格，需要先获取该表格的 app_token
app_token: ""

# 数据表 ID (一个多维表格可以有多个数据表)
table_id: ""

# 字段映射 (字段名 -> 字段ID)
# 首次运行后会自动填充
field_mapping:
  名称: ""
  拉框: ""
  线段: ""
  贴边: ""
  盲区: ""
  更新时间: ""

# 数据属性关键词配置
# 用于从 json_dir 路径中自动识别数据属性
attribute_keywords:
  拉框: ["拉框", "框标注", "box", "bbox"]
  线段: ["线段", "line", "polyline", "划线"]
  贴边: ["贴边", "edge", "边缘", "boundary"]
  盲区: ["盲区", "blind", "遮挡"]
"""
    
    config_path = Path(__file__).parent.parent / FEISHU_CONFIG_PATH
    config_path.parent.mkdir(parents=True, exist_ok=True)
    
    if not config_path.exists():
        with open(config_path, 'w', encoding='utf-8') as f:
            f.write(config_template)
        print(f"已创建默认配置文件: {config_path}")
        print("请填写 app_id, app_secret, app_token 和 table_id 后重新运行")
    else:
        print(f"配置文件已存在: {config_path}")


def main():
    """命令行入口"""
    import argparse
    
    parser = argparse.ArgumentParser(description="飞书表格数据追踪工具")
    parser.add_argument('--init', action='store_true', help='创建默认配置文件')
    parser.add_argument('--test', action='store_true', help='测试连接')
    parser.add_argument('--list-tables', action='store_true', help='列出所有数据表')
    parser.add_argument('--list-fields', action='store_true', help='列出表格字段')
    parser.add_argument('--json_dir', type=str, help='JSON 目录路径 (用于检测属性)')
    parser.add_argument('--name', type=str, help='要追踪的数据名称')
    
    args = parser.parse_args()
    
    if args.init:
        create_default_config()
        return
    
    try:
        tracker = FeishuTracker()
        
        if args.test:
            print("测试飞书连接...")
            token = tracker._get_tenant_access_token()
            print(f"✓ Token 获取成功: {token[:20]}...")
            
        if args.list_tables:
            print("获取数据表列表...")
            tables = tracker.get_tables()
            print(f"找到 {len(tables)} 个数据表:")
            for table in tables:
                print(f"  - {table.get('name')} (ID: {table.get('table_id')})")
        
        if args.list_fields:
            print("获取字段列表...")
            fields = tracker.get_fields()
            print(f"找到 {len(fields)} 个字段:")
            for field in fields:
                print(f"  - {field.get('field_name')} (ID: {field.get('field_id')}, 类型: {field.get('type')})")
        
        if args.json_dir:
            attrs = tracker.detect_attributes(args.json_dir)
            print(f"检测到属性: {attrs}")
            
            if args.name:
                result = tracker.track_data([args.name], args.json_dir)
                print(f"追踪结果: {result}")
                
    except Exception as e:
        print(f"错误: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()
