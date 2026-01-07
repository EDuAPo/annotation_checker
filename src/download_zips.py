"""
功能说明：
此脚本用于根据本地 JSON 文件名，批量从指定 URL 模板下载对应的 ZIP 文件。
需要用户手动提供下载链接模板和 Cookie 信息。
"""

import os
import requests
import logging
from pathlib import Path
import sys

# ================= 配置区域 =================
# 1. JSON 文件所在目录
JSON_DIR = "/home/zgw/Downloads/12.30导出/线段/"

# 2. ZIP 文件下载保存目录
SAVE_DIR = "/media/zgw/T7/from_rere/1230_lines/"

# 3. DataWeave API 配置
# API 接口地址 (通常固定)
API_URL = "https://dataweave.enableai.cn/api/v4/file/url"

# DataWeave 文件路径模板
# 请根据您的实际项目路径修改，保留 {filename}
# 参考 Referer: dataweave://my/TO_RERE
# 更新路径: dataweave://my/TO_RERE/盲区数据/{filename}
DATAWEAVE_PATH_TEMPLATE = "dataweave://my/TO_RERE/未上传平台/{filename}"

# 4. 认证信息 (必须填写!)
# 从 Request Headers 中复制 Authorization 字段 (Bearer ...)
AUTH_TOKEN = "Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ0b2tlbl90eXBlIjoiYWNjZXNzIiwic3ViIjoidjRCUWlhIiwiZXhwIjoxNzY3MTc5OTAzLCJuYmYiOjE3NjcxNzYzMDN9.kPYBI5ucZ1wLLxmBXd0TqOcuCj4OfDESOA_PzJk3rVg"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36",
    "Content-Type": "application/json",
    "Authorization": AUTH_TOKEN,
    "Origin": "https://dataweave.enableai.cn",
    "Referer": "https://dataweave.enableai.cn/home?path=dataweave%3A%2F%2Fmy%2FTO_RERE%2F%25E6%259C%25AA%25E4%25B8%258A%25E4%25BC%25A0%25E5%25B9%25B3%25E5%258F%25B0",
    "sec-ch-ua-platform": '"Linux"',
    "sec-ch-ua": '"Google Chrome";v="143", "Chromium";v="143", "Not A(Brand";v="24"',
    "sec-ch-ua-mobile": "?0",
    # "Cookie": "...", # 如果有 Token，Cookie 通常不是必须的，但如果失败可以补上
}
# ===========================================

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger(__name__)

def get_real_download_url(filename):
    """通过 DataWeave API 获取真实的 S3 下载链接"""
    try:
        # 构建请求体
        dw_path = DATAWEAVE_PATH_TEMPLATE.format(filename=filename)
        # 根据报错 'Uris cannot be empty'，参数名应为 uris，且通常为列表
        payload = {"uris": [dw_path]}
        
        logger.info(f"    请求 API: {API_URL}")
        logger.info(f"    Payload: {payload}")
        
        # 发送 POST 请求
        r = requests.post(API_URL, json=payload, headers=HEADERS, timeout=15)
        r.raise_for_status()
        
        data = r.json()

        # 检查 API 返回的错误码
        if isinstance(data, dict) and data.get("code") != 0:
            msg = data.get("msg", "")
            logger.error(f"    API 返回错误 (Code: {data.get('code')}): {msg}")
            if "Login required" in msg or data.get("code") == 401 or data.get("code") == 40081:
                logger.critical("    !!! 认证失败: Token 已过期或无效 !!!")
                logger.critical("    请在浏览器中登录 DataWeave，按 F12 打开开发者工具 -> 网络(Network)，")
                logger.critical("    找到任意 API 请求，复制 Request Headers 中的 Authorization 值，")
                logger.critical("    并更新脚本中的 AUTH_TOKEN 变量。")
                sys.exit(1) # 遇到认证错误直接退出，避免后续重复失败
            return None

        # 解析响应
        # 预期结构可能是 {"data": ["url1", ...]} 或 {"data": {"uri": "url"}}
        # 实际结构: {'code': 0, 'data': {'urls': [{'url': '...'}]}, 'msg': ''}
        
        if isinstance(data, dict):
            url_data = data.get("data")
            
            # 情况 0: data 包含 urls 列表 (最新发现的结构)
            if isinstance(url_data, dict) and "urls" in url_data:
                urls_list = url_data["urls"]
                if isinstance(urls_list, list) and len(urls_list) > 0:
                    first_url_obj = urls_list[0]
                    if isinstance(first_url_obj, dict) and "url" in first_url_obj:
                        return first_url_obj["url"]
            
            # 情况 1: data 是列表 (对应 uris 输入)
            if isinstance(url_data, list) and len(url_data) > 0:
                first_item = url_data[0]
                if isinstance(first_item, str) and first_item.startswith("http"):
                    return first_item
                if isinstance(first_item, dict) and "url" in first_item:
                    return first_item["url"]
            
            # 情况 2: data 是字典 (可能是 URI 到 URL 的映射)
            if isinstance(url_data, dict):
                if dw_path in url_data:
                    return url_data[dw_path]
                if "url" in url_data:
                    return url_data["url"]
            
            # 情况 3: data 是字符串
            if isinstance(url_data, str) and url_data.startswith("http"):
                return url_data
                
        logger.error(f"    API 响应格式无法解析: {data}")
        return None
            
    except Exception as e:
        logger.error(f"    获取下载链接失败: {e}")
        return None

def download_file(filename, save_path):
    try:
        # 1. 获取真实 URL
        real_url = get_real_download_url(filename)
        if not real_url:
            return False
            
        logger.info(f"    获取到下载链接，开始下载...")
            
        # 2. 下载文件 (注意：下载 S3 链接通常不需要 Authorization header，或者需要去掉它以免冲突)
        # 我们创建一个新的 headers，只保留 User-Agent
        download_headers = {"User-Agent": HEADERS["User-Agent"]}
        
        with requests.get(real_url, headers=download_headers, stream=True, timeout=60) as r:
            r.raise_for_status()
            
            total_size = int(r.headers.get('content-length', 0))
            
            with open(save_path, 'wb') as f:
                if total_size == 0:
                    f.write(r.content)
                else:
                    downloaded = 0
                    for chunk in r.iter_content(chunk_size=8192):
                        if chunk:
                            f.write(chunk)
                            downloaded += len(chunk)
                            percent = (downloaded / total_size) * 100
                            sys.stdout.write(f"\r    下载进度: {percent:.1f}%")
                            sys.stdout.flush()
            print() 
            return True
            
    except Exception as e:
        print()
        logger.error(f"    下载失败: {e}")
        if os.path.exists(save_path):
            os.remove(save_path)
        return False

def main():
    json_path = Path(JSON_DIR)
    save_path = Path(SAVE_DIR)
    
    if not json_path.exists():
        logger.error(f"JSON 目录不存在: {JSON_DIR}")
        return
    
    save_path.mkdir(parents=True, exist_ok=True)
    
    # 获取所有 JSON 文件
    json_files = list(json_path.glob("*.json"))
    if not json_files:
        logger.warning("未找到 JSON 文件")
        return
        
    logger.info(f"找到 {len(json_files)} 个 JSON 文件，准备下载对应的 ZIP")
    
    success_count = 0
    skip_count = 0
    fail_count = 0
    
    for i, json_file in enumerate(json_files):
        stem = json_file.stem # 文件名不含后缀
        zip_name = f"{stem}.zip"
        target_file = save_path / zip_name
        
        logger.info(f"[{i+1}/{len(json_files)}] 处理: {stem}")
        
        # 检查是否已存在
        if target_file.exists() and target_file.stat().st_size > 0:
            logger.info(f"    文件已存在，跳过")
            skip_count += 1
            continue
            
        if download_file(zip_name, target_file):
            success_count += 1
        else:
            fail_count += 1
            
    logger.info("="*30)
    logger.info(f"任务完成 Summary:")
    logger.info(f"成功: {success_count}")
    logger.info(f"跳过: {skip_count}")
    logger.info(f"失败: {fail_count}")

if __name__ == "__main__":
    # 需要安装 requests: pip install requests
    main()
