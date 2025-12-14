"""
工具函数模块
包含项目中常用的工具函数
"""

import os
import logging
import hashlib
from pathlib import Path
from typing import Any, Dict, List, Optional, Union
from datetime import datetime
from loguru import logger
import json


def setup_logging(log_level: str = "INFO", log_file: Optional[str] = None):
    """
    设置日志配置
    
    Args:
        log_level: 日志级别
        log_file: 日志文件路径
    """
    # 移除默认处理器
    logger.remove()
    
    # 添加控制台输出
    logger.add(
        sink=lambda msg: print(msg, end=""),
        level=log_level,
        format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | "
               "<level>{level: <8}</level> | "
               "<cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - "
               "<level>{message}</level>",
        colorize=True
    )
    
    # 添加文件输出
    if log_file:
        log_path = Path(log_file)
        log_path.parent.mkdir(parents=True, exist_ok=True)
        logger.add(
            sink=log_file,
            level=log_level,
            format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {name}:{function}:{line} - {message}",
            rotation="10 MB",
            retention="7 days",
            encoding="utf-8"
        )


def create_directories(dir_paths: Union[str, List[str]]):
    """
    创建目录
    
    Args:
        dir_paths: 目录路径或路径列表
    """
    if isinstance(dir_paths, str):
        dir_paths = [dir_paths]
    
    for dir_path in dir_paths:
        Path(dir_path).mkdir(parents=True, exist_ok=True)
        logger.info(f"目录创建成功: {dir_path}")


def calculate_file_hash(file_path: str) -> str:
    """
    计算文件MD5哈希值
    
    Args:
        file_path: 文件路径
        
    Returns:
        文件的MD5哈希值
    """
    hash_md5 = hashlib.md5()
    
    try:
        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hash_md5.update(chunk)
        return hash_md5.hexdigest()
    except Exception as e:
        logger.error(f"计算文件哈希失败 {file_path}: {e}")
        return ""


def save_json(data: Dict[str, Any], file_path: str, indent: int = 2):
    """
    保存数据为JSON文件
    
    Args:
        data: 要保存的数据
        file_path: 文件路径
        indent: JSON缩进
    """
    try:
        Path(file_path).parent.mkdir(parents=True, exist_ok=True)
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=indent)
        logger.info(f"JSON文件保存成功: {file_path}")
    except Exception as e:
        logger.error(f"JSON文件保存失败 {file_path}: {e}")
        raise


def load_json(file_path: str) -> Dict[str, Any]:
    """
    加载JSON文件
    
    Args:
        file_path: 文件路径
        
    Returns:
        加载的数据
    """
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        logger.info(f"JSON文件加载成功: {file_path}")
        return data
    except Exception as e:
        logger.error(f"JSON文件加载失败 {file_path}: {e}")
        return {}


def get_timestamp() -> str:
    """
    获取当前时间戳字符串
    
    Returns:
        格式化的时间戳
    """
    return datetime.now().strftime("%Y%m%d_%H%M%S")


def detect_file_encoding(file_path: str) -> str:
    """
    检测文件编码
    
    Args:
        file_path: 文件路径
        
    Returns:
        检测到的编码
    """
    try:
        import chardet
        
        with open(file_path, 'rb') as f:
            raw_data = f.read()
            result = chardet.detect(raw_data)
            encoding = result.get('encoding', 'utf-8')
            confidence = result.get('confidence', 0)
            
        logger.info(f"文件编码检测 {file_path}: {encoding} (置信度: {confidence:.2f})")
        return encoding
    except ImportError:
        logger.warning("chardet未安装，使用默认编码utf-8")
        return 'utf-8'
    except Exception as e:
        logger.error(f"编码检测失败 {file_path}: {e}")
        return 'utf-8'


def truncate_text(text: str, max_length: int = 100, suffix: str = "...") -> str:
    """
    截断文本
    
    Args:
        text: 原始文本
        max_length: 最大长度
        suffix: 后缀
        
    Returns:
        截断后的文本
    """
    if len(text) <= max_length:
        return text
    
    return text[:max_length - len(suffix)] + suffix


def format_file_size(size_bytes: int) -> str:
    """
    格式化文件大小
    
    Args:
        size_bytes: 文件大小（字节）
        
    Returns:
        格式化的文件大小字符串
    """
    if size_bytes == 0:
        return "0B"
    
    size_names = ["B", "KB", "MB", "GB", "TB"]
    i = 0
    size = float(size_bytes)
    
    while size >= 1024.0 and i < len(size_names) - 1:
        size /= 1024.0
        i += 1
    
    return f"{size:.1f}{size_names[i]}"


def validate_file_path(file_path: str, allowed_extensions: Optional[List[str]] = None) -> bool:
    """
    验证文件路径
    
    Args:
        file_path: 文件路径
        allowed_extensions: 允许的扩展名列表
        
    Returns:
        是否有效
    """
    path = Path(file_path)
    
    # 检查文件是否存在
    if not path.exists():
        logger.error(f"文件不存在: {file_path}")
        return False
    
    # 检查是否为文件
    if not path.is_file():
        logger.error(f"路径不是文件: {file_path}")
        return False
    
    # 检查扩展名
    if allowed_extensions:
        if path.suffix.lower() not in [ext.lower() for ext in allowed_extensions]:
            logger.error(f"不支持的文件类型: {path.suffix}")
            return False
    
    return True