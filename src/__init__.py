"""
水利多模态数据处理工具包
OpenHydrology Data Processing Toolkit

用于水利大模型的数据预处理、清洗、增强和评估
"""

__version__ = "1.0.0"
__author__ = "OpenHydrology Team"

from .config import ConfigManager
from .utils import setup_logging, create_directories

__all__ = [
    "ConfigManager",
    "setup_logging", 
    "create_directories"
]