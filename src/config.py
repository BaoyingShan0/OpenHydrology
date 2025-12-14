"""
配置管理模块
负责加载和管理项目配置文件
"""

import yaml
import os
from pathlib import Path
from typing import Dict, Any, Optional
from loguru import logger


class ConfigManager:
    """配置管理器类"""
    
    def __init__(self, config_path: Optional[str] = None):
        """
        初始化配置管理器
        
        Args:
            config_path: 配置文件路径，默认为 config/hydro_config.yaml
        """
        if config_path is None:
            # 获取项目根目录
            project_root = Path(__file__).parent.parent
            config_path = project_root / "config" / "hydro_config.yaml"
        
        self.config_path = Path(config_path)
        self.config = self._load_config()
        self._validate_config()
    
    def _load_config(self) -> Dict[str, Any]:
        """加载配置文件"""
        try:
            with open(self.config_path, 'r', encoding='utf-8') as f:
                config = yaml.safe_load(f)
            logger.info(f"配置文件加载成功: {self.config_path}")
            return config
        except FileNotFoundError:
            logger.error(f"配置文件不存在: {self.config_path}")
            raise
        except yaml.YAMLError as e:
            logger.error(f"配置文件格式错误: {e}")
            raise
    
    def _validate_config(self):
        """验证配置文件的基本结构"""
        required_sections = ['global', 'parser', 'cleaner', 'enhancer', 'evaluator', 'pipeline']
        
        for section in required_sections:
            if section not in self.config:
                raise ValueError(f"配置文件缺少必需的节: {section}")
        
        logger.info("配置文件验证通过")
    
    def get(self, key: str, default: Any = None) -> Any:
        """
        获取配置值
        
        Args:
            key: 配置键，支持点号分隔的嵌套键（如 'parser.pdf_settings'）
            default: 默认值
            
        Returns:
            配置值
        """
        keys = key.split('.')
        value = self.config
        
        for k in keys:
            if isinstance(value, dict) and k in value:
                value = value[k]
            else:
                return default
        
        return value
    
    def set(self, key: str, value: Any):
        """
        设置配置值
        
        Args:
            key: 配置键，支持点号分隔的嵌套键
            value: 配置值
        """
        keys = key.split('.')
        config = self.config
        
        for k in keys[:-1]:
            if k not in config:
                config[k] = {}
            config = config[k]
        
        config[keys[-1]] = value
    
    def save(self, path: Optional[str] = None):
        """
        保存配置到文件
        
        Args:
            path: 保存路径，默认为原配置文件路径
        """
        save_path = path if path else self.config_path
        
        try:
            with open(save_path, 'w', encoding='utf-8') as f:
                yaml.dump(self.config, f, default_flow_style=False, 
                         allow_unicode=True, indent=2)
            logger.info(f"配置文件保存成功: {save_path}")
        except Exception as e:
            logger.error(f"配置文件保存失败: {e}")
            raise
    
    def create_directories(self):
        """根据配置创建必要的目录"""
        dirs_to_create = [
            self.get('global.output_dir', './output'),
            self.get('global.temp_dir', './temp'),
            os.path.dirname(self.get('database.path', './data/hydro_data.db'))
        ]
        
        for dir_path in dirs_to_create:
            Path(dir_path).mkdir(parents=True, exist_ok=True)
            logger.info(f"目录创建成功: {dir_path}")


# 全局配置实例
config_manager = ConfigManager()