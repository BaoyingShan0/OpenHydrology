"""
水利数据清洗器Skill
负责清洗和标准化解析后的数据，去除噪声，提高数据质量
"""

import re
import hashlib
from typing import List, Dict, Any, Set, Optional, Tuple
from collections import Counter
from loguru import logger

try:
    import spacy
    SPACY_SUPPORT = True
except ImportError:
    SPACY_SUPPORT = False
    logger.warning("spaCy库未安装，高级文本清洗功能将不可用")

try:
    import jieba
    JIEBA_SUPPORT = True
except ImportError:
    JIEBA_SUPPORT = False
    logger.warning("jieba库未安装，中文分词功能将不可用")

from .base import MonitoredSkill
from ..models import DataChunk, ProcessingStatus, Language


class HydroDataCleaner(MonitoredSkill):
    """
    水利数据清洗器
    
    功能：
    1. 去除重复数据和近似重复数据
    2. 标准化文本格式（空格、标点符号等）
    3. 过滤低质量内容
    4. 修复常见文本错误
    5. 多语言文本清洗
    6. 识别和标记特殊内容
    """
    
    def __init__(self, config_key: str = "cleaner"):
        super().__init__("HydroDataCleaner", config_key)
        
        # 从配置获取参数
        self.remove_duplicates = self.get_config_value("remove_duplicates", True)
        self.normalize_whitespace = self.get_config_value("normalize_whitespace", True)
        self.remove_special_chars = self.get_config_value("remove_special_chars", False)
        self.min_text_length = self.get_config_value("min_text_length", 10)
        self.language_detection = self.get_config_value("language_detection", True)
        self.similarity_threshold = self.get_config_value("similarity_threshold", 0.85)
        
        # 初始化清洗规则
        self._init_cleaning_rules()
        
        # 初始化NLP组件
        self._init_nlp_components()
        
        # 重复数据检测
        self.seen_hashes: Set[str] = set()
        self.seen_contents: Dict[str, str] = {}
        
        self.log_processing_info("数据清洗器初始化完成")
    
    def get_required_config_keys(self) -> List[str]:
        """获取必需的配置键"""
        return ["min_text_length"]
    
    def _init_cleaning_rules(self):
        """初始化清洗规则"""
        # 中文标点符号映射
        self.chinese_punctuation_map = {
            '，': ',',
            '。': '.',
            '！': '!',
            '？': '?',
            '；': ';',
            '：': ':',
            '（': '(',
            '）': ')',
            '【': '[',
            '】': ']',
            '《': '<',
            '》': '>',
            '"': '"',
            '"': '"',
            ''': "'",
            ''': "'",
        }
        
        # 需要清理的特殊字符模式
        self.cleanup_patterns = [
            # 多个连续空格
            (r'\s+', ' '),
            # 多个连续换行符
            (r'\n+', '\n'),
            # HTML标签
            (r'<[^>]+>', ''),
            # URL链接
            (r'http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\\(\\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+', ''),
            # 邮箱地址
            (r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b', ''),
            # 连续的标点符号
            (r'[.!?]{3,}', '...'),
            # 数字和单位之间的空格
            (r'(\d)([a-zA-Z])', r'\1 \2'),
        ]
        
        # 水利专业术语保护列表（这些不会被清洗）
        self.protected_terms = {
            '水文', '气象', '降雨', '径流', '蒸发', '渗透', '地下水', '地表水',
            '水库', '大坝', '堤防', '水闸', '泵站', '灌溉', '排涝', '防洪',
            '水质', '水量', '水位', '流量', '流速', '含沙量', '溶解氧',
            '流域', '水系', '河道', '河床', '河岸', '河口', '三角洲',
            '洪水', '枯水', '丰水', '平水', '汛期', '枯水期', '丰水期',
            '水库调度', '水资源', '水环境', '水生态', '水安全', '水管理'
        }
    
    def _init_nlp_components(self):
        """初始化NLP组件"""
        self.nlp_models = {}
        
        if SPACY_SUPPORT:
            try:
                # 加载英文模型
                self.nlp_models['en'] = spacy.load('en_core_web_sm')
                logger.info("spaCy英文模型加载成功")
            except OSError:
                logger.warning("spaCy英文模型未找到，请安装: python -m spacy download en_core_web_sm")
        
        if JIEBA_SUPPORT:
            # jieba已经内置中文词典，无需额外加载
            logger.info("jieba中文分词工具可用")
    
    def process_single(self, data_chunk: DataChunk) -> DataChunk:
        """处理单个数据块"""
        if not self.validate_input(data_chunk):
            return data_chunk
        
        try:
            # 获取原始内容
            original_content = data_chunk.content
            
            # 1. 基础文本清洗
            cleaned_content = self._basic_text_cleaning(original_content)
            
            # 2. 语言特定的清洗
            cleaned_content = self._language_specific_cleaning(cleaned_content, data_chunk.language)
            
            # 3. 高级清洗（基于NLP）
            cleaned_content = self._advanced_text_cleaning(cleaned_content, data_chunk.language)
            
            # 4. 质量检查
            if not self._quality_check(cleaned_content):
                self.log_processing_info(f"数据块 {data_chunk.id} 未通过质量检查", "warning")
                return data_chunk
            
            # 5. 重复性检查
            if self.remove_duplicates and self._is_duplicate(cleaned_content, data_chunk.id):
                self.log_processing_info(f"发现重复数据块: {data_chunk.id}", "info")
                # 标记为跳过状态
                data_chunk.add_process_metadata(self._create_process_metadata(
                    ProcessingStatus.SKIPPED, "重复数据"
                ))
                return data_chunk
            
            # 更新数据块内容
            data_chunk.content = cleaned_content
            
            # 更新元数据
            data_chunk.extra_data.update({
                "cleaned": True,
                "original_length": len(original_content),
                "cleaned_length": len(cleaned_content),
                "cleaning_ratio": len(cleaned_content) / len(original_content) if original_content else 0
            })
            
            self.log_processing_info(f"数据清洗完成: {data_chunk.id}")
            return data_chunk
            
        except Exception as e:
            self.log_processing_info(f"数据清洗失败 {data_chunk.id}: {e}", "error")
            data_chunk.add_process_metadata(self._create_process_metadata(
                ProcessingStatus.FAILED, str(e)
            ))
            return data_chunk
    
    def _basic_text_cleaning(self, text: str) -> str:
        """基础文本清洗"""
        if not text:
            return text
        
        cleaned = text
        
        # 标准化换行符
        cleaned = cleaned.replace('\r\n', '\n').replace('\r', '\n')
        
        # 应用清洗规则
        for pattern, replacement in self.cleanup_patterns:
            cleaned = re.sub(pattern, replacement, cleaned, flags=re.IGNORECASE | re.MULTILINE)
        
        # 标点符号标准化
        if self._contains_chinese(cleaned):
            for chinese_punct, english_punct in self.chinese_punctuation_map.items():
                cleaned = cleaned.replace(chinese_punct, english_punct)
        
        # 空白字符标准化
        if self.normalize_whitespace:
            cleaned = re.sub(r'[ \t]+', ' ', cleaned)  # 多个空格/制表符转单个空格
            cleaned = re.sub(r'\n[ \t]+', '\n', cleaned)  # 行首空格
            cleaned = re.sub(r'[ \t]+\n', '\n', cleaned)  # 行尾空格
            cleaned = cleaned.strip()  # 首尾空格
        
        return cleaned
    
    def _language_specific_cleaning(self, text: str, language: Language) -> str:
        """语言特定的清洗"""
        if language == Language.CHINESE or (language == Language.AUTO and self._contains_chinese(text)):
            return self._chinese_text_cleaning(text)
        elif language == Language.ENGLISH or (language == Language.AUTO and not self._contains_chinese(text)):
            return self._english_text_cleaning(text)
        else:
            # 自动检测或混合语言
            return self._mixed_language_cleaning(text)
    
    def _chinese_text_cleaning(self, text: str) -> str:
        """中文文本清洗"""
        if not JIEBA_SUPPORT:
            return text
        
        # 使用jieba进行分词和清洗
        try:
            words = jieba.lcut(text)
            
            # 过滤无意义的词
            filtered_words = []
            for word in words:
                # 保留水利专业术语
                if word in self.protected_terms:
                    filtered_words.append(word)
                # 过滤单个标点符号（除非是特殊标点）
                elif len(word) > 1 or word in '。！？；：':
                    filtered_words.append(word)
            
            return ''.join(filtered_words)
        except:
            return text
    
    def _english_text_cleaning(self, text: str) -> str:
        """英文文本清洗"""
        if not SPACY_SUPPORT or 'en' not in self.nlp_models:
            return text
        
        try:
            doc = self.nlp_models['en'](text)
            
            # 过滤无意义的词
            filtered_tokens = []
            for token in doc:
                # 保留有意义的词汇
                if (not token.is_stop and 
                    not token.is_punct and 
                    not token.is_space and 
                    len(token.text.strip()) > 0):
                    filtered_tokens.append(token.text)
            
            return ' '.join(filtered_tokens)
        except:
            return text
    
    def _mixed_language_cleaning(self, text: str) -> str:
        """混合语言文本清洗"""
        # 简单处理：先按中文清洗，再按英文清洗
        cleaned = self._chinese_text_cleaning(text)
        cleaned = self._english_text_cleaning(cleaned)
        return cleaned
    
    def _advanced_text_cleaning(self, text: str, language: Language) -> str:
        """高级文本清洗（基于NLP）"""
        # 移除特殊字符（如果配置允许）
        if self.remove_special_chars:
            # 保留中文字符、英文字符、数字和基本标点
            pattern = r'[^\u4e00-\u9fff\u3400-\u4dbf\w\s.,;:!?()[\]{}"\'-]'
            text = re.sub(pattern, '', text)
        
        # 修复常见错误
        text = self._fix_common_errors(text)
        
        return text
    
    def _fix_common_errors(self, text: str) -> str:
        """修复常见文本错误"""
        # 常见错误修正规则
        error_fixes = [
            # 空格和标点符号
            (r' +([.,;:!?])', r'\1'),  # 标点前的空格
            (r'([.,;:!?]) +', r'\1 '),  # 标点后确保一个空格
            # 数字格式
            (r'(\d),(\d{3})', r'\1\2'),  # 数字中的逗号
            # 括号匹配
            (r'\(\s*', '('),  # 左括号后空格
            (r'\s*\)', ')'),  # 右括号前空格
        ]
        
        for pattern, replacement in error_fixes:
            text = re.sub(pattern, replacement, text)
        
        return text
    
    def _quality_check(self, text: str) -> bool:
        """质量检查"""
        # 长度检查
        if len(text.strip()) < self.min_text_length:
            return False
        
        # 内容检查
        if not text.strip():
            return False
        
        # 字符多样性检查（避免重复字符）
        unique_chars = len(set(text))
        if unique_chars < 5 and len(text) > 20:
            return False
        
        # 水利相关性检查（简单启发式）
        hydro_keywords = ['水', '文', '雨', '河', '湖', '库', '坝', '防', '洪', '涝']
        if not any(keyword in text for keyword in hydro_keywords):
            # 如果没有水利关键词，但文本质量较高，仍然保留
            if len(text) > 100 and unique_chars > 30:
                return True
            else:
                return False
        
        return True
    
    def _is_duplicate(self, text: str, chunk_id: str) -> bool:
        """检查是否为重复数据"""
        # 计算文本哈希
        text_hash = hashlib.md5(text.encode()).hexdigest()
        
        # 精确重复检查
        if text_hash in self.seen_hashes:
            return True
        
        # 近似重复检查（基于相似度）
        for existing_id, existing_content in self.seen_contents.items():
            if self._calculate_similarity(text, existing_content) > self.similarity_threshold:
                return True
        
        # 记录新文本
        self.seen_hashes.add(text_hash)
        self.seen_contents[chunk_id] = text
        
        return False
    
    def _calculate_similarity(self, text1: str, text2: str) -> float:
        """计算文本相似度"""
        if not text1 or not text2:
            return 0.0
        
        # 简单的字符级相似度计算
        set1 = set(text1.lower())
        set2 = set(text2.lower())
        
        intersection = len(set1 & set2)
        union = len(set1 | set2)
        
        if union == 0:
            return 0.0
        
        return intersection / union
    
    def _contains_chinese(self, text: str) -> bool:
        """检查文本是否包含中文字符"""
        return bool(re.search(r'[\u4e00-\u9fff]', text))
    
    def _create_process_metadata(self, status: ProcessingStatus, message: str = None):
        """创建处理元数据"""
        from datetime import datetime
        return self._create_process_metadata_obj(status, message)
    
    def _create_process_metadata_obj(self, status: ProcessingStatus, message: str = None):
        """创建处理元数据对象"""
        from datetime import datetime
        from ..models import ProcessMetadata
        
        return ProcessMetadata(
            processor_name=self.name,
            processing_time=0.0,
            start_time=datetime.now(),
            end_time=datetime.now(),
            status=status,
            error_message=message,
            parameters=self.config
        )
    
    def get_cleaning_statistics(self) -> Dict[str, Any]:
        """获取清洗统计信息"""
        stats = self.get_monitoring_info()
        
        # 添加清洗特定的统计信息
        stats.update({
            "duplicate_hashes_detected": len(self.seen_hashes),
            "contents_stored": len(self.seen_contents),
            "protected_terms_count": len(self.protected_terms)
        })
        
        return stats
    
    def reset_duplicate_tracker(self):
        """重置重复数据追踪器"""
        self.seen_hashes.clear()
        self.seen_contents.clear()
        self.log_processing_info("重复数据追踪器已重置")
    
    def add_protected_terms(self, terms: List[str]):
        """添加保护术语"""
        for term in terms:
            self.protected_terms.add(term)
        self.log_processing_info(f"添加了 {len(terms)} 个保护术语")