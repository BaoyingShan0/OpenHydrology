"""
水利数据解析器Skill
负责解析多种格式的原始数据文件，包括PDF、文本、JSON、CSV等
"""

import os
import json
import csv
import pandas as pd
from pathlib import Path
from typing import List, Dict, Any, Optional, Union
from datetime import datetime
import time
from loguru import logger

try:
    import PyPDF2
    import pdfplumber
    PDF_SUPPORT = True
except ImportError:
    PDF_SUPPORT = False
    logger.warning("PDF处理库未安装，PDF解析功能将不可用")

try:
    from langdetect import detect
    LANG_DETECT_SUPPORT = True
except ImportError:
    LANG_DETECT_SUPPORT = False
    logger.warning("语言检测库未安装，将使用默认语言")

from .base import MonitoredSkill
from ..models import DataChunk, DataType, Language, SourceMetadata, ProcessingStatus
from ..utils import (
    calculate_file_hash, detect_file_encoding, validate_file_path,
    get_timestamp, format_file_size
)


class HydroDataParser(MonitoredSkill):
    """
    水利数据解析器
    
    功能：
    1. 支持多种文件格式解析（PDF、TXT、JSON、CSV、MD）
    2. 自动检测文件编码和语言
    3. 提取文件元数据
    4. 支持大文件分块处理
    5. 多语言支持（中文、英文）
    """
    
    def __init__(self, config_key: str = "parser"):
        super().__init__("HydroDataParser", config_key)
        
        # 从配置获取参数
        self.supported_formats = self.get_config_value("supported_formats", ["pdf", "txt", "json", "csv", "md"])
        self.chunk_size = self.get_config_value("text_settings.chunk_size", 1000)
        self.overlap = self.get_config_value("text_settings.overlap", 100)
        self.encoding_detection = self.get_config_value("text_settings.encoding_detection", True)
        self.min_text_length = self.get_config_value("min_text_length", 10)
        
        # PDF处理配置
        self.pdf_extract_tables = self.get_config_value("pdf_settings.extract_tables", True)
        self.pdf_extract_images = self.get_config_value("pdf_settings.extract_images", False)
        self.pdf_min_confidence = self.get_config_value("pdf_settings.min_confidence", 0.8)
        
        # 支持的文件扩展名映射
        self.extension_mapping = {
            "pdf": DataType.PDF,
            "txt": DataType.TEXT,
            "text": DataType.TEXT,
            "json": DataType.JSON,
            "csv": DataType.CSV,
            "md": DataType.MARKDOWN,
            "markdown": DataType.MARKDOWN
        }
        
        self.log_processing_info("数据解析器初始化完成")
    
    def get_required_config_keys(self) -> List[str]:
        """获取必需的配置键"""
        return ["supported_formats"]
    
    def parse_file(self, file_path: str) -> List[DataChunk]:
        """
        解析单个文件
        
        Args:
            file_path: 文件路径
            
        Returns:
            解析后的数据块列表
        """
        self.log_processing_info(f"开始解析文件: {file_path}")
        
        # 验证文件路径
        if not validate_file_path(file_path, list(self.extension_mapping.keys())):
            raise ValueError(f"不支持的文件格式: {file_path}")
        
        # 获取文件信息
        file_path_obj = Path(file_path)
        file_extension = file_path_obj.suffix.lower().lstrip('.')
        
        # 检查是否支持该格式
        if file_extension not in self.supported_formats:
            raise ValueError(f"不支持的文件格式: {file_extension}")
        
        try:
            # 创建源数据元信息
            source_metadata = self._create_source_metadata(file_path)
            
            # 根据文件类型选择解析方法
            if file_extension == "pdf":
                chunks = self._parse_pdf(file_path, source_metadata)
            elif file_extension in ["txt", "text"]:
                chunks = self._parse_text(file_path, source_metadata)
            elif file_extension == "json":
                chunks = self._parse_json(file_path, source_metadata)
            elif file_extension == "csv":
                chunks = self._parse_csv(file_path, source_metadata)
            elif file_extension in ["md", "markdown"]:
                chunks = self._parse_markdown(file_path, source_metadata)
            else:
                raise ValueError(f"暂不支持的文件格式: {file_extension}")
            
            self.log_processing_info(f"文件解析完成，生成 {len(chunks)} 个数据块")
            return chunks
            
        except Exception as e:
            self.log_processing_info(f"文件解析失败: {e}", "error")
            raise
    
    def _create_source_metadata(self, file_path: str) -> SourceMetadata:
        """创建源数据元信息"""
        file_path_obj = Path(file_path)
        stat = file_path_obj.stat()
        
        return SourceMetadata(
            file_path=str(file_path_obj.absolute()),
            file_name=file_path_obj.name,
            file_size=stat.st_size,
            file_type=file_path_obj.suffix.lower(),
            encoding=detect_file_encoding(file_path) if self.encoding_detection else None,
            hash_value=calculate_file_hash(file_path),
            created_at=datetime.fromtimestamp(stat.st_ctime),
            modified_at=datetime.fromtimestamp(stat.st_mtime)
        )
    
    def _parse_pdf(self, file_path: str, source_metadata: SourceMetadata) -> List[DataChunk]:
        """解析PDF文件"""
        if not PDF_SUPPORT:
            raise ImportError("PDF处理库未安装，请安装 PyPDF2 和 pdfplumber")
        
        chunks = []
        
        try:
            # 使用pdfplumber解析（支持表格提取）
            with pdfplumber.open(file_path) as pdf:
                for page_num, page in enumerate(pdf.pages, 1):
                    page_text = page.extract_text()
                    
                    if page_text and len(page_text.strip()) >= self.min_text_length:
                        # 检测语言
                        language = self._detect_language(page_text)
                        
                        # 创建数据块
                        chunk = DataChunk(
                            content=page_text.strip(),
                            data_type=DataType.PDF,
                            language=language,
                            source_metadata=source_metadata,
                            extra_data={
                                "page_number": page_num,
                                "total_pages": len(pdf.pages),
                                "file_type": "pdf"
                            }
                        )
                        chunks.append(chunk)
                    
                    # 提取表格
                    if self.pdf_extract_tables:
                        tables = page.extract_tables()
                        for table_num, table in enumerate(tables):
                            if table:
                                table_text = self._table_to_text(table)
                                if table_text:
                                    chunk = DataChunk(
                                        content=table_text,
                                        data_type=DataType.TABLE,
                                        language=language,
                                        source_metadata=source_metadata,
                                        extra_data={
                                            "page_number": page_num,
                                            "table_number": table_num,
                                            "file_type": "pdf"
                                        }
                                    )
                                    chunks.append(chunk)
            
        except Exception as e:
            # 如果pdfplumber失败，尝试使用PyPDF2
            try:
                with open(file_path, 'rb') as file:
                    pdf_reader = PyPDF2.PdfReader(file)
                    
                    for page_num, page in enumerate(pdf_reader.pages, 1):
                        page_text = page.extract_text()
                        
                        if page_text and len(page_text.strip()) >= self.min_text_length:
                            language = self._detect_language(page_text)
                            
                            chunk = DataChunk(
                                content=page_text.strip(),
                                data_type=DataType.PDF,
                                language=language,
                                source_metadata=source_metadata,
                                extra_data={
                                    "page_number": page_num,
                                    "total_pages": len(pdf_reader.pages),
                                    "file_type": "pdf"
                                }
                            )
                            chunks.append(chunk)
                            
            except Exception as e2:
                raise Exception(f"PDF解析失败 (pdfplumber: {e}, PyPDF2: {e2})")
        
        return self._chunk_large_texts(chunks)
    
    def _parse_text(self, file_path: str, source_metadata: SourceMetadata) -> List[DataChunk]:
        """解析纯文本文件"""
        encoding = source_metadata.encoding or 'utf-8'
        
        try:
            with open(file_path, 'r', encoding=encoding) as file:
                text = file.read()
            
            if not text.strip() or len(text.strip()) < self.min_text_length:
                return []
            
            # 检测语言
            language = self._detect_language(text)
            
            # 创建数据块
            chunk = DataChunk(
                content=text.strip(),
                data_type=DataType.TEXT,
                language=language,
                source_metadata=source_metadata,
                extra_data={"file_type": "text"}
            )
            
            return self._chunk_large_texts([chunk])
            
        except UnicodeDecodeError:
            # 如果默认编码失败，尝试其他编码
            for enc in ['utf-8', 'gbk', 'gb2312', 'latin-1']:
                try:
                    with open(file_path, 'r', encoding=enc) as file:
                        text = file.read()
                    
                    if text.strip():
                        language = self._detect_language(text)
                        chunk = DataChunk(
                            content=text.strip(),
                            data_type=DataType.TEXT,
                            language=language,
                            source_metadata=source_metadata,
                            extra_data={"file_type": "text", "encoding": enc}
                        )
                        return self._chunk_large_texts([chunk])
                except:
                    continue
            
            raise Exception("无法解码文件，尝试了多种编码格式")
    
    def _parse_json(self, file_path: str, source_metadata: SourceMetadata) -> List[DataChunk]:
        """解析JSON文件"""
        encoding = source_metadata.encoding or 'utf-8'
        
        with open(file_path, 'r', encoding=encoding) as file:
            data = json.load(file)
        
        chunks = []
        
        def process_json_data(data, path=""):
            if isinstance(data, dict):
                for key, value in data.items():
                    new_path = f"{path}.{key}" if path else key
                    process_json_data(value, new_path)
            elif isinstance(data, list):
                for i, item in enumerate(data):
                    new_path = f"{path}[{i}]"
                    process_json_data(item, new_path)
            elif isinstance(data, str) and len(data.strip()) >= self.min_text_length:
                language = self._detect_language(data)
                chunk = DataChunk(
                    content=data.strip(),
                    data_type=DataType.JSON,
                    language=language,
                    source_metadata=source_metadata,
                    extra_data={
                        "file_type": "json",
                        "json_path": path
                    }
                )
                chunks.append(chunk)
        
        process_json_data(data)
        return chunks
    
    def _parse_csv(self, file_path: str, source_metadata: SourceMetadata) -> List[DataChunk]:
        """解析CSV文件"""
        encoding = source_metadata.encoding or 'utf-8'
        
        try:
            df = pd.read_csv(file_path, encoding=encoding)
        except UnicodeDecodeError:
            # 尝试其他编码
            for enc in ['utf-8', 'gbk', 'gb2312', 'latin-1']:
                try:
                    df = pd.read_csv(file_path, encoding=enc)
                    encoding = enc
                    break
                except:
                    continue
            else:
                raise Exception("无法解码CSV文件")
        
        chunks = []
        
        # 处理每一行数据
        for index, row in df.iterrows():
            # 将行转换为文本
            row_text = " | ".join([f"{col}: {val}" for col, val in row.items() if pd.notna(val)])
            
            if row_text.strip():
                language = self._detect_language(row_text)
                chunk = DataChunk(
                    content=row_text,
                    data_type=DataType.CSV,
                    language=language,
                    source_metadata=source_metadata,
                    extra_data={
                        "file_type": "csv",
                        "row_number": index,
                        "encoding": encoding
                    }
                )
                chunks.append(chunk)
        
        return chunks
    
    def _parse_markdown(self, file_path: str, source_metadata: SourceMetadata) -> List[DataChunk]:
        """解析Markdown文件"""
        encoding = source_metadata.encoding or 'utf-8'
        
        with open(file_path, 'r', encoding=encoding) as file:
            text = file.read()
        
        if not text.strip() or len(text.strip()) < self.min_text_length:
            return []
        
        language = self._detect_language(text)
        chunk = DataChunk(
            content=text.strip(),
            data_type=DataType.MARKDOWN,
            language=language,
            source_metadata=source_metadata,
            extra_data={"file_type": "markdown"}
        )
        
        return self._chunk_large_texts([chunk])
    
    def _detect_language(self, text: str) -> Language:
        """检测文本语言"""
        if not LANG_DETECT_SUPPORT:
            return Language.AUTO
        
        try:
            # 只检测前1000个字符以提高速度
            sample_text = text[:1000] if len(text) > 1000 else text
            detected_lang = detect(sample_text)
            
            if detected_lang == 'zh':
                return Language.CHINESE
            elif detected_lang == 'en':
                return Language.ENGLISH
            else:
                return Language.AUTO
        except:
            return Language.AUTO
    
    def _table_to_text(self, table: List[List[str]]) -> str:
        """将表格转换为文本"""
        if not table:
            return ""
        
        text_lines = []
        for row in table:
            # 过滤空值并转换为字符串
            row_text = " | ".join([str(cell) if cell is not None else "" for cell in row])
            if row_text.strip():
                text_lines.append(row_text)
        
        return "\n".join(text_lines)
    
    def _chunk_large_texts(self, chunks: List[DataChunk]) -> List[DataChunk]:
        """将大文本分块处理"""
        result_chunks = []
        
        for chunk in chunks:
            content = chunk.content
            
            if len(content) <= self.chunk_size:
                result_chunks.append(chunk)
            else:
                # 分块处理
                start = 0
                chunk_num = 0
                
                while start < len(content):
                    end = start + self.chunk_size
                    
                    # 如果不是最后一块，尝试在句号、换行符等位置分割
                    if end < len(content):
                        # 寻找最佳分割点
                        split_chars = ['\n\n', '\n', '。', '. ', '！', '！', '？', '? ']
                        split_pos = end
                        
                        for char in split_chars:
                            pos = content.rfind(char, start, end)
                            if pos > start:
                                split_pos = pos + len(char)
                                break
                        
                        end = split_pos
                    
                    # 创建子块
                    sub_content = content[start:end].strip()
                    if len(sub_content) >= self.min_text_length:
                        sub_chunk = DataChunk(
                            content=sub_content,
                            data_type=chunk.data_type,
                            language=chunk.language,
                            source_metadata=chunk.source_metadata,
                            extra_data={
                                **chunk.extra_data,
                                "chunk_number": chunk_num,
                                "total_chunks": (len(content) + self.chunk_size - 1) // self.chunk_size
                            }
                        )
                        result_chunks.append(sub_chunk)
                    
                    start = end - self.overlap if end < len(content) else end
                    chunk_num += 1
        
        return result_chunks
    
    def process_single(self, data_chunk: DataChunk) -> DataChunk:
        """
        处理单个数据块（基类方法实现）
        注意：Parser主要处理文件，此方法用于兼容接口
        """
        # 如果数据块已经有内容，直接返回
        if data_chunk.content.strip():
            return data_chunk
        
        # 如果数据块包含文件路径信息，尝试解析
        if data_chunk.extra_data.get("file_path"):
            chunks = self.parse_file(data_chunk.extra_data["file_path"])
            return chunks[0] if chunks else data_chunk
        
        return data_chunk
    
    def get_supported_formats(self) -> List[str]:
        """获取支持的文件格式列表"""
        return self.supported_formats.copy()
    
    def parse_directory(self, directory_path: str, recursive: bool = True) -> List[DataChunk]:
        """
        解析目录下的所有支持文件
        
        Args:
            directory_path: 目录路径
            recursive: 是否递归处理子目录
            
        Returns:
            所有解析后的数据块列表
        """
        directory = Path(directory_path)
        
        if not directory.exists() or not directory.is_dir():
            raise ValueError(f"目录不存在或不是有效目录: {directory_path}")
        
        all_chunks = []
        
        # 获取所有支持的文件
        if recursive:
            pattern = "**/*"
        else:
            pattern = "*"
        
        for ext in self.supported_formats:
            file_pattern = f"{pattern}.{ext}"
            files = list(directory.glob(file_pattern))
            
            for file_path in files:
                if file_path.is_file():
                    try:
                        chunks = self.parse_file(str(file_path))
                        all_chunks.extend(chunks)
                        self.log_processing_info(f"解析文件成功: {file_path} ({len(chunks)} 个数据块)")
                    except Exception as e:
                        self.log_processing_info(f"解析文件失败: {file_path} - {e}", "error")
        
        self.log_processing_info(f"目录解析完成，共生成 {len(all_chunks)} 个数据块")
        return all_chunks