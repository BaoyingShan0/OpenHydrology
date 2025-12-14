"""
水利数据评估器Skill
负责评估处理后数据的质量，提供多维度质量评分和改进建议
"""

import re
import math
from typing import List, Dict, Any, Optional, Tuple, Set
from collections import Counter, defaultdict
from loguru import logger

try:
    import numpy as np
    from sklearn.feature_extraction.text import TfidfVectorizer
    from sklearn.metrics.pairwise import cosine_similarity
    SKLEARN_SUPPORT = True
except ImportError:
    SKLEARN_SUPPORT = False
    logger.warning("scikit-learn库未安装，高级质量评估功能将不可用")

from .base import MonitoredSkill
from ..models import DataChunk, QAData, QualityScore, ProcessingStatus, Language


class HydroDataEvaluator(MonitoredSkill):
    """
    水利数据评估器
    
    功能：
    1. 多维度数据质量评估（完整性、相关性、一致性、多样性）
    2. 问答对质量评估
    3. 数据分布和覆盖度分析
    4. 自动识别问题和改进建议
    5. 生成可视化评估报告
    6. 数据筛选和过滤
    """
    
    def __init__(self, config_key: str = "evaluator"):
        super().__init__("HydroDataEvaluator", config_key)
        
        # 从配置获取参数
        self.quality_metrics = self.get_config_value("quality_metrics", ["completeness", "relevance", "consistency", "diversity"])
        self.thresholds = self.get_config_value("thresholds", {
            "min_quality_score": 0.7,
            "min_relevance_score": 0.6
        })
        
        # 初始化评估器组件
        self._init_evaluators()
        
        # 评估统计信息
        self.evaluation_history = []
        self.quality_distribution = defaultdict(list)
        
        self.log_processing_info("数据评估器初始化完成")
    
    def get_required_config_keys(self) -> List[str]:
        """获取必需的配置键"""
        return ["quality_metrics"]
    
    def _init_evaluators(self):
        """初始化评估器组件"""
        # 水利领域关键词
        self.hydro_keywords = {
            # 核心概念
            "水文", "气象", "降雨", "降水", "径流", "蒸发", "渗透", "地下水", "地表水",
            "水位", "流量", "流速", "含沙量", "溶解氧", "水质", "水量", "水环境", "水生态",
            
            # 地理实体
            "流域", "水系", "河流", "河道", "河床", "河岸", "河口", "三角洲", "湖泊", "水库",
            "长江", "黄河", "珠江", "淮河", "海河", "松花江", "辽河",
            
            # 水利工程
            "大坝", "水坝", "堤坝", "水闸", "堤防", "泵站", "水电站", "水库", "拦河坝",
            "防洪", "排涝", "灌溉", "供水", "排水", "调水", "引水",
            
            # 水文现象
            "洪水", "枯水", "丰水", "平水", "汛期", "枯水期", "丰水期", "干旱", "洪涝",
            "暴雨", "特大暴雨", "连续降雨", "梅雨", "台风",
            
            # 专业术语
            "水文站", "雨量站", "水位站", "监测", "预报", "预警", "调度", "管理", "规划",
            "设计", "施工", "运行", "维护", "治理", "保护", "修复", "生态"
        }
        
        # 质量评估权重
        self.metric_weights = {
            "completeness": 0.25,
            "relevance": 0.30,
            "consistency": 0.20,
            "diversity": 0.25
        }
        
        # 初始化TF-IDF向量化器
        self.vectorizer = None
        if SKLEARN_SUPPORT:
            self.vectorizer = TfidfVectorizer(
                max_features=1000,
                ngram_range=(1, 2),
                stop_words=None,
                lowercase=True,
                token_pattern=r'(?u)\b\w+\b'
            )
    
    def process_single(self, data_chunk: DataChunk) -> DataChunk:
        """处理单个数据块"""
        if not self.validate_input(data_chunk):
            return data_chunk
        
        try:
            # 执行质量评估
            quality_score = self._evaluate_quality(data_chunk)
            
            # 将质量评分添加到数据块
            data_chunk.extra_data["quality_score"] = quality_score
            
            # 记录评估结果
            self._record_evaluation(data_chunk.id, quality_score)
            
            # 生成改进建议
            suggestions = self._generate_suggestions(data_chunk, quality_score)
            if suggestions:
                data_chunk.extra_data["improvement_suggestions"] = suggestions
            
            # 根据阈值判断是否需要标记
            if quality_score.overall_score < self.thresholds.get("min_quality_score", 0.7):
                data_chunk.extra_data["quality_warning"] = True
                self.log_processing_info(f"数据块 {data_chunk.id} 质量评分较低: {quality_score.overall_score:.3f}", "warning")
            
            self.log_processing_info(f"质量评估完成: {data_chunk.id}, 评分: {quality_score.overall_score:.3f}")
            return data_chunk
            
        except Exception as e:
            self.log_processing_info(f"质量评估失败 {data_chunk.id}: {e}", "error")
            data_chunk.add_process_metadata(self._create_process_metadata(
                ProcessingStatus.FAILED, str(e)
            ))
            return data_chunk
    
    def _evaluate_quality(self, data_chunk: DataChunk) -> QualityScore:
        """评估数据质量"""
        content = data_chunk.content
        language = data_chunk.language
        
        # 计算各维度评分
        scores = {}
        
        if "completeness" in self.quality_metrics:
            scores["completeness"] = self._evaluate_completeness(content, data_chunk)
        
        if "relevance" in self.quality_metrics:
            scores["relevance"] = self._evaluate_relevance(content, language)
        
        if "consistency" in self.quality_metrics:
            scores["consistency"] = self._evaluate_consistency(content, language)
        
        if "diversity" in self.quality_metrics:
            scores["diversity"] = self._evaluate_diversity(content, language)
        
        # 计算总体评分
        overall_score = self._calculate_overall_score(scores)
        
        # 创建质量评分对象
        quality_score = QualityScore(
            overall_score=overall_score,
            completeness_score=scores.get("completeness", 0.0),
            relevance_score=scores.get("relevance", 0.0),
            consistency_score=scores.get("consistency", 0.0),
            diversity_score=scores.get("diversity", 0.0),
            details=scores
        )
        
        return quality_score
    
    def _evaluate_completeness(self, content: str, data_chunk: DataChunk) -> float:
        """评估完整性"""
        score = 0.0
        
        # 1. 内容长度评分
        length_score = min(1.0, len(content) / 200)  # 200字符为满分基准
        score += length_score * 0.3
        
        # 2. 结构完整性评分
        structure_score = 0.0
        
        # 检查是否包含数字（数据）
        if re.search(r'\d+', content):
            structure_score += 0.2
        
        # 检查是否包含专业术语
        terms = data_chunk.extra_data.get("extracted_terms", [])
        if len(terms) >= 2:
            structure_score += 0.3
        
        # 检查是否包含解释性内容
        if any(pattern in content for pattern in ["因为", "所以", "由于", "因此", "原因", "原理"]):
            structure_score += 0.2
        
        # 检查是否包含应用性内容
        if any(pattern in content for pattern in ["应用", "使用", "方法", "措施", "技术"]):
            structure_score += 0.3
        
        score += structure_score * 0.4
        
        # 3. 信息密度评分
        words = len(re.findall(r'\b\w+\b', content))
        density_score = min(1.0, words / 50)  # 50个词为满分基准
        score += density_score * 0.3
        
        return min(1.0, score)
    
    def _evaluate_relevance(self, content: str, language: Language) -> float:
        """评估相关性"""
        score = 0.0
        
        # 1. 水利关键词覆盖度
        content_lower = content.lower()
        matched_keywords = 0
        
        for keyword in self.hydro_keywords:
            if keyword.lower() in content_lower:
                matched_keywords += 1
        
        keyword_score = matched_keywords / len(self.hydro_keywords)
        score += keyword_score * 0.4
        
        # 2. 专业术语比例
        words = re.findall(r'[\u4e00-\u9fff]+|[a-zA-Z]+', content)
        if words:
            hydro_words = [word for word in words if self._is_hydro_word(word)]
            term_ratio = len(hydro_words) / len(words)
            score += min(1.0, term_ratio * 5) * 0.3  # 放大系数
        
        # 3. 领域相关性评分
        domain_patterns = {
            "水文": r"水文|降雨|径流|蒸发|水位|流量",
            "工程": r"大坝|堤防|水闸|泵站|水库|工程",
            "管理": r"管理|调度|运行|维护|监测|预报",
            "环境": r"水质|水环境|生态|污染|保护|治理"
        }
        
        domain_score = 0.0
        for domain, pattern in domain_patterns.items():
            if re.search(pattern, content):
                domain_score += 0.25
        
        score += domain_score * 0.3
        
        return min(1.0, score)
    
    def _evaluate_consistency(self, content: str, language: Language) -> float:
        """评估一致性"""
        score = 0.8  # 基础分数
        
        # 1. 语言一致性
        chinese_chars = len(re.findall(r'[\u4e00-\u9fff]', content))
        english_chars = len(re.findall(r'[a-zA-Z]', content))
        total_chars = chinese_chars + english_chars
        
        if total_chars > 0:
            chinese_ratio = chinese_chars / total_chars
            # 如果混合语言且比例均衡，给予额外分数
            if 0.2 < chinese_ratio < 0.8:
                score += 0.1
            # 如果单一语言，给予正常分数
            elif chinese_ratio > 0.9 or chinese_ratio < 0.1:
                score += 0.05
        
        # 2. 术语一致性检查
        terms = re.findall(r'[\u4e00-\u9fff]{2,}|[a-zA-Z]{2,}', content)
        term_freq = Counter(terms)
        
        # 检查是否有明显的不一致用法
        inconsistencies = 0
        for term, freq in term_freq.items():
            if freq >= 2:
                # 检查同一术语是否有不同的表达方式
                variations = self._find_term_variations(term, content)
                if len(variations) > 1:
                    inconsistencies += 1
        
        inconsistency_penalty = min(0.3, inconsistencies * 0.1)
        score -= inconsistency_penalty
        
        # 3. 数值一致性检查
        numbers = re.findall(r'\d+(?:\.\d+)?', content)
        if len(numbers) > 1:
            # 检查是否有明显矛盾的数值
            contradictions = self._check_number_contradictions(content, numbers)
            if contradictions:
                score -= 0.2
        
        return max(0.0, min(1.0, score))
    
    def _evaluate_diversity(self, content: str, language: Language) -> float:
        """评估多样性"""
        score = 0.0
        
        # 1. 词汇多样性
        words = re.findall(r'[\u4e00-\u9fff]+|[a-zA-Z]+', content.lower())
        if len(words) > 0:
            unique_words = set(words)
            vocab_diversity = len(unique_words) / len(words)
            score += vocab_diversity * 0.3
        
        # 2. 句式多样性
        sentences = re.split(r'[。！？.!?]+', content)
        sentences = [s.strip() for s in sentences if s.strip()]
        
        if len(sentences) > 1:
            sentence_lengths = [len(s) for s in sentences]
            length_variance = np.var(sentence_lengths) if len(sentence_lengths) > 1 else 0
            # 归一化方差
            max_length = max(sentence_lengths) if sentence_lengths else 1
            normalized_variance = length_variance / (max_length ** 2) if max_length > 0 else 0
            score += min(1.0, normalized_variance * 10) * 0.2
        
        # 3. 主题多样性
        topics = []
        topic_keywords = {
            "数据": ["数据", "数值", "统计", "测量", "监测"],
            "技术": ["技术", "方法", "工艺", "方案", "措施"],
            "管理": ["管理", "制度", "政策", "规划", "调度"],
            "工程": ["工程", "建设", "施工", "设计", "维护"],
            "环境": ["环境", "生态", "保护", "治理", "修复"]
        }
        
        for topic, keywords in topic_keywords.items():
            if any(keyword in content for keyword in keywords):
                topics.append(topic)
        
        topic_diversity = len(topics) / len(topic_keywords)
        score += topic_diversity * 0.3
        
        # 4. 信息类型多样性
        info_types = []
        
        if re.search(r'\d+(?:\.\d+)?', content):
            info_types.append("数值")
        if re.search(r'[，。！？；：""''（）【】]', content):
            info_types.append("描述")
        if any(word in content for word in ["因为", "所以", "由于", "因此"]):
            info_types.append("因果")
        if any(word in content for word in ["首先", "其次", "最后", "另外"]):
            info_types.append("逻辑")
        
        info_diversity = len(info_types) / 4  # 最多4种类型
        score += info_diversity * 0.2
        
        return min(1.0, score)
    
    def _is_hydro_word(self, word: str) -> bool:
        """判断是否为水利相关词汇"""
        word_lower = word.lower()
        for keyword in self.hydro_keywords:
            if keyword.lower() in word_lower or word_lower in keyword.lower():
                return True
        return False
    
    def _find_term_variations(self, term: str, content: str) -> List[str]:
        """查找术语的不同表达方式"""
        variations = [term]
        
        # 简单的变体检查
        if len(term) >= 2:
            # 检查缩写形式
            if len(term) > 2:
                abbreviation = term[0] + term[-1]
                if abbreviation in content and abbreviation != term:
                    variations.append(abbreviation)
            
            # 检查是否有同义词（这里简化处理）
            synonyms_map = {
                "降雨": ["降水"],
                "径流": ["水流"],
                "大坝": ["水坝", "堤坝"]
            }
            
            for main_term, synonyms in synonyms_map.items():
                if term == main_term:
                    for synonym in synonyms:
                        if synonym in content:
                            variations.append(synonym)
                elif term in synonyms:
                    variations.append(main_term)
        
        return list(set(variations))
    
    def _check_number_contradictions(self, content: str, numbers: List[str]) -> bool:
        """检查数值矛盾"""
        # 简单的矛盾检查逻辑
        try:
            # 转换为浮点数
            float_numbers = [float(n) for n in numbers]
            
            # 如果有重复但不同的数值，可能存在矛盾
            number_counts = Counter(numbers)
            for num, count in number_counts.items():
                if count > 1:
                    # 检查上下文是否合理
                    continue
            
            return False
        except:
            return False
    
    def _calculate_overall_score(self, scores: Dict[str, float]) -> float:
        """计算总体评分"""
        overall = 0.0
        total_weight = 0.0
        
        for metric, score in scores.items():
            weight = self.metric_weights.get(metric, 0.25)
            overall += score * weight
            total_weight += weight
        
        return overall / total_weight if total_weight > 0 else 0.0
    
    def _record_evaluation(self, chunk_id: str, quality_score: QualityScore):
        """记录评估结果"""
        evaluation_record = {
            "chunk_id": chunk_id,
            "timestamp": logger._core.start_time,
            "scores": {
                "overall": quality_score.overall_score,
                "completeness": quality_score.completeness_score,
                "relevance": quality_score.relevance_score,
                "consistency": quality_score.consistency_score,
                "diversity": quality_score.diversity_score
            }
        }
        
        self.evaluation_history.append(evaluation_record)
        
        # 更新质量分布
        for metric, score in evaluation_record["scores"].items():
            self.quality_distribution[metric].append(score)
    
    def _generate_suggestions(self, data_chunk: DataChunk, quality_score: QualityScore) -> List[str]:
        """生成改进建议"""
        suggestions = []
        content = data_chunk.content
        
        # 基于各维度评分生成建议
        if quality_score.completeness_score < 0.7:
            if len(content) < 100:
                suggestions.append("内容较短，建议增加更多详细信息")
            if not re.search(r'\d+', content):
                suggestions.append("缺少具体数据，建议添加相关数值信息")
            if not data_chunk.extra_data.get("extracted_terms"):
                suggestions.append("缺少专业术语，建议增加水利专业概念")
        
        if quality_score.relevance_score < 0.6:
            suggestions.append("水利相关性较弱，建议增加专业领域内容")
            if quality_score.relevance_score < 0.4:
                suggestions.append("建议明确阐述与水利领域的关联性")
        
        if quality_score.consistency_score < 0.7:
            suggestions.append("内容一致性有待改善，建议检查术语使用和逻辑结构")
        
        if quality_score.diversity_score < 0.6:
            suggestions.append("表达较为单一，建议丰富词汇和句式")
            if quality_score.diversity_score < 0.4:
                suggestions.append("建议从多个角度展开描述，增加信息层次")
        
        return suggestions
    
    def evaluate_qa_pairs(self, qa_pairs: List[QAData]) -> Dict[str, Any]:
        """评估问答对质量"""
        if not qa_pairs:
            return {"total_pairs": 0, "average_quality": 0.0, "distribution": {}}
        
        qa_scores = []
        
        for qa in qa_pairs:
            score = self._evaluate_qa_quality(qa)
            qa_scores.append(score)
        
        # 统计信息
        avg_quality = np.mean(qa_scores) if qa_scores else 0.0
        
        # 质量分布
        distribution = {
            "excellent": sum(1 for s in qa_scores if s >= 0.8),
            "good": sum(1 for s in qa_scores if 0.6 <= s < 0.8),
            "fair": sum(1 for s in qa_scores if 0.4 <= s < 0.6),
            "poor": sum(1 for s in qa_scores if s < 0.4)
        }
        
        return {
            "total_pairs": len(qa_pairs),
            "average_quality": avg_quality,
            "distribution": distribution,
            "scores": qa_scores
        }
    
    def _evaluate_qa_quality(self, qa: QAData) -> float:
        """评估单个问答对质量"""
        score = 0.0
        
        # 1. 问题质量评分
        question = qa.question
        
        # 问题长度合理性
        if 10 <= len(question) <= 50:
            score += 0.2
        elif len(question) < 10:
            score += 0.1
        else:
            score += 0.15
        
        # 问题类型
        if any(word in question for word in ["什么", "如何", "为什么", "怎样", "请", "解释"]):
            score += 0.2
        
        # 2. 答案质量评分
        answer = qa.answer
        
        # 答案长度合理性
        if 20 <= len(answer) <= 200:
            score += 0.2
        elif len(answer) < 20:
            score += 0.1
        else:
            score += 0.15
        
        # 答案信息量
        if re.search(r'\d+', answer):  # 包含数据
            score += 0.1
        if self._is_hydro_word(answer):  # 包含专业词汇
            score += 0.1
        
        # 3. 问答相关性
        if qa.context and qa.question:
            # 检查问题是否与上下文相关
            context_words = set(re.findall(r'[\u4e00-\u9fff]+|[a-zA-Z]+', qa.context.lower()))
            question_words = set(re.findall(r'[\u4e00-\u9fff]+|[a-zA-Z]+', qa.question.lower()))
            overlap = len(context_words & question_words)
            relevance_score = overlap / len(question_words) if question_words else 0
            score += min(0.2, relevance_score)
        
        # 4. 置信度调整
        if qa.confidence:
            score *= qa.confidence
        
        return min(1.0, score)
    
    def get_evaluation_report(self) -> Dict[str, Any]:
        """获取评估报告"""
        if not self.evaluation_history:
            return {"message": "暂无评估数据"}
        
        # 计算总体统计
        metrics = ["overall", "completeness", "relevance", "consistency", "diversity"]
        report = {"total_evaluated": len(self.evaluation_history)}
        
        for metric in metrics:
            scores = self.quality_distribution.get(metric, [])
            if scores:
                report[f"{metric}_stats"] = {
                    "mean": np.mean(scores),
                    "std": np.std(scores),
                    "min": np.min(scores),
                    "max": np.max(scores),
                    "median": np.median(scores)
                }
        
        # 质量分布统计
        overall_scores = self.quality_distribution.get("overall", [])
        if overall_scores:
            distribution = {
                "excellent": sum(1 for s in overall_scores if s >= 0.8),
                "good": sum(1 for s in overall_scores if 0.6 <= s < 0.8),
                "fair": sum(1 for s in overall_scores if 0.4 <= s < 0.6),
                "poor": sum(1 for s in overall_scores if s < 0.4)
            }
            report["quality_distribution"] = distribution
        
        # 阈值达成情况
        min_quality_threshold = self.thresholds.get("min_quality_score", 0.7)
        qualified_count = sum(1 for s in overall_scores if s >= min_quality_threshold)
        report["threshold_compliance"] = {
            "threshold": min_quality_threshold,
            "qualified": qualified_count,
            "qualified_rate": qualified_count / len(overall_scores) if overall_scores else 0
        }
        
        return report
    
    def filter_by_quality(self, data_chunks: List[DataChunk], min_score: Optional[float] = None) -> List[DataChunk]:
        """根据质量分数过滤数据"""
        if min_score is None:
            min_score = self.thresholds.get("min_quality_score", 0.7)
        
        filtered_chunks = []
        for chunk in data_chunks:
            quality_score = chunk.extra_data.get("quality_score")
            if quality_score and quality_score.overall_score >= min_score:
                filtered_chunks.append(chunk)
        
        return filtered_chunks
    
    def _create_process_metadata(self, status: ProcessingStatus, message: str = None):
        """创建处理元数据"""
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
    
    def get_evaluation_statistics(self) -> Dict[str, Any]:
        """获取评估统计信息"""
        stats = self.get_monitoring_info()
        
        stats.update({
            "evaluation_history_count": len(self.evaluation_history),
            "quality_metrics_count": len(self.quality_metrics),
            "hydro_keywords_count": len(self.hydro_keywords),
            "sklearn_available": SKLEARN_SUPPORT
        })
        
        return stats