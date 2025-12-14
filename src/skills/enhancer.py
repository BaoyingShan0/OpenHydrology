"""
水利数据增强器Skill
负责增强和丰富清洗后的数据，生成问答对，提取专业术语，关联知识图谱
"""

import re
import random
from typing import List, Dict, Any, Optional, Tuple, Set
from collections import defaultdict, Counter
from loguru import logger

try:
    import jieba
    import jieba.posseg as pseg
    JIEBA_SUPPORT = True
except ImportError:
    JIEBA_SUPPORT = False
    logger.warning("jieba库未安装，中文NLP功能将不可用")

try:
    from transformers import AutoTokenizer, AutoModelForSeq2SeqLM
    TRANSFORMERS_SUPPORT = True
except ImportError:
    TRANSFORMERS_SUPPORT = False
    logger.warning("transformers库未安装，高级生成功能将不可用")

from .base import MonitoredSkill
from ..models import DataChunk, QAData, Language, ProcessingStatus, KnowledgeBase


class HydroDataEnhancer(MonitoredSkill):
    """
    水利数据增强器
    
    功能：
    1. 自动生成高质量的问答对
    2. 提取水利专业术语和实体
    3. 丰富数据内容，增加专业背景
    4. 构建和更新知识图谱
    5. 多模态数据增强
    6. 领域知识关联
    """
    
    def __init__(self, config_key: str = "enhancer"):
        super().__init__("HydroDataEnhancer", config_key)
        
        # 从配置获取参数
        self.enable_qa_generation = self.get_config_value("enable_qa_generation", True)
        self.enable_term_extraction = self.get_config_value("enable_term_extraction", True)
        self.enable_knowledge_enrichment = self.get_config_value("enable_knowledge_enrichment", True)
        
        # 领域知识配置
        domain_config = self.get_config_value("domain_knowledge", {})
        self.water_resources_enabled = domain_config.get("water_resources", True)
        self.hydrology_enabled = domain_config.get("hydrology", True)
        self.meteorology_enabled = domain_config.get("meteorology", True)
        self.environmental_enabled = domain_config.get("environmental", True)
        
        # 初始化水利知识库
        self._init_hydro_knowledge_base()
        
        # 初始化NLP组件
        self._init_nlp_components()
        
        # 问答生成模板
        self._init_qa_templates()
        
        # 统计信息
        self.generated_qa_count = 0
        self.extracted_terms_count = 0
        
        self.log_processing_info("数据增强器初始化完成")
    
    def get_required_config_keys(self) -> List[str]:
        """获取必需的配置键"""
        return []
    
    def _init_hydro_knowledge_base(self):
        """初始化水利知识库"""
        self.knowledge_base = KnowledgeBase()
        
        # 水利专业术语
        hydro_terms = {
            # 基础水文概念
            "水文": ["水文学", "水文分析", "水文资料"],
            "降雨": ["降水", "降雨量", "降水量", "降雨强度"],
            "径流": ["地表径流", "地下径流", "径流量", "径流系数"],
            "蒸发": ["蒸发量", "蒸发能力", "蒸散发"],
            "渗透": ["入渗", "下渗", "渗透率", "渗透系数"],
            
            # 水体和地形
            "流域": ["集水区", "汇水区", "流域面积", "分水岭"],
            "河流": ["河道", "河床", "河岸", "河口"],
            "湖泊": ["水库", "人工湖", "天然湖", "湖泊水位"],
            "地下水": ["含水层", "地下水位", "地下水补给"],
            
            # 水工建筑物
            "大坝": ["水坝", "堤坝", "重力坝", "拱坝"],
            "水闸": ["节制闸", "泄洪闸", "冲沙闸"],
            "堤防": ["防洪堤", "海堤", "河堤"],
            "泵站": ["抽水站", "排水泵站", "灌溉泵站"],
            
            # 水利应用
            "防洪": ["防洪工程", "防洪标准", "洪水预警"],
            "灌溉": ["农田灌溉", "节水灌溉", "灌溉制度"],
            "排涝": ["排水", "除涝", "排水系统"],
            "供水": ["水源地", "供水工程", "水质保障"],
            
            # 水环境
            "水质": ["水环境", "水污染", "水质监测"],
            "水生态": ["生态系统", "生物多样性", "生态保护"],
            "水土保持": ["土壤侵蚀", "植被恢复", "综合治理"]
        }
        
        for term, aliases in hydro_terms.items():
            self.knowledge_base.add_term(term, aliases)
        
        # 水利实体信息
        entities = {
            "长江": {"type": "河流", "length": "6300km", "basin_area": "180万km²"},
            "黄河": {"type": "河流", "length": "5464km", "basin_area": "79.5万km²"},
            "三峡": {"type": "水利工程", "purpose": "防洪、发电、航运"},
            "南水北调": {"type": "调水工程", "routes": ["东线", "中线", "西线"]},
        }
        
        for entity_id, entity_info in entities.items():
            self.knowledge_base.add_entity(entity_id, entity_info)
        
        # 关系信息
        relationships = [
            ("长江", "流经", "三峡"),
            ("三峡", "是", "水利工程"),
            ("南水北调", "连接", "长江"),
            ("南水北调", "连接", "黄河"),
        ]
        
        for subject, relation, object_ in relationships:
            self.knowledge_base.add_relationship(subject, relation, object_)
    
    def _init_nlp_components(self):
        """初始化NLP组件"""
        self.nlp_models = {}
        
        # 加载中文模型
        if JIEBA_SUPPORT:
            # 添加水利专业词汇到jieba词典
            for term in self.knowledge_base.terms.keys():
                jieba.add_word(term, freq=1000, tag='HYDRO')
            
            logger.info("jieba水利专业词典加载完成")
        
        # 加载问答生成模型（如果可用）
        self.qa_model = None
        self.qa_tokenizer = None
        
        if TRANSFORMERS_SUPPORT and self.enable_qa_generation:
            try:
                # 使用中文问答生成模型
                model_name = "ClueAI/ChatYuan-large-v2"  # 示例模型
                self.qa_tokenizer = AutoTokenizer.from_pretrained(model_name)
                self.qa_model = AutoModelForSeq2SeqLM.from_pretrained(model_name)
                logger.info("问答生成模型加载成功")
            except Exception as e:
                logger.warning(f"问答生成模型加载失败: {e}")
                self.qa_model = None
    
    def _init_qa_templates(self):
        """初始化问答生成模板"""
        self.qa_templates = {
            "定义型": [
                "什么是{term}？",
                "{term}的定义是什么？",
                "请解释一下{term}的含义。"
            ],
            "原理型": [
                "{term}的原理是什么？",
                "{term}是如何工作的？",
                "请说明{term}的工作机制。"
            ],
            "应用型": [
                "{term}在实际中有什么应用？",
                "{term}的主要用途是什么？",
                "如何应用{term}解决实际问题？"
            ],
            "计算型": [
                "如何计算{term}？",
                "{term}的计算公式是什么？",
                "请说明{term}的计算方法。"
            ],
            "比较型": [
                "{term}和{other}有什么区别？",
                "{term}与{other}相比有什么优势？",
                "请比较{term}和{other}的特点。"
            ]
        }
    
    def process_single(self, data_chunk: DataChunk) -> DataChunk:
        """处理单个数据块"""
        if not self.validate_input(data_chunk):
            return data_chunk
        
        try:
            enhanced_chunk = data_chunk
            
            # 1. 专业术语提取
            if self.enable_term_extraction:
                terms = self._extract_terms(enhanced_chunk.content, enhanced_chunk.language)
                if terms:
                    enhanced_chunk.extra_data["extracted_terms"] = terms
                    self.extracted_terms_count += len(terms)
                    self.log_processing_info(f"提取到 {len(terms)} 个专业术语")
            
            # 2. 问答对生成
            if self.enable_qa_generation:
                qa_pairs = self._generate_qa_pairs(enhanced_chunk)
                if qa_pairs:
                    enhanced_chunk.extra_data["generated_qa"] = [
                        {"question": qa.question, "answer": qa.answer} for qa in qa_pairs
                    ]
                    self.generated_qa_count += len(qa_pairs)
                    self.log_processing_info(f"生成了 {len(qa_pairs)} 个问答对")
            
            # 3. 知识丰富化
            if self.enable_knowledge_enrichment:
                enriched_content = self._enrich_content(enhanced_chunk)
                if enriched_content != enhanced_chunk.content:
                    enhanced_chunk.extra_data["original_content"] = enhanced_chunk.content
                    enhanced_chunk.content = enriched_content
                    enhanced_chunk.extra_data["knowledge_enriched"] = True
            
            # 4. 领域标注
            domain_tags = self._assign_domain_tags(enhanced_chunk.content)
            if domain_tags:
                enhanced_chunk.extra_data["domain_tags"] = domain_tags
            
            # 标记已增强
            enhanced_chunk.extra_data["enhanced"] = True
            enhanced_chunk.extra_data["enhancement_time"] = logger._core.start_time
            
            self.log_processing_info(f"数据增强完成: {enhanced_chunk.id}")
            return enhanced_chunk
            
        except Exception as e:
            self.log_processing_info(f"数据增强失败 {data_chunk.id}: {e}", "error")
            data_chunk.add_process_metadata(self._create_process_metadata(
                ProcessingStatus.FAILED, str(e)
            ))
            return data_chunk
    
    def _extract_terms(self, text: str, language: Language) -> List[Dict[str, Any]]:
        """提取专业术语"""
        terms = []
        
        if language == Language.CHINESE or (language == Language.AUTO and self._contains_chinese(text)):
            terms = self._extract_chinese_terms(text)
        elif language == Language.ENGLISH:
            terms = self._extract_english_terms(text)
        else:
            # 混合语言处理
            terms = self._extract_chinese_terms(text)
            terms.extend(self._extract_english_terms(text))
        
        # 去重和过滤
        unique_terms = []
        seen_terms = set()
        
        for term in terms:
            term_text = term["term"]
            if term_text not in seen_terms and len(term_text) > 1:
                seen_terms.add(term_text)
                unique_terms.append(term)
        
        return unique_terms
    
    def _extract_chinese_terms(self, text: str) -> List[Dict[str, Any]]:
        """提取中文专业术语"""
        terms = []
        
        if not JIEBA_SUPPORT:
            return terms
        
        try:
            # 使用jieba进行词性标注
            words = pseg.cut(text)
            
            for word, flag in words:
                # 检查是否为水利专业术语
                if word in self.knowledge_base.terms:
                    terms.append({
                        "term": word,
                        "type": "专业术语",
                        "confidence": 0.9,
                        "pos": flag,
                        "aliases": self.knowledge_base.terms[word]
                    })
                # 检查是否为实体
                elif word in self.knowledge_base.entities:
                    terms.append({
                        "term": word,
                        "type": "实体",
                        "confidence": 0.95,
                        "entity_info": self.knowledge_base.entities[word]
                    })
                # 检查是否为复合专业术语（2-4个汉字）
                elif (2 <= len(word) <= 4 and 
                      self._is_hydro_term(word)):
                    terms.append({
                        "term": word,
                        "type": "疑似专业术语",
                        "confidence": 0.7,
                        "pos": flag
                    })
        
        except Exception as e:
            logger.error(f"中文术语提取失败: {e}")
        
        return terms
    
    def _extract_english_terms(self, text: str) -> List[Dict[str, Any]]:
        """提取英文专业术语"""
        terms = []
        
        # 英文水利术语列表（示例）
        english_hydro_terms = {
            'hydrology', 'water', 'river', 'dam', 'reservoir', 'flood',
            'drought', 'precipitation', 'evaporation', 'runoff', 'watershed',
            'groundwater', 'irrigation', 'drainage', 'water quality', 'ecosystem'
        }
        
        words = re.findall(r'\b[a-zA-Z]+\b', text.lower())
        
        for word in words:
            if word in english_hydro_terms:
                terms.append({
                    "term": word,
                    "type": "English专业术语",
                    "confidence": 0.8
                })
        
        return terms
    
    def _is_hydro_term(self, word: str) -> bool:
        """判断是否为水利相关术语"""
        hydro_indicators = ['水', '文', '雨', '洪', '涝', '旱', '河', '湖', '库', '坝', '闸', '泵', '灌', '排']
        return any(indicator in word for indicator in hydro_indicators)
    
    def _generate_qa_pairs(self, data_chunk: DataChunk) -> List[QAData]:
        """生成问答对"""
        qa_pairs = []
        content = data_chunk.content
        
        # 方法1：基于模板的问答生成
        template_qa = self._generate_template_qa(data_chunk)
        qa_pairs.extend(template_qa)
        
        # 方法2：基于模型的问答生成（如果模型可用）
        if self.qa_model is not None:
            model_qa = self._generate_model_qa(data_chunk)
            qa_pairs.extend(model_qa)
        
        # 方法3：基于内容的问答生成
        content_qa = self._generate_content_based_qa(data_chunk)
        qa_pairs.extend(content_qa)
        
        return qa_pairs[:5]  # 限制每个数据块最多生成5个问答对
    
    def _generate_template_qa(self, data_chunk: DataChunk) -> List[QAData]:
        """基于模板生成问答对"""
        qa_pairs = []
        terms = data_chunk.extra_data.get("extracted_terms", [])
        
        for term_info in terms[:3]:  # 只处理前3个术语
            term = term_info["term"]
            
            # 随机选择问答类型
            qa_type = random.choice(list(self.qa_templates.keys()))
            templates = self.qa_templates[qa_type]
            template = random.choice(templates)
            
            # 生成问题
            if qa_type == "比较型" and len(terms) > 1:
                other_term = random.choice([t["term"] for t in terms if t["term"] != term])
                question = template.format(term=term, other=other_term)
            else:
                question = template.format(term=term)
            
            # 生成答案（基于内容查找或生成）
            answer = self._generate_answer_for_term(term, data_chunk.content, qa_type)
            
            if answer:
                qa_pairs.append(QAData(
                    question=question,
                    answer=answer,
                    context=data_chunk.content[:200] + "..." if len(data_chunk.content) > 200 else data_chunk.content,
                    domain=self._determine_domain(term),
                    confidence=0.8
                ))
        
        return qa_pairs
    
    def _generate_answer_for_term(self, term: str, content: str, qa_type: str) -> str:
        """为术语生成答案"""
        # 查找术语在内容中的位置
        term_positions = []
        start = 0
        while True:
            pos = content.find(term, start)
            if pos == -1:
                break
            term_positions.append(pos)
            start = pos + 1
        
        if not term_positions:
            return self._generate_default_answer(term, qa_type)
        
        # 提取包含术语的句子或段落
        answers = []
        for pos in term_positions:
            # 提取前后文（前后各100个字符）
            start_pos = max(0, pos - 100)
            end_pos = min(len(content), pos + len(term) + 100)
            context = content[start_pos:end_pos].strip()
            
            if len(context) > 20:
                answers.append(context)
        
        # 选择最合适的答案
        if answers:
            return max(answers, key=len)
        else:
            return self._generate_default_answer(term, qa_type)
    
    def _generate_default_answer(self, term: str, qa_type: str) -> str:
        """生成默认答案"""
        default_answers = {
            "定义型": f"{term}是水利领域的重要概念，具体定义需要结合专业文献确定。",
            "原理型": f"{term}的工作原理涉及多个水利专业知识点，需要详细分析。",
            "应用型": f"{term}在水利工程中有重要应用，具体应用场景需要根据实际情况确定。",
            "计算型": f"{term}的计算方法需要参考相关的水利计算规范和手册。",
            "比较型": f"{term}与其他概念的比较需要从多个维度进行分析。"
        }
        
        return default_answers.get(qa_type, f"关于{term}的详细信息需要进一步分析。")
    
    def _generate_model_qa(self, data_chunk: DataChunk) -> List[QAData]:
        """基于模型生成问答对"""
        # 这里需要实际的问答生成模型
        # 由于模型加载和推理较复杂，这里提供框架
        return []
    
    def _generate_content_based_qa(self, data_chunk: DataChunk) -> List[QAData]:
        """基于内容生成问答对"""
        qa_pairs = []
        content = data_chunk.content
        
        # 寻找包含具体数值的数据点
        number_pattern = r'\d+(?:\.\d+)?\s*(?:km|mm|m|℃|%|万|亿|吨|立方米)'
        numbers = re.findall(number_pattern, content)
        
        for i, num in enumerate(numbers[:2]):  # 最多处理2个数值
            # 查找包含该数值的句子
            num_pos = content.find(num)
            if num_pos != -1:
                # 提取句子
                start = max(0, num_pos - 50)
                end = min(len(content), num_pos + len(num) + 50)
                sentence = content[start:end].strip()
                
                # 生成问答
                question = f"文中提到的{num}这个数据代表什么含义？"
                answer = f"根据原文内容，{sentence}"
                
                qa_pairs.append(QAData(
                    question=question,
                    answer=answer,
                    context=sentence,
                    domain="数据",
                    confidence=0.7
                ))
        
        return qa_pairs
    
    def _enrich_content(self, data_chunk: DataChunk) -> str:
        """丰富内容"""
        content = data_chunk.content
        terms = data_chunk.extra_data.get("extracted_terms", [])
        
        # 为专业术语添加解释
        for term_info in terms:
            term = term_info["term"]
            if term_info.get("type") == "专业术语":
                # 查找术语在内容中的位置
                if term in content:
                    # 在术语后添加简短解释（如果还没有的话）
                    explanation = self._get_term_explanation(term)
                    if explanation and term + explanation not in content:
                        content = content.replace(term, f"{term}（{explanation}）")
        
        return content
    
    def _get_term_explanation(self, term: str) -> str:
        """获取术语解释"""
        # 简单的术语解释映射
        explanations = {
            "水文": "研究水的各种现象和规律",
            "降雨": "大气中的水汽凝结后降落到地面的现象",
            "径流": "降水或融雪形成的地表水流",
            "蒸发": "水从液态转变为气态的过程",
            "流域": "分水线所包围的集水区域",
            "防洪": "防止洪水灾害的各种措施",
            "灌溉": "人为补给农田水分的措施"
        }
        
        return explanations.get(term, "")
    
    def _assign_domain_tags(self, content: str) -> List[str]:
        """分配领域标签"""
        domains = []
        
        domain_keywords = {
            "水资源": ["水资源", "水量", "用水", "供水", "节水"],
            "水文学": ["水文", "降雨", "径流", "蒸发", "渗透"],
            "水工程": ["大坝", "水闸", "堤防", "泵站", "水库"],
            "水环境": ["水质", "水环境", "污染", "生态", "保护"],
            "防洪": ["洪水", "防洪", "防汛", "预警", "调度"],
            "灌溉": ["灌溉", "农田", "排水", "旱情", "抗旱"]
        }
        
        for domain, keywords in domain_keywords.items():
            if any(keyword in content for keyword in keywords):
                domains.append(domain)
        
        return domains
    
    def _determine_domain(self, term: str) -> str:
        """确定术语所属领域"""
        domain_mapping = {
            "水文": "水文学",
            "降雨": "水文学",
            "径流": "水文学",
            "大坝": "水工程",
            "防洪": "防洪",
            "灌溉": "灌溉",
            "水质": "水环境"
        }
        
        return domain_mapping.get(term, "综合")
    
    def _contains_chinese(self, text: str) -> bool:
        """检查文本是否包含中文字符"""
        return bool(re.search(r'[\u4e00-\u9fff]', text))
    
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
    
    def get_enhancement_statistics(self) -> Dict[str, Any]:
        """获取增强统计信息"""
        stats = self.get_monitoring_info()
        
        stats.update({
            "generated_qa_count": self.generated_qa_count,
            "extracted_terms_count": self.extracted_terms_count,
            "knowledge_base_terms": len(self.knowledge_base.terms),
            "knowledge_base_entities": len(self.knowledge_base.entities),
            "qa_model_loaded": self.qa_model is not None
        })
        
        return stats
    
    def get_knowledge_base(self) -> KnowledgeBase:
        """获取知识库"""
        return self.knowledge_base
    
    def update_knowledge_base(self, new_terms: List[str], new_entities: Dict[str, Dict] = None):
        """更新知识库"""
        for term in new_terms:
            self.knowledge_base.add_term(term)
        
        if new_entities:
            for entity_id, entity_info in new_entities.items():
                self.knowledge_base.add_entity(entity_id, entity_info)
        
        self.log_processing_info(f"知识库更新完成：{len(new_terms)} 个新术语，{len(new_entities) or 0} 个新实体")