#!/usr/bin/env python3
"""
水利多模态数据处理工具主入口
OpenHydrology Data Processing Tool

使用方法:
    python main.py --input /path/to/files --output /path/to/output
    python main.py --config config.yaml --input /path/to/directory --recursive
    python main.py --help
"""

import argparse
import sys
import os
from pathlib import Path
from typing import Optional, List
from loguru import logger

# 添加src目录到Python路径
sys.path.insert(0, str(Path(__file__).parent / "src"))

from src.config import config_manager
from src.utils import setup_logging
from src.skills.pipeline import HydroDataPipeline


def setup_argument_parser() -> argparse.ArgumentParser:
    """设置命令行参数解析器"""
    parser = argparse.ArgumentParser(
        description="水利多模态数据处理工具",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例用法:
  # 处理单个文件
  python main.py --input data/report.pdf --output processed_data.json
  
  # 处理目录（递归）
  python main.py --input ./documents --output results/ --recursive
  
  # 使用自定义配置
  python main.py --config custom_config.yaml --input ./data --output output.json
  
  # 生成处理报告
  python main.py --input ./data --report-only
        """
    )
    
    # 输入参数
    parser.add_argument(
        "--input", "-i",
        type=str,
        required=True,
        help="输入文件或目录路径"
    )
    
    # 输出参数
    parser.add_argument(
        "--output", "-o",
        type=str,
        help="输出文件路径（默认：./output/processed_data_TIMESTAMP.json）"
    )
    
    # 配置文件
    parser.add_argument(
        "--config", "-c",
        type=str,
        help="自定义配置文件路径"
    )
    
    # 递归处理
    parser.add_argument(
        "--recursive", "-r",
        action="store_true",
        help="递归处理子目录"
    )
    
    # 批量大小
    parser.add_argument(
        "--batch-size",
        type=int,
        help="批处理大小（覆盖配置文件设置）"
    )
    
    # 并行工作线程数
    parser.add_argument(
        "--workers", "-w",
        type=int,
        help="并行工作线程数（覆盖配置文件设置）"
    )
    
    # 日志级别
    parser.add_argument(
        "--log-level",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        default="INFO",
        help="日志级别"
    )
    
    # 日志文件
    parser.add_argument(
        "--log-file",
        type=str,
        help="日志文件路径"
    )
    
    # 仅生成报告
    parser.add_argument(
        "--report-only",
        action="store_true",
        help="仅生成处理报告，不执行处理"
    )
    
    # 禁用检查点
    parser.add_argument(
        "--no-checkpoint",
        action="store_true",
        help="禁用检查点功能"
    )
    
    # 错误处理模式
    parser.add_argument(
        "--on-error",
        choices=["skip", "stop", "retry"],
        default="skip",
        help="错误处理模式"
    )
    
    # 显示支持的格式
    parser.add_argument(
        "--list-formats",
        action="store_true",
        help="显示支持的文件格式并退出"
    )
    
    # 版本信息
    parser.add_argument(
        "--version",
        action="version",
        version="%(prog)s 1.0.0"
    )
    
    return parser


def setup_custom_config(args) -> bool:
    """设置自定义配置"""
    try:
        # 加载自定义配置文件
        if args.config:
            if not os.path.exists(args.config):
                logger.error(f"配置文件不存在: {args.config}")
                return False
            
            # 重新初始化配置管理器
            global config_manager
            from src.config import ConfigManager
            config_manager = ConfigManager(args.config)
            logger.info(f"加载自定义配置文件: {args.config}")
        
        # 应用命令行参数覆盖
        if args.batch_size:
            config_manager.set("pipeline.batch_size", args.batch_size)
        
        if args.workers:
            config_manager.set("pipeline.max_workers", args.workers)
        
        if args.no_checkpoint:
            config_manager.set("pipeline.checkpoint_enabled", False)
        
        if args.on_error:
            config_manager.set("pipeline.error_handling", args.on_error)
        
        return True
        
    except Exception as e:
        logger.error(f"配置设置失败: {e}")
        return False


def validate_input_path(input_path: str) -> bool:
    """验证输入路径"""
    path = Path(input_path)
    
    if not path.exists():
        logger.error(f"输入路径不存在: {input_path}")
        return False
    
    if not (path.is_file() or path.is_dir()):
        logger.error(f"输入路径不是有效的文件或目录: {input_path}")
        return False
    
    return True


def get_default_output_path() -> str:
    """获取默认输出路径"""
    from src.utils import get_timestamp
    output_dir = Path(config_manager.get('global.output_dir', './output'))
    output_dir.mkdir(parents=True, exist_ok=True)
    
    timestamp = get_timestamp()
    return str(output_dir / f"processed_data_{timestamp}.json")


def process_data(pipeline: HydroDataPipeline, input_path: str, 
                output_path: str, recursive: bool = False) -> bool:
    """处理数据"""
    input_path_obj = Path(input_path)
    
    try:
        if input_path_obj.is_file():
            # 处理单个文件
            logger.info(f"开始处理文件: {input_path}")
            result = pipeline.process_files([input_path], output_path)
        else:
            # 处理目录
            logger.info(f"开始处理目录: {input_path} (递归: {recursive})")
            result = pipeline.process_directory(input_path, recursive, output_path)
        
        # 处理结果
        if result.success:
            logger.info("数据处理完成!")
            logger.info(f"处理时间: {result.processing_time:.2f}秒")
            
            if result.data:
                stats = result.data.get_statistics()
                logger.info(f"生成数据块: {stats['total_chunks']}")
                logger.info(f"生成问答对: {stats['total_qa_pairs']}")
                logger.info(f"总字符数: {stats['total_characters']}")
            
            return True
        else:
            logger.error(f"数据处理失败: {result.error_message}")
            return False
            
    except Exception as e:
        logger.error(f"处理过程中出现异常: {e}")
        return False


def generate_report(pipeline: HydroDataPipeline, input_path: str, 
                   output_path: Optional[str] = None) -> None:
    """生成处理报告"""
    report = pipeline.get_processing_report()
    
    if output_path:
        report_file = output_path
    else:
        from src.utils import get_timestamp
        report_file = f"./processing_report_{get_timestamp()}.json"
    
    try:
        import json
        with open(report_file, 'w', encoding='utf-8') as f:
            json.dump(report, f, ensure_ascii=False, indent=2, default=str)
        
        logger.info(f"处理报告已保存: {report_file}")
        
        # 显示简要信息
        print("\n" + "="*50)
        print("处理报告摘要")
        print("="*50)
        print(f"处理的技能: {', '.join(report['skills'])}")
        print(f"批处理大小: {report['config']['batch_size']}")
        print(f"并行工作线程: {report['config']['max_workers']}")
        print(f"DataFlow集成: {'启用' if report['dataflow_enabled'] else '禁用'}")
        
        if report['statistics']['total_files'] > 0:
            print(f"处理文件数: {report['statistics']['processed_files']}/{report['statistics']['total_files']}")
            print(f"处理数据块数: {report['statistics']['processed_chunks']}")
        
        print("="*50)
        
    except Exception as e:
        logger.error(f"生成报告失败: {e}")


def list_supported_formats(pipeline: HydroDataPipeline) -> None:
    """显示支持的文件格式"""
    formats = pipeline.get_supported_file_formats()
    
    print("\n支持的文件格式:")
    print("-" * 30)
    for fmt in formats:
        print(f"  .{fmt}")
    print("-" * 30)
    print(f"共支持 {len(formats)} 种格式")


def main():
    """主函数"""
    # 解析命令行参数
    parser = setup_argument_parser()
    args = parser.parse_args()
    
    # 设置日志
    setup_logging(args.log_level, args.log_file)
    
    # 显示支持的格式并退出
    if args.list_formats:
        # 创建临时pipeline实例来获取支持的格式
        try:
            pipeline = HydroDataPipeline()
            list_supported_formats(pipeline)
        except Exception as e:
            logger.error(f"初始化失败: {e}")
            sys.exit(1)
        sys.exit(0)
    
    # 验证输入路径
    if not validate_input_path(args.input):
        sys.exit(1)
    
    # 设置配置
    if not setup_custom_config(args):
        sys.exit(1)
    
    # 创建必要的目录
    config_manager.create_directories()
    
    try:
        # 初始化pipeline
        pipeline = HydroDataPipeline()
        
        # 如果只是生成报告
        if args.report_only:
            generate_report(pipeline, args.input, args.output)
            sys.exit(0)
        
        # 确定输出路径
        output_path = args.output if args.output else get_default_output_path()
        
        # 处理数据
        success = process_data(pipeline, args.input, output_path, args.recursive)
        
        if success:
            # 生成处理报告
            generate_report(pipeline, args.input)
            
            print("\n🎉 数据处理成功完成!")
            print(f"📁 输出文件: {output_path}")
            
            sys.exit(0)
        else:
            print("\n❌ 数据处理失败!")
            sys.exit(1)
            
    except KeyboardInterrupt:
        logger.info("用户中断处理")
        print("\n⏹️  处理已中断")
        sys.exit(1)
    except Exception as e:
        logger.error(f"程序执行失败: {e}")
        print(f"\n💥 程序执行失败: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()