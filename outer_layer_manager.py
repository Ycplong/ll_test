import os
import sqlite3
import json
import hashlib
import shutil
import tempfile
from datetime import datetime

# 全局索引库路径
GLOBAL_INDEX_DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'wafer_global_index.db')

class OuterLayerManager:
    def __init__(self):
        # 初始化全局索引库
        self._init_global_index_db()
    
    def _init_global_index_db(self):
        """初始化全局索引库，创建晶圆元数据表"""
        conn = sqlite3.connect(GLOBAL_INDEX_DB_PATH)
        cursor = conn.cursor()
        
        # 创建晶圆元数据表
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS wafer_metadata (
            wafer_id TEXT PRIMARY KEY,
            wafer_name TEXT NOT NULL,
            folder_path TEXT NOT NULL UNIQUE,
            total_defects INTEGER DEFAULT 0,
            labeled_defects INTEGER DEFAULT 0,
            progress REAL DEFAULT 0.00,
            label_status INTEGER DEFAULT 0,
            parsed_status INTEGER NOT NULL DEFAULT 0,
            parse_error TEXT,
            last_operated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            create_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        ''')
        
        # 创建索引
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_folder_path ON wafer_metadata(folder_path)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_label_status ON wafer_metadata(label_status)')
        
        conn.commit()
        conn.close()
    
    def _calculate_wafer_id(self, folder_path):
        """计算晶圆ID：使用文件夹路径的SHA256作为唯一标识"""
        return hashlib.sha256(folder_path.encode()).hexdigest()
    
    def _parse_wafer_folder(self, folder_path):
        """解析晶圆文件夹（模拟内层解析函数）"""
        # 检查raw_data.txt文件是否存在
        raw_data_path = os.path.join(folder_path, 'raw_data.txt')
        if not os.path.exists(raw_data_path):
            raise Exception("raw_data.txt文件不存在")
        
        # 检查图像文件是否存在
        if not os.path.exists(os.path.join(folder_path, 'bright_field.png')) or \
           not os.path.exists(os.path.join(folder_path, 'dark_field.png')):
            raise Exception("明场/暗场图像文件缺失")
        
        # 统计缺陷数量
        with open(raw_data_path, 'r') as f:
            lines = f.readlines()
            # 减去表头行
            defect_count = len(lines) - 1
        
        # 模拟创建内层数据库
        self._create_inner_database(folder_path, raw_data_path, defect_count)
        
        return defect_count
    
    def _create_inner_database(self, folder_path, raw_data_path, defect_count):
        """创建模拟的内层数据库"""
        inner_db_path = os.path.join(folder_path, 'database.db')
        print(f"开始创建内层数据库: {inner_db_path}")
        
        # 确保文件夹存在
        if not os.path.exists(folder_path):
            try:
                os.makedirs(folder_path)
                print(f"已创建晶圆文件夹: {folder_path}")
            except Exception as e:
                print(f"创建文件夹失败: {e}")
                return
        
        # 先删除可能存在的旧数据库
        if os.path.exists(inner_db_path):
            try:
                os.remove(inner_db_path)
                print(f"已删除旧数据库文件")
            except Exception as e:
                print(f"删除旧数据库文件失败: {e}")
        
        conn = None
        try:
            # 创建新的数据库连接
            conn = sqlite3.connect(inner_db_path)
            cursor = conn.cursor()
            
            # 创建缺陷信息表（使用标准SQLite语法）
            print("创建defect_info表结构")
            create_table_sql = '''
            CREATE TABLE IF NOT EXISTS defect_info (
                defect_id TEXT PRIMARY KEY,
                center_x INTEGER,
                center_y INTEGER,
                ai_adc_type INTEGER,
                adc_type INTEGER
            )
            '''
            cursor.execute(create_table_sql)
            
            # 创建索引以提高查询性能
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_defect_id ON defect_info(defect_id)")
            
            # 导入原始数据
            print(f"开始导入原始数据，raw_data_path={raw_data_path}")
            
            # 使用事务批量插入以提高性能
            conn.execute("BEGIN TRANSACTION")
            
            with open(raw_data_path, 'r') as f:
                # 读取所有行并过滤
                lines = [line.strip() for line in f if line.strip() and not line.strip().startswith('#')]
                if not lines:
                    print("警告：raw_data.txt文件为空或只包含注释行")
                    conn.rollback()
                    return
                
                # 跳过表头（第一行）
                data_lines = lines[1:]
                print(f"跳过表头，实际数据行数: {len(data_lines)}")
                
                inserted_count = 0
                skipped_count = 0
                
                # 准备批量插入的数据
                insert_data = []
                for line_num, line in enumerate(data_lines, start=2):  # 从第2行开始计数
                    try:
                        parts = line.split(',')
                        # 严格检查字段数量和格式
                        if len(parts) >= 4:
                            defect_id = parts[0].strip()
                            if not defect_id:
                                print(f"跳过第{line_num}行：缺陷ID为空")
                                skipped_count += 1
                                continue
                            
                            # 转换数值字段
                            try:
                                center_x = int(parts[1].strip())
                                center_y = int(parts[2].strip())
                                ai_adc_type = int(parts[3].strip())
                            except ValueError as ve:
                                print(f"跳过第{line_num}行：数值转换失败: {ve}, 行内容: {line}")
                                skipped_count += 1
                                continue
                            
                            # 添加到批量数据中，初始时adc_type设为None表示未标注状态
                            insert_data.append((defect_id, center_x, center_y, ai_adc_type, None))
                            inserted_count += 1
                        else:
                            print(f"跳过第{line_num}行：字段数量不足: {line}")
                            skipped_count += 1
                    except Exception as e:
                        print(f"处理第{line_num}行失败: {e}, 行内容: {line}")
                        skipped_count += 1
                        continue
                
                # 执行批量插入
                if insert_data:
                    print(f"准备插入{len(insert_data)}条记录")
                    cursor.executemany(
                        "INSERT OR REPLACE INTO defect_info (defect_id, center_x, center_y, ai_adc_type, adc_type) VALUES (?, ?, ?, ?, ?)",
                        insert_data
                    )
                
                # 提交事务
                conn.commit()
            
            # 验证数据插入是否成功
            cursor.execute("SELECT COUNT(*) FROM defect_info")
            actual_count = cursor.fetchone()[0]
            print(f"内层数据库创建完成，计划插入{inserted_count}条，实际插入{actual_count}条，跳过{skipped_count}条")
            
            # 验证表结构是否正确
            cursor.execute("PRAGMA table_info(defect_info)")
            columns = [row[1] for row in cursor.fetchall()]
            required_columns = ['defect_id', 'center_x', 'center_y', 'ai_adc_type', 'adc_type']
            missing_columns = [col for col in required_columns if col not in columns]
            if missing_columns:
                print(f"警告：表结构不完整，缺少列: {missing_columns}")
            
        except sqlite3.Error as e:
            print(f"SQLite错误: {e}")
            import traceback
            traceback.print_exc()
            # 回滚事务
            if conn:
                try:
                    conn.rollback()
                except:
                    pass
        except Exception as e:
            print(f"创建内层数据库失败: {e}")
            import traceback
            traceback.print_exc()
            # 回滚事务
            if conn:
                try:
                    conn.rollback()
                except:
                    pass
        finally:
            # 确保关闭连接
            if conn:
                try:
                    conn.close()
                except:
                    pass
            
            # 如果创建失败，删除数据库文件
            if 'actual_count' not in locals() or actual_count == 0:
                if os.path.exists(inner_db_path):
                    try:
                        os.remove(inner_db_path)
                        print(f"因创建失败，已删除空数据库文件")
                    except Exception as e:
                        print(f"删除失败数据库文件时出错: {e}")
    
    def load_wafer_folders(self, root_dir, recursive=True):
        """加载晶圆文件夹并更新全局索引库"""
        processed_count = 0
        success_count = 0
        fail_count = 0
        
        # 获取所有晶圆文件夹
        wafer_folders = []
        if recursive:
            for root, dirs, files in os.walk(root_dir):
                if 'raw_data.txt' in files:
                    wafer_folders.append(root)
        else:
            # 仅搜索一级目录
            for item in os.listdir(root_dir):
                item_path = os.path.join(root_dir, item)
                if os.path.isdir(item_path) and os.path.exists(os.path.join(item_path, 'raw_data.txt')):
                    wafer_folders.append(item_path)
        
        conn = sqlite3.connect(GLOBAL_INDEX_DB_PATH)
        cursor = conn.cursor()
        
        for folder_path in wafer_folders:
            wafer_id = self._calculate_wafer_id(folder_path)
            wafer_name = os.path.basename(folder_path)
            inner_db_path = os.path.join(folder_path, 'database.db')
            
            try:
                # 检查全局索引库是否已有记录
                cursor.execute("SELECT * FROM wafer_metadata WHERE wafer_id = ?", (wafer_id,))
                existing_record = cursor.fetchone()
                
                # 检查是否存在内层数据库
                has_inner_db = os.path.exists(inner_db_path)
                
                if not existing_record and not has_inner_db:
                    # 首次加载，需要解析
                    total_defects = self._parse_wafer_folder(folder_path)
                    
                    # 插入到全局索引库
                    cursor.execute('''
                    INSERT INTO wafer_metadata 
                    (wafer_id, wafer_name, folder_path, total_defects, 
                     progress, label_status, parsed_status, last_operated)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ''', (wafer_id, wafer_name, folder_path, total_defects, 0, 0.0, 0, 1, datetime.now()))
                    
                    success_count += 1
                    
                elif existing_record or has_inner_db:
                    # 已存在记录或内层数据库，需要刷新数据
                    if not existing_record:
                        # 有内层数据库但无索引记录，需要解析
                        total_defects = self._parse_wafer_folder(folder_path)
                        cursor.execute('''
                        INSERT INTO wafer_metadata 
                        (wafer_id, wafer_name, folder_path, total_defects, 
                         progress, label_status, parsed_status, last_operated)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                        ''', (wafer_id, wafer_name, folder_path, total_defects, 0, 0.0, 0, 1, datetime.now()))
                    else:
                        # 刷新进度和状态 - 强制重新创建内层数据库确保数据一致性
                        print(f"对已存在晶圆强制重新同步进度: {wafer_name}")
                        self._sync_progress(conn, wafer_id, folder_path)
                    
                    success_count += 1
                    
                processed_count += 1
                
            except Exception as e:
                # 解析失败，更新状态
                error_msg = str(e)
                print(f"处理晶圆失败: {wafer_name}, 错误: {error_msg}")
                
                if existing_record:
                    cursor.execute('''
                    UPDATE wafer_metadata 
                    SET parsed_status = 2, parse_error = ?, last_operated = ? 
                    WHERE wafer_id = ?
                    ''', (error_msg, datetime.now(), wafer_id))
                else:
                    cursor.execute('''
                    INSERT INTO wafer_metadata 
                    (wafer_id, wafer_name, folder_path, parsed_status, parse_error, last_operated)
                    VALUES (?, ?, ?, ?, ?, ?)
                    ''', (wafer_id, wafer_name, folder_path, 2, error_msg, datetime.now()))
                
                fail_count += 1
                processed_count += 1
        
        conn.commit()
        conn.close()
        
        return {
            'total_processed': processed_count,
            'success': success_count,
            'failed': fail_count
        }

    def enter_inner_layer(self, wafer_id):
        """进入内层标注功能，准备内层数据库"""
        print(f"尝试进入内层标注: wafer_id={wafer_id}")
        
        conn = sqlite3.connect(GLOBAL_INDEX_DB_PATH)
        cursor = conn.cursor()
        
        # 获取晶圆信息
        cursor.execute("SELECT wafer_name, folder_path FROM wafer_metadata WHERE wafer_id = ?", (wafer_id,))
        result = cursor.fetchone()
        
        if not result:
            conn.close()
            return False, "晶圆不存在"
        
        wafer_name, folder_path = result
        inner_db_path = os.path.join(folder_path, 'database.db')
        raw_data_path = os.path.join(folder_path, 'raw_data.txt')
        
        try:
            # 确保原始数据文件存在
            if not os.path.exists(raw_data_path):
                conn.close()
                return False, "原始数据文件不存在"
            
            # 确保内层数据库存在且有效
            if not os.path.exists(inner_db_path):
                print(f"内层数据库不存在，尝试重建: {inner_db_path}")
                # 重新创建内层数据库
                with open(raw_data_path, 'r') as f:
                    lines = [line.strip() for line in f if line.strip() and not line.strip().startswith('#')]
                    defect_count = len(lines) - 1 if len(lines) > 0 else 0
                
                self._create_inner_database(folder_path, raw_data_path, defect_count)
            
            # 验证内层数据库完整性
            inner_conn = sqlite3.connect(inner_db_path)
            inner_cursor = inner_conn.cursor()
            
            # 检查必要的表是否存在
            inner_cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='defect_info'")
            if not inner_cursor.fetchone():
                inner_conn.close()
                conn.close()
                return False, "内层数据库表结构不完整"
            
            # 检查是否有数据
            inner_cursor.execute("SELECT COUNT(*) FROM defect_info")
            count = inner_cursor.fetchone()[0]
            if count == 0:
                inner_conn.close()
                conn.close()
                return False, "内层数据库没有缺陷数据"
            
            inner_conn.close()
            conn.close()
            
            print(f"成功进入内层标注: {wafer_name}")
            return True, "成功进入内层标注"
        
        except Exception as e:
            print(f"进入内层标注失败: {str(e)}")
            import traceback
            traceback.print_exc()
            
            # 更新解析状态为失败
            cursor.execute('''
            UPDATE wafer_metadata 
            SET parsed_status = 2, parse_error = ?, last_operated = ? 
            WHERE wafer_id = ?
            ''', (f"进入内层失败: {str(e)}", datetime.now(), wafer_id))
            conn.commit()
            conn.close()
            
            return False, str(e)

    def _sync_progress(self, conn, wafer_id, folder_path):
        """同步晶圆标注进度"""
        cursor = conn.cursor()
        inner_db_path = os.path.join(folder_path, 'database.db')
        raw_data_path = os.path.join(folder_path, 'raw_data.txt')
        
        print(f"开始同步晶圆进度: wafer_id={wafer_id}, folder_path={folder_path}")
        
        # 确保raw_data.txt存在
        if not os.path.exists(raw_data_path):
            print(f"错误: raw_data.txt文件不存在: {raw_data_path}")
            # 更新状态为解析失败
            cursor.execute('''
            UPDATE wafer_metadata 
            SET parsed_status = 2, parse_error = ?, last_operated = ? 
            WHERE wafer_id = ?
            ''', ("raw_data.txt文件不存在", datetime.now(), wafer_id))
            return
        
        # 强制删除并重新创建内层数据库，确保状态干净
        if os.path.exists(inner_db_path):
            try:
                os.remove(inner_db_path)
                print(f"已删除旧的内层数据库，准备重新创建: {inner_db_path}")
            except Exception as e:
                print(f"删除旧数据库失败: {e}")
        
        # 重新创建内层数据库
        try:
            # 计算缺陷数量并过滤可能的注释行
            with open(raw_data_path, 'r') as f:
                lines = [line.strip() for line in f if line.strip() and not line.strip().startswith('#')]
                if len(lines) < 1:
                    print(f"错误: raw_data.txt文件为空或格式不正确")
                    cursor.execute('''
                    UPDATE wafer_metadata 
                    SET parsed_status = 2, parse_error = ?, last_operated = ? 
                    WHERE wafer_id = ?
                    ''', ("raw_data.txt文件为空或格式不正确", datetime.now(), wafer_id))
                    return
                defect_count = len(lines) - 1  # 减去表头行
                if defect_count < 0:
                    defect_count = 0
            
            print(f"计算到缺陷数量: {defect_count}")
            self._create_inner_database(folder_path, raw_data_path, defect_count)
            
            # 验证数据库是否创建成功
            if not os.path.exists(inner_db_path):
                print(f"错误: 内层数据库创建失败，文件不存在")
                cursor.execute('''
                UPDATE wafer_metadata 
                SET parsed_status = 2, parse_error = ?, last_operated = ? 
                WHERE wafer_id = ?
                ''', ("内层数据库创建失败", datetime.now(), wafer_id))
                return
            
        except Exception as e:
            print(f"创建内层数据库失败: {e}")
            import traceback
            traceback.print_exc()
            # 更新状态为解析失败
            cursor.execute('''
            UPDATE wafer_metadata 
            SET parsed_status = 2, parse_error = ?, last_operated = ? 
            WHERE wafer_id = ?
            ''', (f"创建数据库失败: {str(e)}", datetime.now(), wafer_id))
            return
        
        try:
            # 连接内层数据库
            inner_conn = sqlite3.connect(inner_db_path)
            inner_cursor = inner_conn.cursor()
            
            # 检查defect_info表是否存在
            print("检查defect_info表是否存在")
            inner_cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='defect_info'")
            if not inner_cursor.fetchone():
                print("错误: defect_info表不存在，数据库创建不完整")
                inner_conn.close()
                cursor.execute('''
                UPDATE wafer_metadata 
                SET parsed_status = 2, parse_error = ?, last_operated = ? 
                WHERE wafer_id = ?
                ''', ("数据库表结构不完整", datetime.now(), wafer_id))
                return
            
            # 统计总缺陷数
            print("执行统计总缺陷数查询")
            inner_cursor.execute("SELECT COUNT(*) FROM defect_info")
            total_defects = inner_cursor.fetchone()[0]
            print(f"总缺陷数: {total_defects}")
            
            # 如果没有数据，可能是导入失败
            if total_defects == 0 and defect_count > 0:
                print(f"警告: 缺陷数据导入失败，预期{defect_count}条，实际0条")
                # 尝试重新导入一次
                print("尝试重新导入缺陷数据...")
                self._create_inner_database(folder_path, raw_data_path, defect_count)
                # 重新查询
                inner_cursor.execute("SELECT COUNT(*) FROM defect_info")
                total_defects = inner_cursor.fetchone()[0]
                print(f"重新导入后总缺陷数: {total_defects}")
            
            # 统计已标注数（使用label_count字段，只要标注次数>=1就视为已标注）
            print("执行统计已标注数查询")
            # 先检查表中是否有label_count字段
            inner_cursor.execute("PRAGMA table_info(defect_info)")
            columns = [col[1] for col in inner_cursor.fetchall()]
            
            if 'label_count' in columns:
                # 使用label_count字段统计已标注的缺陷数量
                inner_cursor.execute("SELECT COUNT(*) FROM defect_info WHERE label_count >= 1")
                labeled_defects = inner_cursor.fetchone()[0]
                print(f"已标注数(基于label_count): {labeled_defects}")
            elif 'severity' in columns:
                # 兼容旧的判断方式
                inner_cursor.execute("SELECT COUNT(*) FROM defect_info WHERE ai_adc_type != adc_type OR (severity IS NOT NULL AND severity != '')")
                labeled_defects = inner_cursor.fetchone()[0]
                print(f"已标注数(基于类型差异或severity值): {labeled_defects}")
            else:
                # 回退到原来的判断方式
                inner_cursor.execute("SELECT COUNT(*) FROM defect_info WHERE ai_adc_type != adc_type")
                labeled_defects = inner_cursor.fetchone()[0]
                print(f"已标注数(基于类型差异): {labeled_defects}")
            
            inner_conn.close()
            
            # 计算进度
            progress = (labeled_defects / total_defects * 100) if total_defects > 0 else 0
            progress = round(progress, 2)  # 保留2位小数
            print(f"计算进度: {progress}%")
            
            # 确定标注状态
            if progress == 100:
                label_status = 2  # 标注完成
            elif labeled_defects > 0:
                label_status = 1  # 标注中
            else:
                label_status = 0  # 未开始
            
            # 更新全局索引库，包括设置解析状态为成功
            print("更新全局索引库，设置解析状态为成功")
            current_time = datetime.now()
            cursor.execute('''
            UPDATE wafer_metadata 
            SET total_defects = ?, labeled_defects = ?, progress = ?, 
                label_status = ?, parsed_status = 1, parse_error = NULL, last_operated = ? 
            WHERE wafer_id = ?
            ''', (total_defects, labeled_defects, progress, label_status, current_time, wafer_id))
            print("同步进度完成，解析状态设置为成功")
            
        except Exception as e:
            print(f"同步进度失败: {e}")
            import traceback
            traceback.print_exc()
            # 更新状态为解析失败
            cursor.execute('''
            UPDATE wafer_metadata 
            SET parsed_status = 2, parse_error = ?, last_operated = ? 
            WHERE wafer_id = ?
            ''', (f"同步失败: {str(e)}", datetime.now(), wafer_id))

    def sync_wafer_progress(self, wafer_id):
        """手动触发晶圆进度同步"""
        conn = sqlite3.connect(GLOBAL_INDEX_DB_PATH)
        cursor = conn.cursor()
        
        # 获取晶圆文件夹路径
        cursor.execute("SELECT folder_path FROM wafer_metadata WHERE wafer_id = ?", (wafer_id,))
        result = cursor.fetchone()
        
        if result:
            folder_path = result[0]
            self._sync_progress(conn, wafer_id, folder_path)
            
        conn.commit()
        conn.close()
        
        return result is not None
    
    def reset_wafer_status(self, wafer_id):
        """重置晶圆状态，删除内层数据库和相关缓存，允许重新加载晶圆"""
        conn = sqlite3.connect(GLOBAL_INDEX_DB_PATH)
        cursor = conn.cursor()
        
        # 获取晶圆文件夹路径
        cursor.execute("SELECT folder_path FROM wafer_metadata WHERE wafer_id = ?", (wafer_id,))
        result = cursor.fetchone()
        
        if result:
            folder_path = result[0]
            inner_db_path = os.path.join(folder_path, 'database.db')
            
            try:
                # 删除内层数据库文件
                if os.path.exists(inner_db_path):
                    os.remove(inner_db_path)
                    print(f"已删除内层数据库: {inner_db_path}")
                
                # 重置晶圆状态为未解析
                cursor.execute('''
                UPDATE wafer_metadata 
                SET parsed_status = 0, parse_error = NULL, last_operated = ? 
                WHERE wafer_id = ?
                ''', (datetime.now(), wafer_id))
                
                conn.commit()
                print(f"已重置晶圆状态: {wafer_id}")
                success = True
            except Exception as e:
                print(f"重置晶圆状态失败: {e}")
                success = False
        else:
            success = False
        
        conn.close()
        return success
    
    def get_wafer_list(self):
        """获取晶圆列表"""
        conn = sqlite3.connect(GLOBAL_INDEX_DB_PATH)
        conn.row_factory = sqlite3.Row  # 允许通过列名访问
        cursor = conn.cursor()
        
        try:
            # 查询所有晶圆信息
            cursor.execute("SELECT * FROM wafer_metadata")
            wafers = []
            
            for row in cursor.fetchall():
                wafer = dict(row)
                # 确保每个晶圆都有完整的必要字段
                wafer.setdefault('wafer_id', '')
                wafer.setdefault('wafer_name', '')
                wafer.setdefault('folder_path', '')
                wafer.setdefault('parsed_status', 0)
                wafer.setdefault('parse_error', '')
                wafer.setdefault('label_status', 0)
                wafer.setdefault('progress', 0.0)
                wafer.setdefault('total_defects', 0)
                wafer.setdefault('labeled_defects', 0)
                wafer.setdefault('last_operated', '')
                wafers.append(wafer)
            
            # 按晶圆名称排序
            wafers.sort(key=lambda x: x['wafer_name'])
            
            return wafers
        except Exception as e:
            print(f"获取晶圆列表失败: {str(e)}")
            return []
        finally:
            conn.close()
    
    def export_wafer_kfl(self, wafer_id, export_all=False):
        """导出单个晶圆的KFL文件"""
        conn = sqlite3.connect(GLOBAL_INDEX_DB_PATH)
        cursor = conn.cursor()
        
        # 获取晶圆信息
        cursor.execute("SELECT wafer_name, folder_path, label_status FROM wafer_metadata WHERE wafer_id = ?", (wafer_id,))
        result = cursor.fetchone()
        
        if not result:
            conn.close()
            return None, "晶圆不存在"
        
        wafer_name, folder_path, label_status = result
        inner_db_path = os.path.join(folder_path, 'database.db')
        
        if not os.path.exists(inner_db_path):
            conn.close()
            return None, "内层数据库不存在"
        
        # 连接内层数据库
        inner_conn = sqlite3.connect(inner_db_path)
        inner_cursor = inner_conn.cursor()
        
        # 查询要导出的缺陷数据
        if export_all or label_status == 2:
            # 导出所有缺陷
            inner_cursor.execute("SELECT * FROM defect_info")
        else:
            # 仅导出已标注的缺陷
            inner_cursor.execute("SELECT * FROM defect_info WHERE ai_adc_type != adc_type")
        
        defects = inner_cursor.fetchall()
        inner_conn.close()
        conn.close()
        
        # 创建临时目录保存导出文件
        temp_dir = tempfile.mkdtemp()
        export_file = os.path.join(temp_dir, f"{wafer_name}_defects.kfl")
        
        # 生成KFL文件（这里简单模拟）
        with open(export_file, 'w') as f:
            f.write(f"KFL Export for {wafer_name}\n")
            f.write("defect_id,center_x,center_y,ai_adc_type,adc_type\n")
            
            for defect in defects:
                f.write(f"{defect[0]},{defect[1]},{defect[2]},{defect[3]},{defect[4]}\n")
        
        return export_file, None
    
    def batch_export_kfl(self, wafer_ids):
        """批量导出多个晶圆的KFL文件"""
        # 创建临时目录保存所有导出文件
        temp_dir = tempfile.mkdtemp()
        export_files = []
        
        for wafer_id in wafer_ids:
            export_file, error = self.export_wafer_kfl(wafer_id)
            if error:
                continue
            
            # 复制到临时目录
            shutil.copy(export_file, os.path.join(temp_dir, os.path.basename(export_file)))
            export_files.append(os.path.join(temp_dir, os.path.basename(export_file)))
        
        # 创建ZIP包
        if export_files:
            zip_file = os.path.join(temp_dir, "batch_export.zip")
            import zipfile
            with zipfile.ZipFile(zip_file, 'w', zipfile.ZIP_DEFLATED) as zf:
                for file in export_files:
                    zf.write(file, os.path.basename(file))
            
            return zip_file, None
        
        return None, "没有找到可导出的晶圆"

# Web API接口层（用于前端调用）
class WebInterface:
    def __init__(self):
        self.manager = OuterLayerManager()
    
    def get_wafer_data(self, wafer_id, defect_id=None):
        """获取晶圆缺陷数据"""
        conn = sqlite3.connect(GLOBAL_INDEX_DB_PATH)
        cursor = conn.cursor()
        
        # 获取晶圆信息
        cursor.execute("SELECT folder_path, wafer_name FROM wafer_metadata WHERE wafer_id = ?", (wafer_id,))
        result = cursor.fetchone()
        conn.close()
        
        if not result:
            return {"success": False, "error": "晶圆不存在"}
        
        folder_path, wafer_name = result
        inner_db_path = os.path.join(folder_path, 'database.db')
        
        try:
            inner_conn = sqlite3.connect(inner_db_path)
            inner_conn.row_factory = sqlite3.Row  # 允许通过列名访问
            inner_cursor = inner_conn.cursor()
            
            # 获取所有缺陷数据
            inner_cursor.execute("SELECT * FROM defect_info")
            defects = []
            for row in inner_cursor.fetchall():
                # 转换为前端需要的格式
                defect = dict(row)
                # 确保每个缺陷都有必要的字段
                defect['id'] = defect.get('defect_id', f'defect_{len(defects)+1}')
                defect['x'] = defect.get('center_x', '--')
                defect['y'] = defect.get('center_y', '--')
                # 映射AI预测类型
                ai_type_map = {1: 'Particle', 2: 'Scratch', 3: 'Stain', 4: 'Pinhole', 5: 'Other'}
                defect['ai_adc_type'] = ai_type_map.get(defect.get('ai_adc_type', 0), '--')
                # 如果已经有手动标注，也映射
                if defect.get('adc_type') and defect.get('adc_type') != defect.get('ai_adc_type'):
                    defect['adc_type'] = ai_type_map.get(defect.get('adc_type', 0), '')
                else:
                    defect['adc_type'] = ''
                # 获取标注次数，默认为0
                defect['label_count'] = defect.get('label_count', 0)
                defect['size'] = '--'
                defect['intensity'] = '--'
                defect['category'] = '--'
                defect['severity'] = ''
                defect['comment'] = ''
                defects.append(defect)
            
            # 返回兼容前端的数据格式
            return {
                "success": True,
                "data": defects,
                "wafer": {
                    "wafer_name": wafer_name,
                    "folder_path": folder_path,
                    "progress": 0,
                    "label_status": 1
                }
            }
        except Exception as e:
            print(f"获取晶圆数据失败: {str(e)}")
            return {"success": False, "error": str(e)}
        finally:
            if 'inner_conn' in locals():
                inner_conn.close()
    
    def save_label(self, wafer_id, defect_index, adc_type, severity=None, comment=None):
        """保存标注信息"""
        # 确保severity和comment有默认值，避免空值问题
        if severity is None:
            severity = ""
        if comment is None:
            comment = ""
        print(f"保存标注: wafer_id={wafer_id}, defect_index={defect_index}, adc_type={adc_type}, severity={severity}")
        
        conn = sqlite3.connect(GLOBAL_INDEX_DB_PATH)
        cursor = conn.cursor()
        
        # 获取晶圆信息
        cursor.execute("SELECT folder_path FROM wafer_metadata WHERE wafer_id = ?", (wafer_id,))
        result = cursor.fetchone()
        conn.close()
        
        if not result:
            return {"success": False, "error": "晶圆不存在"}
        
        folder_path = result[0]
        inner_db_path = os.path.join(folder_path, 'database.db')
        
        try:
            # 转换adc_type为数字
            type_map = {'Particle': 1, 'Scratch': 2, 'Stain': 3, 'Pinhole': 4, 'Other': 5}
            adc_type_num = type_map.get(adc_type, 0)
            
            inner_conn = sqlite3.connect(inner_db_path)
            inner_cursor = inner_conn.cursor()
            
            try:
                # 确保表中有severity和comment字段
                inner_cursor.execute("PRAGMA table_info(defect_info)")
                columns = [col[1] for col in inner_cursor.fetchall()]
                
                # 获取所有缺陷，找到对应索引的缺陷
                inner_cursor.execute("SELECT defect_id FROM defect_info")
                defect_ids = [row[0] for row in inner_cursor.fetchall()]
                
                if 0 <= defect_index < len(defect_ids):
                    target_defect_id = defect_ids[defect_index]
                    
                    # 确保表中有必要的字段
                    if 'severity' not in columns:
                        inner_cursor.execute("ALTER TABLE defect_info ADD COLUMN severity TEXT")
                    if 'comment' not in columns:
                        inner_cursor.execute("ALTER TABLE defect_info ADD COLUMN comment TEXT")
                    if 'label_time' not in columns:
                        inner_cursor.execute("ALTER TABLE defect_info ADD COLUMN label_time TIMESTAMP")
                    if 'label_count' not in columns:
                        inner_cursor.execute("ALTER TABLE defect_info ADD COLUMN label_count INTEGER DEFAULT 0")
                    inner_conn.commit()
                    
                    # 更新缺陷标注，包括adc_type、severity、comment、时间戳和标注次数+1
                    inner_cursor.execute(
                        "UPDATE defect_info SET adc_type = ?, severity = ?, comment = ?, label_time = ?, label_count = label_count + 1 WHERE defect_id = ?",
                        (adc_type_num, severity, comment, datetime.now().strftime('%Y-%m-%d %H:%M:%S'), target_defect_id)
                    )
                    inner_conn.commit()
                    
                    print(f"标注保存成功: {wafer_id}, defect_id={target_defect_id}, severity={severity}")
            finally:
                # 确保数据库连接关闭
                if inner_conn:
                    inner_conn.close()
            
            # 保存成功后，同步晶圆进度
            self.manager.sync_wafer_progress(wafer_id)
            print(f"已同步晶圆进度: {wafer_id}")
            
            return {"success": True, "message": "标注保存成功"}
        except Exception as e:
            print(f"保存标注失败: {str(e)}")
            return {"success": False, "error": str(e)}
    
    def handle_request(self, action, params):
        """处理前端请求"""
        if action == "load_folders":
            root_dir = params.get("root_dir")
            recursive = params.get("recursive", True)
            
            # 修复路径问题：如果前端只传递了文件夹名称，在当前工作目录下查找
            if root_dir and not os.path.isabs(root_dir):
                # 检查是否是当前目录下的子文件夹
                if os.path.exists(os.path.join(os.path.dirname(os.path.abspath(__file__)), root_dir)):
                    root_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), root_dir)
                else:
                    # 默认为当前目录
                    root_dir = os.path.dirname(os.path.abspath(__file__))
                    
            result = self.manager.load_wafer_folders(root_dir, recursive)
            
            # 加载完成后，重新获取更新后的晶圆列表
            wafers = self.manager.get_wafer_list()
            return {
                "success": True,
                "result": result,
                "wafers": wafers
            }
        
        elif action == "get_wafer_list":
            wafers = self.manager.get_wafer_list()
            # 确保返回的数据格式与前端预期一致
            return wafers
            
        elif action == "get_wafer_data":
            wafer_id = params.get("wafer_id")
            return self.get_wafer_data(wafer_id)
            
        elif action == "save_label":
            wafer_id = params.get("wafer_id")
            defect_index = params.get("defect_index")
            adc_type = params.get("adc_type")
            severity = params.get("severity", "")  # 允许空的severity值
            comment = params.get("comment", "")  # 允许空的comment值
            return self.save_label(wafer_id, defect_index, adc_type, severity, comment)
        
        elif action == "sync_progress":
            wafer_id = params.get("wafer_id")
            return {"success": self.manager.sync_wafer_progress(wafer_id)}
        
        elif action == "export_kfl":
            wafer_id = params.get("wafer_id")
            export_all = params.get("export_all", False)
            file_path, error = self.manager.export_wafer_kfl(wafer_id, export_all)
            if error:
                return {"success": False, "error": error}
            return {"success": True, "file_path": file_path}
        
        elif action == "batch_export_kfl":
            wafer_ids = params.get("wafer_ids", [])
            file_path, error = self.manager.batch_export_kfl(wafer_ids)
            if error:
                return {"success": False, "error": error}
            return {"success": True, "file_path": file_path}
        
        elif action == "reset_wafer_status":
            wafer_id = params.get("wafer_id")
            success = self.manager.reset_wafer_status(wafer_id)
            return {"success": success}
        
        elif action == "enter_inner_layer":
            wafer_id = params.get("wafer_id")
            success, error = self.manager.enter_inner_layer(wafer_id)
            result = {"success": success, "error": error}
            if success:
                result["redirect_url"] = f"/inner_labeling.html?wafer_id={wafer_id}"
            return result

        
        else:
            return {"success": False, "error": "未知的操作"}

# 简单的服务器实现，用于演示
if __name__ == "__main__":
    import http.server
    import socketserver
    import json
    from urllib.parse import parse_qs
    
    web_interface = WebInterface()
    
    class Handler(http.server.BaseHTTPRequestHandler):
        def do_GET(self):
            # 忽略Vite客户端请求
            if "@vite/client" in self.path:
                self.send_response(404)
                self.end_headers()
                return
            
            # 处理图像请求
            if self.path.startswith("/api/get_image"):
                self._handle_image_request()
                return
            
            # 特殊处理内层标注页面（带查询参数）
            if self.path.startswith("/inner_labeling.html"):
                file_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "inner_labeling.html")
                if os.path.exists(file_path):
                    self.send_response(200)
                    self.send_header("Content-Type", "text/html")
                    self.end_headers()
                    with open(file_path, "rb") as f:
                        self.wfile.write(f.read())
                else:
                    self.send_response(404)
                    self.end_headers()
                    self.wfile.write("File not found".encode())
                return
            
            # 静态文件服务
            if self.path == "/" or self.path.startswith("/?"):
                self.path = "/frontend.html"
            
            try:
                if self.path.startswith("/api/"):
                    # API请求处理
                    self.send_response(200)
                    self.send_header("Content-Type", "application/json")
                    self.send_header("Access-Control-Allow-Origin", "*")
                    self.end_headers()
                    
                    # 解析请求路径和参数
                    parts = self.path.split("?")
                    api_path = parts[0].replace("/api/", "")
                    params = {}
                    
                    if len(parts) > 1:
                        params = parse_qs(parts[1])
                        # 将参数从列表转换为单个值
                        for key in params:
                            params[key] = params[key][0]
                    
                    # 处理API请求
                    result = web_interface.handle_request(api_path, params)
                    self.wfile.write(json.dumps(result).encode())
                else:
                    # 静态文件服务
                    file_path = self.path[1:]  # 去掉前导斜杠
                    if not file_path:
                        file_path = "frontend.html"
                    
                    # 确保文件存在
                    if not os.path.exists(file_path):
                        # 特殊处理内层标注页面
                        if file_path == "inner_labeling.html":
                            # 尝试在当前目录查找内层标注页面
                            inner_labeling_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "inner_labeling.html")
                            if os.path.exists(inner_labeling_path):
                                with open(inner_labeling_path, "rb") as f:
                                    self.send_response(200)
                                    self.send_header("Content-Type", "text/html")
                                    self.end_headers()
                                    self.wfile.write(f.read())
                                return
                        
                        self.send_response(404)
                        self.end_headers()
                        self.wfile.write(b"File not found")
                        return
                    
                    with open(file_path, "rb") as f:
                        self.send_response(200)
                        if file_path.endswith(".html"):
                            self.send_header("Content-Type", "text/html")
                        elif file_path.endswith(".js"):
                            self.send_header("Content-Type", "application/javascript")
                        elif file_path.endswith(".css"):
                            self.send_header("Content-Type", "text/css")
                        self.end_headers()
                        self.wfile.write(f.read())
            except Exception as e:
                self.send_response(500)
                self.send_header("Content-Type", "text/plain")
                self.end_headers()
                self.wfile.write(f"Error: {str(e)}".encode())
                print(f"Server error: {str(e)}")
                
        def do_POST(self):
            # 处理POST请求
            content_length = int(self.headers['Content-Length'])
            post_data = self.rfile.read(content_length)
            
            try:
                data = json.loads(post_data)
                action = data.get("action")
                params = data.get("params", {})
                
                # 处理请求
                result = web_interface.handle_request(action, params)
                
                # 返回结果
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Access-Control-Allow-Origin", "*")
                self.end_headers()
                self.wfile.write(json.dumps(result).encode())
            except Exception as e:
                self.send_response(500)
                self.send_header("Content-Type", "application/json")
                self.send_header("Access-Control-Allow-Origin", "*")
                self.end_headers()
                error_response = {"success": False, "error": str(e)}
                self.wfile.write(json.dumps(error_response).encode())
                print(f"API错误: {str(e)}")
                
        def _handle_image_request(self):
            # 解析请求参数
            parts = self.path.split("?")
            params = {}
            
            if len(parts) > 1:
                from urllib.parse import parse_qs
                params = parse_qs(parts[1])
                # 将参数从列表转换为单个值
                for key in params:
                    params[key] = params[key][0]
            
            wafer_id = params.get('wafer_id')
            image_path = params.get('image_path')
            
            if not wafer_id or not image_path:
                self.send_response(400)
                self.end_headers()
                self.wfile.write("Missing required parameters".encode())
                return
            
            try:
                # 获取晶圆信息
                conn = sqlite3.connect(GLOBAL_INDEX_DB_PATH)
                cursor = conn.cursor()
                cursor.execute("SELECT folder_path FROM wafer_metadata WHERE wafer_id = ?", (wafer_id,))
                result = cursor.fetchone()
                conn.close()
                
                if not result:
                    self.send_response(404)
                    self.end_headers()
                    self.wfile.write("Wafer not found".encode())
                    return
                
                folder_path = result[0]
                full_image_path = os.path.join(folder_path, image_path)
                
                if os.path.exists(full_image_path):
                    self.send_response(200)
                    # 设置正确的Content-Type
                    if full_image_path.endswith(".jpg") or full_image_path.endswith(".jpeg"):
                        self.send_header("Content-Type", "image/jpeg")
                    elif full_image_path.endswith(".png"):
                        self.send_header("Content-Type", "image/png")
                    else:
                        self.send_header("Content-Type", "application/octet-stream")
                    self.end_headers()
                    with open(full_image_path, "rb") as f:
                        self.wfile.write(f.read())
                else:
                    self.send_response(404)
                    self.end_headers()
                    self.wfile.write("Image file not found".encode())
            except Exception as e:
                self.send_response(500)
                self.end_headers()
                self.wfile.write(str(e).encode())
    




# 为了兼容可能的旧调用方式，保留这个方法
def save_label_compat(self, wafer_id, defect_id, label_data):
    """保存缺陷标注数据"""
    conn = sqlite3.connect(GLOBAL_INDEX_DB_PATH)
    cursor = conn.cursor()
    
    # 获取晶圆信息
    cursor.execute("SELECT folder_path FROM wafer_metadata WHERE wafer_id = ?", (wafer_id,))
    result = cursor.fetchone()
    conn.close()
    
    if not result:
        return {"success": False, "error": "晶圆不存在"}
    
    folder_path = result[0]
    inner_db_path = os.path.join(folder_path, 'database.db')
    
    try:
        inner_conn = sqlite3.connect(inner_db_path)
        inner_cursor = inner_conn.cursor()
        
        # 检查defect_info表是否有label列
        inner_cursor.execute("PRAGMA table_info(defect_info)")
        columns = [row[1] for row in inner_cursor.fetchall()]
        
        if 'label' not in columns:
            # 添加label列
            inner_cursor.execute("ALTER TABLE defect_info ADD COLUMN label TEXT")
        if 'label_time' not in columns:
            inner_cursor.execute("ALTER TABLE defect_info ADD COLUMN label_time TIMESTAMP")
        if 'is_labeled' not in columns:
            inner_cursor.execute("ALTER TABLE defect_info ADD COLUMN is_labeled INTEGER DEFAULT 0")
        
        # 保存标注数据（暂时更新所有记录，因为没有id列）
        inner_cursor.execute('''
            UPDATE defect_info 
            SET label = ?, label_time = ?, is_labeled = 1
        ''', (json.dumps(label_data), datetime.now()))
        
        inner_conn.commit()
        inner_conn.close()
        
        # 更新全局索引库中的进度信息
        self.manager.sync_wafer_progress(wafer_id)
        
        return {"success": True, "message": "标注保存成功"}
    except Exception as e:
        print(f"保存标注失败: {str(e)}")
        if 'inner_conn' in locals():
            inner_conn.rollback()
            inner_conn.close()
        return {"success": False, "error": str(e)}

# 启动服务器
PORT = 8002
with socketserver.TCPServer(("", PORT), Handler) as httpd:
    print(f"服务器已启动，访问 http://localhost:{PORT}")
    httpd.serve_forever()