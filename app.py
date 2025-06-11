from flask import Flask, request, jsonify
from flask_restx import Api, Resource, fields, Namespace
import redis
import os
import json
import uuid
import fnmatch
from datetime import datetime
import time
from functools import wraps
import threading
from collections import defaultdict
import hashlib

app = Flask(__name__)

# 配置 Swagger
api = Api(
    app,
    version='2.0',
    title='Key-Value-Type Server API',
    description='''
    一個高效能的 Redis 結構化 Key-Value-Type 儲存服務
    
    ## 功能特色：
    - ✅ 支援單筆與批次操作
    - ✅ 結構化資料儲存 (key-type-value)
    - ✅ 完整的 REST API
    - ✅ 自動去重與更新機制
    - ✅ 模式搜尋與類型篩選
    - ✅ 高併發與緩存優化
    
    ## 批次操作：
    - **批次寫入**: POST /api/v1/records/batch/set
    - **批次查詢**: POST /api/v1/records/batch/get
    
    ## 向後相容：
    所有端點都提供根路由的向後相容支援
    ''',
    doc='/swagger/',
    prefix='/api/v1',
    contact='API Support',
    contact_email='support@example.com'
)

# 建立命名空間
ns_health = Namespace('health', description='健康檢查相關操作')
ns_records = Namespace('records', description='記錄相關操作')
ns_query = Namespace('query', description='查詢相關操作')

api.add_namespace(ns_health, path='/health')
api.add_namespace(ns_records, path='/records') 
api.add_namespace(ns_query, path='/query')

# 定義資料模型
record_input_model = api.model('RecordInput', {
    'key': fields.String(required=True, description='資料鍵名', example='temperature_sensor_01'),
    'type': fields.String(required=True, description='資料類型標籤', example='temperature'),
    'value': fields.Float(required=True, description='數值（整數或浮點數）', example=25.5)
})

record_output_model = api.model('RecordOutput', {
    'id': fields.String(description='內部唯一識別碼'),
    'key': fields.String(description='資料鍵名'),
    'type': fields.String(description='資料類型標籤'),
    'value': fields.Float(description='數值'),
    'updated_at': fields.String(description='最後更新時間 (ISO 8601)')
})

success_response_model = api.model('SuccessResponse', {
    'message': fields.String(description='成功訊息'),
    'id': fields.String(description='記錄的內部 ID'),
    'key': fields.String(description='記錄的 key'),
    'type': fields.String(description='記錄的 type'),
    'value': fields.Float(description='記錄的 value'),
    'updated_at': fields.String(description='最後更新時間 (ISO 8601)')
})

records_list_model = api.model('RecordsList', {
    'records': fields.List(fields.Nested(record_output_model), description='記錄列表'),
    'count': fields.Integer(description='記錄總數')
})

type_filtered_model = api.model('TypeFilteredRecords', {
    'type': fields.String(description='篩選的類型'),
    'records': fields.List(fields.Nested(record_output_model), description='符合類型的記錄'),
    'count': fields.Integer(description='符合記錄總數')
})

# 批次操作模型
batch_input_model = api.model('BatchInput', {
    'records': fields.List(fields.Nested(record_input_model), required=True, description='批次記錄列表')
})

batch_get_input_model = api.model('BatchGetInput', {
    'keys': fields.List(fields.String, required=True, description='要查詢的 key 列表', example=['temperature_sensor_01', 'humidity_sensor_02'])
})

batch_response_model = api.model('BatchResponse', {
    'message': fields.String(description='批次操作結果訊息'),
    'successful_count': fields.Integer(description='成功處理的記錄數量'),
    'failed_count': fields.Integer(description='失敗的記錄數量'),
    'results': fields.List(fields.Nested(success_response_model), description='成功處理的記錄詳情'),
    'errors': fields.List(fields.String, description='錯誤訊息列表')
})

error_model = api.model('Error', {
    'error': fields.String(description='錯誤訊息')
})

health_model = api.model('Health', {
    'status': fields.String(description='服務狀態'),
    'message': fields.String(description='狀態訊息'),
    'redis_connected': fields.Boolean(description='Redis 連線狀態')
})

# Redis 連線設定 - 優化連接池配置
redis_host = os.getenv('REDIS_HOST', 'localhost')
redis_port = int(os.getenv('REDIS_PORT', 6379))
redis_password = os.getenv('REDIS_PASSWORD', None)

# 創建 Redis 連接池以提高併發性能 - 優化配置
redis_pool = redis.ConnectionPool(
    host=redis_host,
    port=redis_port,
    password=redis_password,
    decode_responses=True,
    max_connections=200,  # 增加最大連接數
    retry_on_timeout=True,
    socket_connect_timeout=3,  # 減少連接超時
    socket_timeout=3,  # 減少操作超時
    retry_on_error=[redis.BusyLoadingError, redis.ConnectionError],
    socket_keepalive=True,
    socket_keepalive_options={},
    health_check_interval=30  # 健康檢查間隔
)

try:
    r = redis.Redis(connection_pool=redis_pool)
    # 測試連線
    r.ping()
    print(f"✅ 成功連接到 Redis: {redis_host}:{redis_port}")
except redis.ConnectionError:
    print(f"❌ 無法連接到 Redis: {redis_host}:{redis_port}")
    r = None

# 改進的本地緩存系統
class OptimizedCache:
    def __init__(self, max_size=1000, default_ttl=30):
        self.cache = {}
        self.access_times = {}
        self.max_size = max_size
        self.default_ttl = default_ttl
        self.lock = threading.RLock()
        
    def get(self, key):
        with self.lock:
            if key in self.cache:
                data, timestamp = self.cache[key]
                if time.time() - timestamp < self.default_ttl:
                    self.access_times[key] = time.time()
                    return data
                else:
                    # 清理過期數據
                    del self.cache[key]
                    if key in self.access_times:
                        del self.access_times[key]
            return None
    
    def set(self, key, value):
        with self.lock:
            current_time = time.time()
            
            # 如果緩存已滿，移除最久未使用的項目
            if len(self.cache) >= self.max_size and key not in self.cache:
                self._evict_lru()
            
            self.cache[key] = (value, current_time)
            self.access_times[key] = current_time
    
    def _evict_lru(self):
        if not self.access_times:
            return
        
        # 找到最久未使用的 key
        lru_key = min(self.access_times.items(), key=lambda x: x[1])[0]
        del self.cache[lru_key]
        del self.access_times[lru_key]
    
    def clear(self):
        with self.lock:
            self.cache.clear()
            self.access_times.clear()

# 全局優化緩存實例
optimized_cache = OptimizedCache(max_size=2000, default_ttl=60)

# 索引系統 - 加速查詢
class RecordIndex:
    def __init__(self):
        self.key_index = defaultdict(set)  # key -> set of record_ids
        self.type_index = defaultdict(set)  # type -> set of record_ids
        self.key_type_index = {}  # (key, type) -> record_id
        self.lock = threading.RLock()
        self.dirty = True
    
    def rebuild_index(self):
        """重建索引"""
        with self.lock:
            if not self.dirty:
                return
            
            self.key_index.clear()
            self.type_index.clear()
            self.key_type_index.clear()
            
            if r is None:
                return
            
            try:
                # 使用 pipeline 批次獲取所有記錄
                pipe = r.pipeline()
                all_keys = r.keys('record:*')
                
                if not all_keys:
                    self.dirty = False
                    return
                
                for redis_key in all_keys:
                    pipe.get(redis_key)
                
                values = pipe.execute()
                
                for redis_key, value in zip(all_keys, values):
                    if value:
                        try:
                            record = json.loads(value)
                            if (isinstance(record, dict) and 
                                'key' in record and 'type' in record):
                                
                                record_id = redis_key.replace('record:', '')
                                key = record['key']
                                type_val = record['type']
                                
                                self.key_index[key].add(record_id)
                                self.type_index[type_val].add(record_id)
                                self.key_type_index[(key, type_val)] = record_id
                        except (json.JSONDecodeError, Exception):
                            continue
                
                self.dirty = False
            except Exception as e:
                print(f"索引重建失敗: {e}")
    
    def get_by_key(self, key):
        """根據 key 獲取記錄 ID"""
        with self.lock:
            if self.dirty:
                self.rebuild_index()
            return list(self.key_index.get(key, []))
    
    def get_by_type(self, type_val):
        """根據 type 獲取記錄 ID"""
        with self.lock:
            if self.dirty:
                self.rebuild_index()
            return list(self.type_index.get(type_val, []))
    
    def get_by_key_type(self, key, type_val):
        """根據 key-type 組合獲取記錄 ID"""
        with self.lock:
            if self.dirty:
                self.rebuild_index()
            return self.key_type_index.get((key, type_val))
    
    def mark_dirty(self):
        """標記索引需要更新"""
        with self.lock:
            self.dirty = True

# 全局索引實例
record_index = RecordIndex()

def optimized_cache_decorator(ttl=60):
    """優化的緩存裝飾器"""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            # 生成緩存鍵
            cache_key_data = f"{func.__name__}:{str(args)}:{str(sorted(kwargs.items()))}"
            cache_key = hashlib.md5(cache_key_data.encode()).hexdigest()
            
            # 嘗試從緩存獲取
            cached_result = optimized_cache.get(cache_key)
            if cached_result is not None:
                return cached_result
            
            # 執行函數並緩存結果
            result = func(*args, **kwargs)
            optimized_cache.set(cache_key, result)
            
            return result
        return wrapper
    return decorator

def get_records_by_ids(record_ids):
    """批次獲取記錄"""
    if not record_ids or r is None:
        return []
    
    try:
        # 使用 pipeline 批次獲取
        pipe = r.pipeline()
        redis_keys = [f'record:{rid}' for rid in record_ids]
        
        for redis_key in redis_keys:
            pipe.get(redis_key)
        
        values = pipe.execute()
        
        records = []
        for redis_key, value in zip(redis_keys, values):
            if value:
                try:
                    record = json.loads(value)
                    if isinstance(record, dict):
                        # 確保必要欄位存在
                        if 'updated_at' not in record:
                            record['updated_at'] = 'N/A'
                        if 'id' not in record:
                            record['id'] = redis_key.replace('record:', '')
                        records.append(record)
                except (json.JSONDecodeError, Exception):
                    continue
        
        return records
    except Exception as e:
        print(f"批次獲取記錄失敗: {e}")
        return []

@ns_health.route('/')
class HealthCheck(Resource):
    @ns_health.doc('health_check')
    @ns_health.marshal_with(health_model)
    def get(self):
        """健康檢查端點"""
        return {
            'status': 'ok',
            'message': 'Flask API 運行中',
            'redis_connected': r is not None and r.ping()
        }

@ns_records.route('/set')
class SetRecord(Resource):
    @ns_records.doc('set_record')
    @ns_records.expect(record_input_model)
    @ns_records.marshal_with(success_response_model, code=200)
    @ns_records.response(400, 'Bad Request', error_model)
    @ns_records.response(500, 'Internal Server Error', error_model)
    def post(self):
        """建立新記錄或更新現有記錄 - 優化版本"""
        try:
            data = request.get_json()
            if not data or 'key' not in data or 'type' not in data or 'value' not in data:
                api.abort(400, '需要提供 key、type 和 value 三個參數')
            
            key = data['key']
            type_value = data['type']
            value = data['value']
            
            # 驗證參數類型
            if not isinstance(type_value, str):
                api.abort(400, 'type 必須是字串')
            
            if not isinstance(value, (int, float)):
                api.abort(400, 'value 必須是數字')
            
            if r is None:
                api.abort(500, 'Redis 連線失敗')
            
            current_time = datetime.now().isoformat()
            
            # 使用索引快速查找現有記錄
            existing_record_id = record_index.get_by_key_type(key, type_value)
            
            if existing_record_id:
                # 更新現有記錄
                redis_key = f'record:{existing_record_id}'
                stored_data = r.get(redis_key)
                if stored_data:
                    record = json.loads(stored_data)
                    record['value'] = value
                    record['updated_at'] = current_time
                    
                    r.set(redis_key, json.dumps(record, ensure_ascii=False))
                    
                    # 清理相關緩存
                    optimized_cache.clear()
                    
                    return {
                        'message': f'成功更新記錄 (key: {key}, type: {type_value}, value: {value})',
                        'id': existing_record_id,
                        'key': key,
                        'type': type_value,
                        'value': value,
                        'updated_at': current_time
                    }
            
            # 建立新記錄
            record_id = str(uuid.uuid4())
            redis_key = f'record:{record_id}'
            
            record = {
                'id': record_id,
                'key': key,
                'type': type_value,
                'value': value,
                'updated_at': current_time
            }
            
            r.set(redis_key, json.dumps(record, ensure_ascii=False))
            
            # 標記索引需要更新
            record_index.mark_dirty()
            optimized_cache.clear()
            
            return {
                'message': f'成功建立新記錄 (key: {key}, value: {value})',
                'id': record_id,
                'key': key,
                'type': type_value,
                'value': value,
                'updated_at': current_time
            }
        
        except Exception as e:
            api.abort(500, str(e))

@ns_records.route('/get/<string:key>')
class GetRecord(Resource):
    @ns_records.doc('get_record')
    @ns_records.marshal_with(records_list_model, code=200)
    @ns_records.response(404, 'Not Found', error_model)
    @ns_records.response(500, 'Internal Server Error', error_model)
    @optimized_cache_decorator(ttl=30)
    def get(self, key):
        """取得指定 key 的所有記錄 - 優化版本"""
        try:
            if r is None:
                api.abort(500, 'Redis 連線失敗')
            
            # 使用索引快速查找
            record_ids = record_index.get_by_key(key)
            
            if not record_ids:
                api.abort(404, f'找不到 key: {key}')
            
            # 批次獲取記錄
            records = get_records_by_ids(record_ids)
            
            if not records:
                api.abort(404, f'找不到 key: {key}')
            
            return {
                'records': records,
                'count': len(records)
            }
        
        except Exception as e:
            api.abort(500, str(e))

@ns_records.route('/delete/<string:key>')
class DeleteRecord(Resource):
    @ns_records.doc('delete_record')
    @ns_records.response(200, 'Success')
    @ns_records.response(404, 'Not Found', error_model)
    @ns_records.response(500, 'Internal Server Error', error_model)
    def delete(self, key):
        """刪除指定 key 的所有記錄 - 優化版本"""
        try:
            if r is None:
                api.abort(500, 'Redis 連線失敗')
            
            # 使用索引快速查找
            record_ids = record_index.get_by_key(key)
            
            if not record_ids:
                api.abort(404, f'找不到 key: {key}')
            
            # 批次刪除
            redis_keys = [f'record:{rid}' for rid in record_ids]
            deleted_count = r.delete(*redis_keys)
            
            if deleted_count == 0:
                api.abort(404, f'找不到 key: {key}')
            
            # 更新索引和緩存
            record_index.mark_dirty()
            optimized_cache.clear()
            
            return {'message': f'成功刪除 {deleted_count} 筆記錄 (key: {key})'}
        
        except Exception as e:
            api.abort(500, str(e))

@ns_query.route('/keys')
class ListAllRecords(Resource):
    @ns_query.doc('list_all_records')
    @ns_query.marshal_with(records_list_model, code=200)
    @ns_query.response(500, 'Internal Server Error', error_model)
    @ns_query.param('pattern', '搜尋模式 (預設: *)', type=str, default='*')
    def get(self):
        """列出所有記錄"""
        try:
            if r is None:
                api.abort(500, 'Redis 連線失敗')
            
            pattern = request.args.get('pattern', '*')
            # 搜尋所有記錄
            keys = r.keys('record:*')
            
            # 取得所有記錄的詳細資訊
            records = []
            for redis_key in keys:
                try:
                    value = r.get(redis_key)
                    if value:
                        record = json.loads(value)
                        if isinstance(record, dict) and 'key' in record and 'type' in record and 'value' in record:
                            # 如果舊記錄沒有 updated_at，加入預設值
                            if 'updated_at' not in record:
                                record['updated_at'] = 'N/A'
                            if 'id' not in record:
                                record['id'] = redis_key.replace('record:', '')
                            
                            # 根據 pattern 篩選 key
                            if pattern == '*' or self._match_pattern(record['key'], pattern):
                                records.append(record)
                        else:
                            # 舊格式資料，跳過或標記
                            if pattern == '*':
                                records.append({
                                    'id': redis_key.replace('record:', ''),
                                    'key': redis_key,
                                    'type': 'unknown',
                                    'value': 'invalid_format',
                                    'updated_at': 'N/A',
                                    'error': '資料格式不正確'
                                })
                except (json.JSONDecodeError, Exception):
                    if pattern == '*':
                        records.append({
                            'id': redis_key.replace('record:', ''),
                            'key': redis_key,
                            'type': 'unknown',
                            'value': 'parse_error',
                            'updated_at': 'N/A',
                            'error': '解析失敗'
                        })
            
            return {'records': records, 'count': len(records)}
        
        except Exception as e:
            api.abort(500, str(e))
    
    def _match_pattern(self, key, pattern):
        """簡單的 pattern 匹配"""
        import fnmatch
        return fnmatch.fnmatch(key, pattern)

@ns_query.route('/keys/type/<string:type_filter>')
class GetRecordsByType(Resource):
    @ns_query.doc('get_records_by_type')
    @ns_query.marshal_with(type_filtered_model, code=200)
    @ns_query.response(500, 'Internal Server Error', error_model)
    @ns_query.param('pattern', '搜尋模式 (預設: *)', type=str, default='*')
    @optimized_cache_decorator(ttl=60)
    def get(self, type_filter):
        """根據 type 篩選記錄 - 優化版本"""
        try:
            if r is None:
                api.abort(500, 'Redis 連線失敗')
            
            pattern = request.args.get('pattern', '*')
            
            # 使用索引快速查找
            record_ids = record_index.get_by_type(type_filter)
            
            if not record_ids:
                return {
                    'type': type_filter,
                    'records': [],
                    'count': 0
                }
            
            # 批次獲取記錄
            records = get_records_by_ids(record_ids)
            
            # 根據 pattern 篩選
            if pattern != '*':
                records = [r for r in records if self._match_pattern(r.get('key', ''), pattern)]
            
            return {
                'type': type_filter,
                'records': records,
                'count': len(records)
            }
        
        except Exception as e:
            api.abort(500, str(e))
    
    def _match_pattern(self, key, pattern):
        """簡單的 pattern 匹配"""
        import fnmatch
        return fnmatch.fnmatch(key, pattern)

@ns_records.route('/flush')
class FlushAllRecords(Resource):
    @ns_records.doc('flush_all_records')
    @ns_records.response(200, 'Success')
    @ns_records.response(500, 'Internal Server Error', error_model)
    def post(self):
        """清空所有資料"""
        try:
            if r is None:
                api.abort(500, 'Redis 連線失敗')
            
            # 刪除所有記錄
            keys = r.keys('record:*')
            if keys:
                r.delete(*keys)
                return {'message': f'成功清空 {len(keys)} 筆記錄'}
            else:
                return {'message': '沒有記錄需要清空'}
        
        except Exception as e:
            api.abort(500, str(e))

@ns_records.route('/batch/set')
class BatchSetRecords(Resource):
    @ns_records.doc('batch_set_records')
    @ns_records.expect(batch_input_model)
    @ns_records.marshal_with(batch_response_model, code=200)
    @ns_records.response(400, 'Bad Request', error_model)
    @ns_records.response(500, 'Internal Server Error', error_model)
    def post(self):
        """批次建立或更新多筆記錄 - 優化版本"""
        try:
            data = request.get_json()
            if not data or 'records' not in data:
                api.abort(400, '需要提供 records 陣列')
            
            records = data['records']
            if not isinstance(records, list) or len(records) == 0:
                api.abort(400, 'records 必須是非空陣列')
            
            if r is None:
                api.abort(500, 'Redis 連線失敗')
            
            results = []
            errors = []
            successful_count = 0
            failed_count = 0
            current_time = datetime.now().isoformat()
            
            # 使用 pipeline 進行批次操作
            pipe = r.pipeline()
            operations = []
            
            for i, record_data in enumerate(records):
                try:
                    # 驗證單筆記錄
                    if not isinstance(record_data, dict):
                        errors.append(f'記錄 {i+1}: 資料格式錯誤')
                        failed_count += 1
                        continue
                    
                    if 'key' not in record_data or 'type' not in record_data or 'value' not in record_data:
                        errors.append(f'記錄 {i+1}: 缺少必要欄位 (key, type, value)')
                        failed_count += 1
                        continue
                    
                    key = record_data['key']
                    type_value = record_data['type']
                    value = record_data['value']
                    
                    # 驗證參數類型
                    if not isinstance(type_value, str):
                        errors.append(f'記錄 {i+1}: type 必須是字串')
                        failed_count += 1
                        continue
                    
                    if not isinstance(value, (int, float)):
                        errors.append(f'記錄 {i+1}: value 必須是數字')
                        failed_count += 1
                        continue
                    
                    # 檢查是否已存在
                    existing_record_id = record_index.get_by_key_type(key, type_value)
                    
                    if existing_record_id:
                        # 更新現有記錄
                        redis_key = f'record:{existing_record_id}'
                        record = {
                            'id': existing_record_id,
                            'key': key,
                            'type': type_value,
                            'value': value,
                            'updated_at': current_time
                        }
                        
                        pipe.set(redis_key, json.dumps(record, ensure_ascii=False))
                        operations.append(('update', i, existing_record_id, key, type_value, value))
                    else:
                        # 建立新記錄
                        record_id = str(uuid.uuid4())
                        redis_key = f'record:{record_id}'
                        
                        record = {
                            'id': record_id,
                            'key': key,
                            'type': type_value,
                            'value': value,
                            'updated_at': current_time
                        }
                        
                        pipe.set(redis_key, json.dumps(record, ensure_ascii=False))
                        operations.append(('create', i, record_id, key, type_value, value))
                    
                except Exception as e:
                    errors.append(f'記錄 {i+1}: {str(e)}')
                    failed_count += 1
            
            # 執行批次操作
            try:
                pipe.execute()
                
                # 處理結果
                for op_type, index, record_id, key, type_value, value in operations:
                    if op_type == 'update':
                        results.append({
                            'message': f'更新記錄 (key: {key}, type: {type_value})',
                            'id': record_id,
                            'key': key,
                            'type': type_value,
                            'value': value,
                            'updated_at': current_time
                        })
                    else:  # create
                        results.append({
                            'message': f'建立新記錄 (key: {key})',
                            'id': record_id,
                            'key': key,
                            'type': type_value,
                            'value': value,
                            'updated_at': current_time
                        })
                    successful_count += 1
                
                # 更新索引和緩存
                record_index.mark_dirty()
                optimized_cache.clear()
                
            except Exception as e:
                # 如果批次操作失敗，記錄錯誤
                errors.append(f'批次操作失敗: {str(e)}')
                failed_count += len(operations)
            
            return {
                'message': f'批次處理完成：成功 {successful_count} 筆，失敗 {failed_count} 筆',
                'successful_count': successful_count,
                'failed_count': failed_count,
                'results': results,
                'errors': errors
            }
        
        except Exception as e:
            api.abort(500, str(e))

@ns_records.route('/batch/get')
class BatchGetRecords(Resource):
    @ns_records.doc('batch_get_records')
    @ns_records.expect(batch_get_input_model)
    @ns_records.marshal_with(records_list_model, code=200)
    @ns_records.response(400, 'Bad Request', error_model)
    @ns_records.response(500, 'Internal Server Error', error_model)
    def post(self):
        """批次取得多個 key 的記錄"""
        try:
            data = request.get_json()
            if not data or 'keys' not in data:
                api.abort(400, '需要提供 keys 陣列')
            
            keys = data['keys']
            if not isinstance(keys, list) or len(keys) == 0:
                api.abort(400, 'keys 必須是非空陣列')
            
            if r is None:
                api.abort(500, 'Redis 連線失敗')
            
            # 搜尋所有記錄
            all_redis_keys = r.keys('record:*')
            matching_records = []
            
            for redis_key in all_redis_keys:
                try:
                    value = r.get(redis_key)
                    if value:
                        record = json.loads(value)
                        if (isinstance(record, dict) and 
                            'key' in record and 'type' in record and 'value' in record and
                            record['key'] in keys):
                            # 如果舊記錄沒有必要欄位，加入預設值
                            if 'updated_at' not in record:
                                record['updated_at'] = 'N/A'
                            if 'id' not in record:
                                record['id'] = redis_key.replace('record:', '')
                            matching_records.append(record)
                except (json.JSONDecodeError, Exception):
                    continue
            
            return {
                'records': matching_records,
                'count': len(matching_records)
            }
        
        except Exception as e:
            api.abort(500, str(e))

# 為了向後兼容，保留一些根路由
@app.route('/', methods=['GET'])
def root_health_check():
    """根路由健康檢查（向後兼容）"""
    return jsonify({
        'status': 'ok',
        'message': 'Flask API 運行中',
        'redis_connected': r is not None and r.ping(),
        'swagger_url': '/swagger/',
        'api_prefix': '/api/v1'
    })

@app.route('/set', methods=['POST'])
def root_set():
    """根路由設定記錄（向後兼容）"""
    response = app.test_client().post('/api/v1/records/set', 
                                     json=request.get_json(),
                                     headers={'Content-Type': 'application/json'})
    return response.get_json(), response.status_code

@app.route('/get/<key>', methods=['GET'])
def root_get(key):
    """根路由取得記錄（向後兼容）"""
    response = app.test_client().get(f'/api/v1/records/get/{key}')
    return response.get_json(), response.status_code

@app.route('/delete/<key>', methods=['DELETE'])
def root_delete(key):
    """根路由刪除記錄（向後兼容）"""
    response = app.test_client().delete(f'/api/v1/records/delete/{key}')
    return response.get_json(), response.status_code

@app.route('/keys', methods=['GET'])
def root_keys():
    """根路由列出記錄（向後兼容）"""
    pattern = request.args.get('pattern', '*')
    response = app.test_client().get(f'/api/v1/query/keys?pattern={pattern}')
    return response.get_json(), response.status_code

@app.route('/keys/type/<type_filter>', methods=['GET'])
def root_keys_by_type(type_filter):
    """根路由依類型篩選（向後兼容）"""
    pattern = request.args.get('pattern', '*')
    response = app.test_client().get(f'/api/v1/query/keys/type/{type_filter}?pattern={pattern}')
    return response.get_json(), response.status_code

@app.route('/flush', methods=['POST'])
def root_flush():
    """根路由清空資料（向後兼容）"""
    response = app.test_client().post('/api/v1/records/flush')
    return response.get_json(), response.status_code

@app.route('/batch/set', methods=['POST'])
def root_batch_set():
    """根路由批次設定記錄（向後兼容）"""
    response = app.test_client().post('/api/v1/records/batch/set', 
                                     json=request.get_json(),
                                     headers={'Content-Type': 'application/json'})
    return response.get_json(), response.status_code

@app.route('/batch/get', methods=['POST'])
def root_batch_get():
    """根路由批次取得記錄（向後兼容）"""
    response = app.test_client().post('/api/v1/records/batch/get',
                                     json=request.get_json(),
                                     headers={'Content-Type': 'application/json'})
    return response.get_json(), response.status_code

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)
