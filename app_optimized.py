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

app = Flask(__name__)

# 配置 Swagger
api = Api(
    app,
    version='1.0',
    title='Key-Value-Type Server API',
    description='一個使用 Redis 的結構化 Key-Value-Type 儲存服務',
    doc='/swagger/',
    prefix='/api/v1'
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

error_model = api.model('Error', {
    'error': fields.String(description='錯誤訊息')
})

health_model = api.model('Health', {
    'status': fields.String(description='服務狀態'),
    'message': fields.String(description='狀態訊息'),
    'redis_connected': fields.Boolean(description='Redis 連線狀態')
})

# Redis 連線設定 - 優化版本
redis_host = os.getenv('REDIS_HOST', 'localhost')
redis_port = int(os.getenv('REDIS_PORT', 6379))
redis_password = os.getenv('REDIS_PASSWORD', None)

# 創建 Redis 連接池以提高併發性能
redis_pool = redis.ConnectionPool(
    host=redis_host,
    port=redis_port,
    password=redis_password,
    decode_responses=True,
    max_connections=100,  # 最大連接數
    retry_on_timeout=True,
    socket_connect_timeout=5,
    socket_timeout=5
)

try:
    r = redis.Redis(connection_pool=redis_pool)
    # 測試連線
    r.ping()
    print(f"✅ 成功連接到 Redis: {redis_host}:{redis_port}")
except redis.ConnectionError:
    print(f"❌ 無法連接到 Redis: {redis_host}:{redis_port}")
    r = None

# 本地緩存以減少 Redis 查詢
local_cache = {}
cache_lock = threading.RLock()
CACHE_TTL = 30  # 緩存 30 秒

def cache_decorator(ttl=CACHE_TTL):
    """簡單的本地緩存裝飾器"""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            # 生成緩存鍵
            cache_key = f"{func.__name__}:{str(args)}:{str(sorted(kwargs.items()))}"
            current_time = time.time()
            
            with cache_lock:
                if cache_key in local_cache:
                    cached_data, cached_time = local_cache[cache_key]
                    if current_time - cached_time < ttl:
                        return cached_data
                
                # 執行函數並緩存結果
                result = func(*args, **kwargs)
                local_cache[cache_key] = (result, current_time)
                
                # 清理過期緩存
                expired_keys = [k for k, (_, t) in local_cache.items() 
                              if current_time - t > ttl]
                for k in expired_keys:
                    del local_cache[k]
                
                return result
        return wrapper
    return decorator

def batch_get_records(pattern='*'):
    """批量獲取記錄並緩存結果"""
    if r is None:
        return []
    
    keys = r.keys('record:*')
    if not keys:
        return []
    
    # 使用 pipeline 批量獲取數據以提高性能
    pipe = r.pipeline()
    for key in keys:
        pipe.get(key)
    
    values = pipe.execute()
    
    records = []
    for i, value in enumerate(values):
        if value:
            try:
                record = json.loads(value)
                if isinstance(record, dict) and 'key' in record and 'type' in record and 'value' in record:
                    # 確保記錄完整性
                    if 'updated_at' not in record:
                        record['updated_at'] = 'N/A'
                    if 'id' not in record:
                        record['id'] = keys[i].replace('record:', '')
                    
                    # 根據 pattern 篩選 key
                    if pattern == '*' or fnmatch.fnmatch(record['key'], pattern):
                        records.append(record)
            except (json.JSONDecodeError, Exception):
                continue
    
    return records

@ns_health.route('/')
class HealthCheck(Resource):
    @ns_health.doc('health_check')
    @ns_health.marshal_with(health_model)
    def get(self):
        """健康檢查端點"""
        try:
            redis_status = r is not None and r.ping()
        except:
            redis_status = False
            
        return {
            'status': 'ok',
            'message': 'Flask API 運行中',
            'redis_connected': redis_status
        }

@ns_records.route('/set')
class SetRecord(Resource):
    @ns_records.doc('set_record')
    @ns_records.expect(record_input_model)
    @ns_records.marshal_with(success_response_model, code=200)
    @ns_records.response(400, 'Bad Request', error_model)
    @ns_records.response(500, 'Internal Server Error', error_model)
    def post(self):
        """建立新記錄或更新現有記錄"""
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
            
            # 取得當前時間
            current_time = datetime.now().isoformat()
            
            # 使用 Redis SCAN 來查找現有記錄，避免一次性載入所有鍵
            cursor = 0
            existing_record_id = None
            
            while True:
                cursor, keys = r.scan(cursor=cursor, match='record:*', count=100)
                
                if keys:
                    # 使用 pipeline 批量獲取數據
                    pipe = r.pipeline()
                    for redis_key in keys:
                        pipe.get(redis_key)
                    values = pipe.execute()
                    
                    for i, stored_data in enumerate(values):
                        if stored_data:
                            try:
                                record = json.loads(stored_data)
                                if (record.get('key') == key and 
                                    record.get('type') == type_value):
                                    existing_record_id = keys[i]
                                    break
                            except (json.JSONDecodeError, Exception):
                                continue
                    
                    if existing_record_id:
                        break
                
                if cursor == 0:
                    break
            
            # 清理緩存
            with cache_lock:
                local_cache.clear()
            
            if existing_record_id:
                # 更新現有記錄的 value
                stored_data = r.get(existing_record_id)
                record = json.loads(stored_data)
                record['value'] = value
                record['updated_at'] = current_time
                
                r.set(existing_record_id, json.dumps(record, ensure_ascii=False))
                
                return {
                    'message': f'成功更新記錄 (key: {key}, type: {type_value}, value: {value})',
                    'id': existing_record_id.replace('record:', ''),
                    'key': key,
                    'type': type_value,
                    'value': value,
                    'updated_at': current_time
                }
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
                
                r.set(redis_key, json.dumps(record, ensure_ascii=False))
                
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
    @cache_decorator(ttl=15)  # 緩存 15 秒
    def get(self, key):
        """取得指定 key 的所有記錄"""
        try:
            if r is None:
                api.abort(500, 'Redis 連線失敗')
            
            records = batch_get_records()
            matching_records = [record for record in records if record.get('key') == key]
            
            if not matching_records:
                api.abort(404, f'找不到 key: {key}')
            
            return {
                'records': matching_records,
                'count': len(matching_records)
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
        """刪除指定 key 的所有記錄"""
        try:
            if r is None:
                api.abort(500, 'Redis 連線失敗')
            
            deleted_count = 0
            cursor = 0
            
            while True:
                cursor, keys = r.scan(cursor=cursor, match='record:*', count=100)
                
                if keys:
                    # 使用 pipeline 批量獲取數據
                    pipe = r.pipeline()
                    for redis_key in keys:
                        pipe.get(redis_key)
                    values = pipe.execute()
                    
                    keys_to_delete = []
                    for i, value in enumerate(values):
                        if value:
                            try:
                                record = json.loads(value)
                                if (isinstance(record, dict) and 
                                    'key' in record and 
                                    record['key'] == key):
                                    keys_to_delete.append(keys[i])
                            except (json.JSONDecodeError, Exception):
                                continue
                    
                    # 批量刪除
                    if keys_to_delete:
                        r.delete(*keys_to_delete)
                        deleted_count += len(keys_to_delete)
                
                if cursor == 0:
                    break
            
            # 清理緩存
            with cache_lock:
                local_cache.clear()
            
            if deleted_count == 0:
                api.abort(404, f'找不到 key: {key}')
            
            return {'message': f'成功刪除 {deleted_count} 筆記錄 (key: {key})'}
        
        except Exception as e:
            api.abort(500, str(e))

@ns_records.route('/delete/id/<string:record_id>')
class DeleteRecordById(Resource):
    @ns_records.doc('delete_record_by_id')
    @ns_records.response(200, 'Success')
    @ns_records.response(404, 'Not Found', error_model)
    @ns_records.response(500, 'Internal Server Error', error_model)
    def delete(self, record_id):
        """根據內部 ID 刪除指定記錄"""
        try:
            if r is None:
                api.abort(500, 'Redis 連線失敗')
            
            redis_key = f'record:{record_id}'
            result = r.delete(redis_key)
            
            # 清理緩存
            with cache_lock:
                local_cache.clear()
            
            if result == 0:
                api.abort(404, f'找不到 ID: {record_id}')
            
            return {'message': f'成功刪除記錄 ID: {record_id}'}
        
        except Exception as e:
            api.abort(500, str(e))

@ns_query.route('/keys')
class ListAllRecords(Resource):
    @ns_query.doc('list_all_records')
    @ns_query.marshal_with(records_list_model, code=200)
    @ns_query.response(500, 'Internal Server Error', error_model)
    @ns_query.param('pattern', '搜尋模式 (預設: *)', type=str, default='*')
    @cache_decorator(ttl=10)  # 緩存 10 秒
    def get(self):
        """列出所有記錄"""
        try:
            if r is None:
                api.abort(500, 'Redis 連線失敗')
            
            pattern = request.args.get('pattern', '*')
            records = batch_get_records(pattern)
            
            return {'records': records, 'count': len(records)}
        
        except Exception as e:
            api.abort(500, str(e))

@ns_query.route('/keys/type/<string:type_filter>')
class GetRecordsByType(Resource):
    @ns_query.doc('get_records_by_type')
    @ns_query.marshal_with(type_filtered_model, code=200)
    @ns_query.response(500, 'Internal Server Error', error_model)
    @ns_query.param('pattern', '搜尋模式 (預設: *)', type=str, default='*')
    @cache_decorator(ttl=15)  # 緩存 15 秒
    def get(self, type_filter):
        """根據 type 篩選記錄"""
        try:
            if r is None:
                api.abort(500, 'Redis 連線失敗')
            
            pattern = request.args.get('pattern', '*')
            records = batch_get_records(pattern)
            
            # 篩選符合 type 的記錄
            filtered_records = [record for record in records 
                              if record.get('type') == type_filter]
            
            return {
                'type': type_filter,
                'records': filtered_records, 
                'count': len(filtered_records)
            }
        
        except Exception as e:
            api.abort(500, str(e))

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
            
            # 使用 SCAN 和 batch 刪除以提高性能
            deleted_count = 0
            cursor = 0
            
            while True:
                cursor, keys = r.scan(cursor=cursor, match='record:*', count=1000)
                
                if keys:
                    r.delete(*keys)
                    deleted_count += len(keys)
                
                if cursor == 0:
                    break
            
            # 清理緩存
            with cache_lock:
                local_cache.clear()
            
            if deleted_count > 0:
                return {'message': f'成功清空 {deleted_count} 筆記錄'}
            else:
                return {'message': '沒有記錄需要清空'}
        
        except Exception as e:
            api.abort(500, str(e))

# 為了向後兼容，保留一些根路由
@app.route('/', methods=['GET'])
def root_health_check():
    """根路由健康檢查（向後兼容）"""
    try:
        redis_status = r is not None and r.ping()
    except:
        redis_status = False
        
    return jsonify({
        'status': 'ok',
        'message': 'Flask API 運行中',
        'redis_connected': redis_status,
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

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True, threaded=True)
