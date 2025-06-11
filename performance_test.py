#!/usr/bin/env python3
"""
性能測試腳本
用於測試 Key-Value-Type Server 的響應速度和併發處理能力
"""
import requests
import time
import threading
import json
import random
import statistics
from concurrent.futures import ThreadPoolExecutor, as_completed

# 設定
BASE_URL = "http://localhost:35001"
API_BASE = f"{BASE_URL}/api/v1"

class PerformanceTest:
    def __init__(self, base_url=BASE_URL):
        self.base_url = base_url
        self.api_base = f"{base_url}/api/v1"
        self.results = []
        
    def test_health_check(self):
        """測試健康檢查端點"""
        print("🔍 測試健康檢查...")
        start_time = time.time()
        response = requests.get(f"{self.base_url}/")
        end_time = time.time()
        
        if response.status_code == 200:
            print(f"✅ 健康檢查成功 - 響應時間: {(end_time - start_time)*1000:.2f}ms")
            return True
        else:
            print(f"❌ 健康檢查失敗 - 狀態碼: {response.status_code}")
            return False
    
    def single_request_test(self, endpoint, method="GET", data=None):
        """單一請求測試"""
        start_time = time.time()
        try:
            if method == "POST":
                response = requests.post(endpoint, json=data)
            elif method == "DELETE":
                response = requests.delete(endpoint)
            else:
                response = requests.get(endpoint)
            
            end_time = time.time()
            duration = (end_time - start_time) * 1000
            
            return {
                'status_code': response.status_code,
                'duration_ms': duration,
                'success': 200 <= response.status_code < 300
            }
        except Exception as e:
            end_time = time.time()
            return {
                'status_code': None,
                'duration_ms': (end_time - start_time) * 1000,
                'success': False,
                'error': str(e)
            }
    
    def concurrent_test(self, test_func, num_threads=10, num_requests_per_thread=10):
        """併發測試"""
        print(f"🚀 開始併發測試 - {num_threads} 個線程，每個線程 {num_requests_per_thread} 個請求")
        
        results = []
        
        with ThreadPoolExecutor(max_workers=num_threads) as executor:
            futures = []
            
            for thread_id in range(num_threads):
                for req_id in range(num_requests_per_thread):
                    future = executor.submit(test_func, thread_id, req_id)
                    futures.append(future)
            
            for future in as_completed(futures):
                try:
                    result = future.result()
                    results.append(result)
                except Exception as e:
                    results.append({
                        'success': False,
                        'error': str(e),
                        'duration_ms': 0
                    })
        
        return results
    
    def set_record_test(self, thread_id, req_id):
        """設定記錄測試"""
        data = {
            'key': f'test_key_{thread_id}_{req_id}',
            'type': f'test_type_{thread_id % 5}',  # 5 種不同類型
            'value': random.uniform(10.0, 100.0)
        }
        return self.single_request_test(f"{self.api_base}/records/set", "POST", data)
    
    def get_record_test(self, thread_id, req_id):
        """獲取記錄測試"""
        key = f'test_key_{thread_id}_{req_id}'
        return self.single_request_test(f"{self.api_base}/records/get/{key}")
    
    def list_records_test(self, thread_id, req_id):
        """列出記錄測試"""
        return self.single_request_test(f"{self.api_base}/query/keys")
    
    def filter_by_type_test(self, thread_id, req_id):
        """按類型篩選測試"""
        type_filter = f'test_type_{thread_id % 5}'
        return self.single_request_test(f"{self.api_base}/query/keys/type/{type_filter}")
    
    def mixed_load_test(self, thread_id, req_id):
        """混合負載測試 - 隨機執行不同操作"""
        operation = random.choice(['set', 'get', 'list', 'filter'])
        
        if operation == 'set':
            return self.set_record_test(thread_id, req_id)
        elif operation == 'get':
            # 隨機獲取已存在的記錄
            key = f'test_key_{random.randint(0, 19)}_{random.randint(0, 49)}'
            return self.single_request_test(f"{self.api_base}/records/get/{key}")
        elif operation == 'list':
            return self.list_records_test(thread_id, req_id)
        else:  # filter
            return self.filter_by_type_test(thread_id, req_id)
    
    def analyze_results(self, results, test_name):
        """分析測試結果"""
        if not results:
            print(f"❌ {test_name} - 沒有結果")
            return
        
        successful_results = [r for r in results if r.get('success', False)]
        failed_results = [r for r in results if not r.get('success', False)]
        
        if successful_results:
            durations = [r['duration_ms'] for r in successful_results]
            
            print(f"\n📊 {test_name} 結果分析:")
            print(f"   總請求數: {len(results)}")
            print(f"   成功請求: {len(successful_results)}")
            print(f"   失敗請求: {len(failed_results)}")
            print(f"   成功率: {len(successful_results)/len(results)*100:.2f}%")
            print(f"   平均響應時間: {statistics.mean(durations):.2f}ms")
            print(f"   中位數響應時間: {statistics.median(durations):.2f}ms")
            print(f"   最快響應時間: {min(durations):.2f}ms")
            print(f"   最慢響應時間: {max(durations):.2f}ms")
            
            if len(durations) > 1:
                print(f"   響應時間標準差: {statistics.stdev(durations):.2f}ms")
            
            # 計算不同百分位數
            sorted_durations = sorted(durations)
            p95 = sorted_durations[int(len(sorted_durations) * 0.95)]
            p99 = sorted_durations[int(len(sorted_durations) * 0.99)]
            print(f"   95% 百分位數: {p95:.2f}ms")
            print(f"   99% 百分位數: {p99:.2f}ms")
        
        if failed_results:
            print(f"   失敗原因:")
            error_counts = {}
            for result in failed_results:
                error = result.get('error', f"HTTP {result.get('status_code', 'Unknown')}")
                error_counts[error] = error_counts.get(error, 0) + 1
            
            for error, count in error_counts.items():
                print(f"     - {error}: {count} 次")
    
    def stress_test_progressive(self):
        """漸進式壓力測試"""
        print("\n🔥 開始漸進式壓力測試")
        print("="*60)
        
        # 測試配置：逐步增加負載
        test_configs = [
            (5, 20, "輕負載"),
            (10, 50, "中負載"),
            (25, 100, "重負載"),
            (50, 200, "極重負載"),
            (100, 500, "超重負載")
        ]
        
        for threads, requests_per_thread, description in test_configs:
            print(f"\n📈 {description} - {threads} 線程 × {requests_per_thread} 請求")
            print("-" * 40)
            
            results = self.concurrent_test(self.set_record_test, threads, requests_per_thread)
            self.analyze_results(results, f"設定記錄 - {description}")
            
            # 短暫休息讓系統恢復
            time.sleep(2)
    
    def long_duration_test(self, duration_minutes=5):
        """長時間穩定性測試"""
        print(f"\n⏰ 開始 {duration_minutes} 分鐘長時間穩定性測試")
        print("="*60)
        
        start_time = time.time()
        end_time = start_time + (duration_minutes * 60)
        results = []
        request_count = 0
        
        with ThreadPoolExecutor(max_workers=20) as executor:
            while time.time() < end_time:
                # 提交一批請求
                futures = []
                for i in range(20):  # 每批20個請求
                    future = executor.submit(self.mixed_load_test, 0, request_count + i)
                    futures.append(future)
                
                # 收集結果
                for future in as_completed(futures):
                    try:
                        result = future.result()
                        results.append(result)
                        request_count += 1
                    except Exception as e:
                        results.append({
                            'success': False,
                            'error': str(e),
                            'duration_ms': 0
                        })
                        request_count += 1
                
                # 每100個請求報告一次進度
                if request_count % 100 == 0:
                    elapsed = time.time() - start_time
                    print(f"   進度: {request_count} 請求完成，已運行 {elapsed/60:.1f} 分鐘")
                
                # 短暫休息避免過度負載
                time.sleep(0.1)
        
        total_duration = time.time() - start_time
        print(f"\n📊 長時間測試結果 (運行 {total_duration/60:.1f} 分鐘):")
        print(f"   總請求數: {request_count}")
        print(f"   平均 QPS: {request_count/total_duration:.2f}")
        
        self.analyze_results(results, "長時間穩定性測試")

    def run_comprehensive_test(self):
        """執行綜合性能測試"""
        print("🎯 開始綜合性能測試")
        print("="*50)
        
        # 1. 健康檢查
        if not self.test_health_check():
            print("❌ 服務不可用，終止測試")
            return
        
        # 2. 清空現有資料
        print("\n🧹 清空現有資料...")
        try:
            response = requests.post(f"{self.api_base}/records/flush")
            if response.status_code == 200:
                print("✅ 資料清空成功")
            else:
                print(f"⚠️ 資料清空可能失敗 - 狀態碼: {response.status_code}")
        except Exception as e:
            print(f"⚠️ 清空資料時出錯: {e}")
        
        # 3. 大量設定記錄的併發測試
        print("\n" + "="*50)
        results = self.concurrent_test(self.set_record_test, num_threads=50, num_requests_per_thread=200)
        self.analyze_results(results, "大量設定記錄併發測試 (50 threads × 200 requests)")
        
        # 4. 等待讓資料穩定
        time.sleep(3)
        
        # 5. 大量獲取記錄的併發測試
        print("\n" + "="*50)
        results = self.concurrent_test(self.get_record_test, num_threads=40, num_requests_per_thread=150)
        self.analyze_results(results, "大量獲取記錄併發測試 (40 threads × 150 requests)")
        
        # 6. 大量列出所有記錄的併發測試
        print("\n" + "="*50)
        results = self.concurrent_test(self.list_records_test, num_threads=30, num_requests_per_thread=100)
        self.analyze_results(results, "大量列出記錄併發測試 (30 threads × 100 requests)")
        
        # 7. 大量按類型篩選的併發測試
        print("\n" + "="*50)
        results = self.concurrent_test(self.filter_by_type_test, num_threads=25, num_requests_per_thread=120)
        self.analyze_results(results, "大量按類型篩選併發測試 (25 threads × 120 requests)")
        
        # 8. 混合負載測試
        print("\n" + "="*50)
        results = self.concurrent_test(self.mixed_load_test, num_threads=35, num_requests_per_thread=80)
        self.analyze_results(results, "混合負載併發測試 (35 threads × 80 requests)")
        
        # 9. 漸進式壓力測試
        self.stress_test_progressive()
        
        # 10. 超高併發短時間測試
        print("\n" + "="*50)
        print("🚀 超高併發短時間測試")
        results = self.concurrent_test(self.set_record_test, num_threads=100, num_requests_per_thread=100)
        self.analyze_results(results, "超高併發測試 (100 threads × 100 requests)")
        
        # 11. 長時間穩定性測試 (可選，預設關閉)
        run_long_test = input("\n是否執行 5 分鐘長時間穩定性測試？(y/N): ").lower() == 'y'
        if run_long_test:
            self.long_duration_test(5)
        
        print("\n🎉 大量性能測試完成！")
        print("💡 建議檢查系統資源使用情況和錯誤日誌")

if __name__ == "__main__":
    test = PerformanceTest()
    test.run_comprehensive_test()
