import os
import sys
import psycopg2
import psycopg2.extras
import requests
import time
from datetime import datetime, timezone
from dotenv import load_dotenv

# ==========================================
# 0. 환경 변수 로드 (.env 파일 읽기)
# ==========================================
load_dotenv()  # 현재 디렉토리의 .env 파일을 찾아 로드합니다.

def get_required_env(var_name):
    """환경 변수를 가져오고, 없으면 에러 메시지 출력 후 종료합니다."""
    value = os.getenv(var_name)
    if not value:
        print(f"[오류] 환경 변수 '{var_name}'가 정의되지 않았습니다.")
        print(f"       동일한 경로에 .env 파일을 생성하고 {var_name}=값을 설정해 주세요.")
        sys.exit(1)
    return value

# 환경 변수에서 필수 인증 정보 추출
DB_USER = get_required_env("DB_USER")
DB_PASSWORD = get_required_env("DB_PASSWORD")
GRAFANA_USER_ENV = get_required_env("GRAFANA_USER")
GRAFANA_PASS_ENV = get_required_env("GRAFANA_PASS")

# ==========================================
# 1. 설정 (Configuration)
# ==========================================
DB_CONFIG = {
    "dbname": "seoul_transport",  # 타겟 데이터베이스
    "user": DB_USER,
    "password": DB_PASSWORD,  
    "host": os.getenv("DB_HOST", "localhost"),  # 없으면 기본값 'localhost'
    "port": os.getenv("DB_PORT", "5432")        # 없으면 기본값 '5432'
}

GRAFANA_URL = os.getenv("GRAFANA_URL", "http://localhost:3000")
GRAFANA_USER = GRAFANA_USER_ENV
GRAFANA_PASS = GRAFANA_PASS_ENV

DATASOURCE_NAME = "PostgreSQL_SeoulTransport"

# ==========================================
# 2. Grafana 자동화 함수
# ==========================================
def create_grafana_datasource():
    """Grafana에 PostgreSQL 데이터 소스를 등록하고 UID를 반환합니다."""
    url = f"{GRAFANA_URL}/api/datasources"
    headers = {'Content-Type': 'application/json'}
    
    payload = {
        "name": DATASOURCE_NAME,
        "type": "postgres",
        "url": f"{DB_CONFIG['host']}:{DB_CONFIG['port']}",
        "access": "proxy",
        "user": DB_CONFIG["user"],
        "database": DB_CONFIG["dbname"],
        "basicAuth": False,
        "isDefault": True,
        "secureJsonData": {
            "password": DB_CONFIG["password"]
        },
        "jsonData": {
            "sslmode": "disable",
            "postgresVersion": 16,
            "timescaledb": False
        }
    }

    # 기존 데이터 소스 존재 여부 확인
    check_res = requests.get(f"{url}/name/{DATASOURCE_NAME}", auth=(GRAFANA_USER, GRAFANA_PASS))
    if check_res.status_code == 200:
        uid = check_res.json()['uid']
        print(f"[Grafana] 데이터 소스 '{DATASOURCE_NAME}'가 이미 존재합니다. (UID: {uid})")
        return uid

    # 새로 생성
    response = requests.post(url, json=payload, auth=(GRAFANA_USER, GRAFANA_PASS), headers=headers)
    if response.status_code == 200:
        uid = response.json()['datasource']['uid']
        print(f"[Grafana] 데이터 소스 등록 성공 (UID: {uid})")
        return uid
    else:
        print(f"[Grafana] 데이터 소스 등록 실패: {response.text}")
        return None

def create_grafana_dashboard(datasource_uid):
    """UID를 매핑하여 대시보드 JSON을 생성하고 배포합니다."""
    if not datasource_uid:
        print("[Grafana] 데이터 소스 UID가 없어 대시보드 생성을 중단합니다.")
        return

    dashboard_json = {
        "dashboard": {
            "id": None,
            "title": "Seoul Transport Transaction Monitor",
            "tags": ["postgresql", "seoul_transport"],
            "timezone": "browser",
            "refresh": "5s",
            "schemaVersion": 16,
            "version": 0,
            "panels": [
                {
                    "title": "TPS (Transactions Per Second)",
                    "type": "timeseries",
                    "gridPos": {"h": 8, "w": 8, "x": 0, "y": 0},
                    "datasource": {"type": "postgres", "uid": datasource_uid},
                    # 수정됨: WHERE 구문 추가
                    "targets": [{"rawSql": "SELECT time AS \"time\", tps FROM monitor_metrics WHERE $__timeFilter(time) ORDER BY time ASC", "format": "time_series"}],
                },
                {
                    "title": "Avg Service Time (ms)",
                    "type": "timeseries",
                    "gridPos": {"h": 8, "w": 8, "x": 8, "y": 0},
                    "datasource": {"type": "postgres", "uid": datasource_uid},
                    # 수정됨: WHERE 구문 추가
                    "targets": [{"rawSql": "SELECT time AS \"time\", avg_service_time FROM monitor_metrics WHERE $__timeFilter(time) ORDER BY time ASC", "format": "time_series"}],
                },
                {
                    "title": "Active Connections",
                    "type": "timeseries",
                    "gridPos": {"h": 8, "w": 8, "x": 16, "y": 0},
                    "datasource": {"type": "postgres", "uid": datasource_uid},
                    "targets": [{"rawSql": "SELECT time AS \"time\", active_connections FROM monitor_metrics WHERE $__timeFilter(time) ORDER BY time ASC", "format": "time_series"}],
                },
                {
                    "title": "Top 10 Longest Transactions",
                    "type": "table",
                    "gridPos": {"h": 10, "w": 24, "x": 0, "y": 8},
                    "datasource": {"type": "postgres", "uid": datasource_uid},
                    "targets": [{"rawSql": "SELECT duration_ms as \"Duration (ms)\", completed_at as \"Completed At\", username as \"User\", table_name as \"Table Name\", query as \"Query\" FROM long_transactions ORDER BY duration_ms DESC LIMIT 10", "format": "table"}]
                }
            ]
        },
        "overwrite": True
    }

    headers = {'Content-Type': 'application/json'}
    response = requests.post(
        f"{GRAFANA_URL}/api/dashboards/db", 
        json=dashboard_json, 
        auth=(GRAFANA_USER, GRAFANA_PASS),
        headers=headers
    )
    
    if response.status_code == 200:
        print("[Grafana] 대시보드가 성공적으로 배포되었습니다.")
    else:
        print(f"[Grafana] 대시보드 배포 실패: {response.text}")

# ==========================================
# 3. PostgreSQL 메트릭 수집 함수
# ==========================================
def collect_metrics():
    """5초마다 PostgreSQL 통계를 수집하여 테이블에 저장합니다."""
    conn = psycopg2.connect(**DB_CONFIG)
    conn.autocommit = True
    cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    
    # 1. 익스텐션 활성화
    try:
        cursor.execute("CREATE EXTENSION IF NOT EXISTS pg_stat_statements;")
    except Exception as e:
        print(f"[DB] 익스텐션 확인 중 오류 (무시 가능): {e}")

    # 2. 기존 테이블 삭제 (스키마 충돌 및 과거 KST 데이터 찌꺼기 방지)
    #cursor.execute("DROP TABLE IF EXISTS monitor_metrics;")
    #cursor.execute("DROP TABLE IF EXISTS long_transactions;")
    cursor.execute("select pg_stat_statements_reset();")

    # 3. 새로운 스키마로 테이블 생성 (TIMESTAMPTZ 사용)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS monitor_metrics (
            time TIMESTAMPTZ,
            tps FLOAT,
            avg_service_time FLOAT,
            active_connections INT
        );
        CREATE TABLE IF NOT EXISTS long_transactions (
            time TIMESTAMPTZ,
            duration_ms FLOAT,
            completed_at TIMESTAMPTZ,
            username TEXT,
            table_name TEXT,
            query TEXT
        );
    """)
    print("[DB] 모니터링용 테이블 초기화 완료.")

    # 4. TPS 계산을 위한 초기값 세팅 (수정됨: float 형변환)
    cursor.execute("SELECT sum(xact_commit), sum(xact_rollback) FROM pg_stat_database WHERE datname = %s;", (DB_CONFIG['dbname'],))
    row = cursor.fetchone()
    if row and row[0] is not None:
        prev_commits, prev_rollbacks = float(row[0]), float(row[1])
    else:
        prev_commits, prev_rollbacks = 0.0, 0.0

    print(f"[{DB_CONFIG['dbname']}] 메트릭 수집을 시작합니다... (종료: Ctrl+C)")
    try:
        while True:
            time.sleep(5)
            
            # 명시적인 UTC 시간 생성 (수정됨: No data 현상 해결)
            now_utc = datetime.now(timezone.utc)
            
            # 1. TPS 계산 (수정됨: float 형변환)
            cursor.execute("SELECT sum(xact_commit), sum(xact_rollback) FROM pg_stat_database WHERE datname = %s;", (DB_CONFIG['dbname'],))
            row = cursor.fetchone()
            curr_commits = float(row[0] if row and row[0] is not None else 0.0)
            curr_rollbacks = float(row[1] if row and row[1] is not None else 0.0)
            
            tps = ((curr_commits - prev_commits) + (curr_rollbacks - prev_rollbacks)) / 5.0
            prev_commits, prev_rollbacks = curr_commits, curr_rollbacks
            
            # 2. Avg Service Time
            cursor.execute("SELECT sum(total_exec_time)/nullif(sum(calls), 0) FROM pg_stat_statements;")
            row = cursor.fetchone()
            avg_time = float(row[0]) if row and row[0] is not None else 0.0
            
            # 3. Active Connections
            cursor.execute("SELECT count(*) FROM pg_stat_activity WHERE state = 'active' AND datname = %s;", (DB_CONFIG['dbname'],))
            active_conn = int(cursor.fetchone()[0])
            
            print(tps, avg_time, active_conn)
            # 메트릭 인서트 (수정됨: UTC 시간 직접 주입)
            cursor.execute(
                "INSERT INTO monitor_metrics (time, tps, avg_service_time, active_connections) VALUES (%s, %s, %s, %s)",
                (now_utc, tps, avg_time, active_conn)
            )
            
            # 4. Longest Transactions (수정됨: UTC 시간 직접 주입)
            cursor.execute("""
                INSERT INTO long_transactions (time, duration_ms, completed_at, username, table_name, query)
                SELECT 
                    %s as time,
                    total_exec_time / calls as duration_ms,
                    CURRENT_TIMESTAMP as completed_at,
                    rolname as username,
                    'Mixed/Parse_Required' as table_name,
                    query
                FROM pg_stat_statements
                JOIN pg_roles ON pg_stat_statements.userid = pg_roles.oid
                ORDER BY (total_exec_time / calls) DESC
                LIMIT 10;
            """, (now_utc,))

    except KeyboardInterrupt:
        print("\n수집을 종료합니다.")
    except Exception as e:
        print(f"\n[오류 발생] 수집 중단: {e}")
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    # 1. 데이터 소스 생성
    ds_uid = create_grafana_datasource()
    
    # 2. 대시보드 생성
    create_grafana_dashboard(ds_uid)
    
    # 3. 데이터 수집 시작
    collect_metrics()
