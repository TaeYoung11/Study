# 배치 처리 워크플로우 구축(Airflow + Spark) 정리

**Airflow**와 **Spark**를 연동해 배치 처리 워크플로우를 구축하는 방법을 정리한 문서입니다. Spark의 데이터 구조, Airflow Connections & Hooks, SparkSubmitOperator를 이용한 Spark Job 실행, 그리고 DAG 실행 오류 확인 방법을 다룹니다.

---

## 1. 배치 처리와 실시간 처리

### 배치 처리(Batch Processing)란?
* 일정량의 데이터를 모아서 한꺼번에 처리하는 방식
* 정해진 시간(예: 매일 밤 12시) 또는 특정 이벤트(예: 파일 업로드) 발생 시 실행
* 대량 데이터를 처리하는데 적합하며, 주로 ETL 파이프라인, 데이터 웨어하우스 적재 등에 활용
* **주요 활용 사례:** 데이터 웨어하우스 적재(예: Redshift, Snowflake), 사용자 로그 분석(예: 하루 단위 사용자 접속 로그 집계), 기계 학습 모델 학습을 위한 데이터 준비

```
Source(여러 소스) → Batch Processor → Destination
                  (Delay: mins, hours, days)
```

### 실시간 처리(Real-time Processing)란?
* 데이터가 발생하는 즉시 실시간으로 처리하는 방식
* 지연 시간이 짧고, 스트리밍 데이터(예: 실시간 로그, IoT 센서 데이터)에 최적화
* 주로 실시간 모니터링, 이상 탐지, 실시간 추천 시스템 등에 활용

```
Source → (스트림) → Destination   (Almost-Real-time)
```

### 배치 처리 vs 실시간 처리
| 항목 | 배치 처리 | 실시간 처리 |
| --- | --- | --- |
| 데이터 처리 방식 | 일정량의 데이터를 모아서 한번에 처리 | 데이터가 들어오는 즉시 처리 |
| 처리 속도 | 느림(분~시간 단위) | 빠름(밀리초~초 단위) |
| 적용 사례 | 데이터 웨어하우스, 머신러닝 학습 데이터 준비 | 실시간 이상 탐지, 실시간 추천 시스템, IoT데이터 분석 |
| 리소스 사용 | 주어진 시간에만 리소스 사용 | 지속적인 리소스 사용 |
| 비용 | 상대적으로 저렴(일정한 리소스 사용) | 고비용 |
| 사례 | 매일 오후 20시 로그 데이터 집계 | 실시간 주식 거래 분석 |

---

## 2. Airflow와 Spark를 활용한 데이터 처리

### Apache Spark
* 대규모 데이터를 빠르고 효율적으로 처리하는 분산 데이터 처리 프레임워크
* **In-Memory Computing:** Spark는 데이터를 메모리(RAM)에서 처리하기 때문에 디스크 I/O가 많은 Hadoop보다 훨씬 빠름. 메모리에서 데이터를 유지한 채 연산을 수행하여 반복 연산(예: 머신러닝, 데이터 변환)이 Hadoop보다 최대 100배 빠름
* **다양한 데이터 처리 방식 지원:** Spark는 단순한 배치 처리가 아니라 여러 가지 방식으로 데이터를 처리할 수 있음 — RDD(기본적인 Spark 데이터 구조), DataFrame, Spark SQL
* **Scalability:** 수십~수천 대의 클러스터 노드에서 병렬 실행 가능. AWS, Azure, Google Cloud 환경에서도 손쉽게 확장 가능
* **배치 & 실시간 데이터 처리 모두 가능:** Spark는 기본적으로 배치 처리를 지원하지만, 스트리밍 처리도 가능

### Spark Data Structure
* **RDD(Resilient Distributed Dataset):** 분산된 데이터를 저장하고 처리하는 기본 단위(Hadoop의 HDFS와 유사). 변경 불가능(Immutable) → 안정적인 분산 처리를 지원. 여러 노드에서 병렬로 처리 가능
* **DataFrame(Pandas와 유사):** 구조화된 데이터(테이블 형태)를 처리하는 최적화된 데이터 구조. Spark SQL과 연동 가능 → SQL 쿼리를 사용하여 데이터 변환
* **Spark SQL:** SQL을 활용해 데이터를 쉽게 조회하고 변환 가능. 다양한 데이터 소스(HDFS, S3, JDBC, Hive, Cassandra 등)에서 데이터를 가져올 수 있음

### Apache Spark의 RDD(Resilient Distributed Dataset)
* RDD는 여러 Worker Node에 분산되어 저장되고 병렬 처리됨
* Driver Node는 RDD의 연산을 스케줄링하고 각 Worker Node에 작업 분배

```
                Driver Node
                    |
   ┌────────┬────────┼────────┬────────┐
Worker    Worker            Worker    Worker
Node      Node    (RDD)     Node      Node
```

### Apache Spark의 RDD 배치 처리 과정
1. 데이터를 `parallelize` 또는 외부 소스에서 불러와 RDD를 생성
2. Transformation 연산(`map`, `filter` 등)은 RDD를 새로 생성하지만 즉시 실행되지 않음(Lazy Evaluation)
3. 여러 Transformation이 체이닝되어 실행 계획(DAG)을 구성
4. Action 연산이 호출될 때 DAG가 실행되어 실제 데이터 처리가 수행

```
Data Source → parallelize → RDD → Transform → RDD → Transform → RDD → Action
```

### Apache Spark의 Dataframe
* Spark DataFrame은 Hive, CSV, JSON, RDBMS, XML, RDD, Cassandra 등 다양한 소스의 데이터를 통합하여 구조화된 형식으로 표현
* Spark SQL을 통해 생성된 DataFrame은 열(Column) 기반의 테이블 형태로 데이터를 처리하며, SQL 쿼리도 가능

```
Hive Data / CSV Data / Json Data / RDBMS Data / XML Data / RDDs / Cassandra Data
        └─────────────────┬─────────────────┘
                     Spark SQL
                          ↓
                     DataFrame
                 (Col1 | Col2 | …)
```

### Apache Spark의 DataFrame 배치 처리 과정
1. 다양한 데이터 소스에서 `read()` 또는 `load()`를 통해 DataFrame을 생성
2. 생성된 DataFrame에 `select()`, `filter()`, `groupBy()` 등의 Transformation 연산 적용
3. 여러 단계의 Transformation이 체이닝되며, 새로운 DataFrame이 연속적으로 생성
4. 마지막에 `show()`, `count()`, `write()`와 같은 Action이 호출되어 실제 실행이 이루어짐

### Apache Spark 배치 처리의 주요 활용 사례
* **데이터 웨어하우스 적재(ETL):** Spark를 활용해 데이터를 정제 & 변환 후 데이터 웨어하우스에 저장 (예: 매일 수집한 CSV 데이터를 Parquet 변환 후 Snowflake, BigQuery 적재)
* **로그 데이터 분석:** 웹사이트, 애플리케이션, 서버 로그 데이터를 분석하여 사용자 행동 분석 (예: 사용자의 클릭 로그를 분석하여 마케팅 전략 최적화)
* **머신러닝 데이터 전처리:** 대량의 데이터를 Spark에서 전처리하여 머신러닝 모델 학습에 사용 (예: 추천 시스템을 위한 사용자 행동 데이터 전처리)

---

## 3. 배치 워크플로우에서 DAG의 역할

### 배치 워크플로우에서 DAG의 중요성
* **자동화:** 사람이 직접 실행할 필요 없이 정해진 스케줄에 따라 실행
* **유지보수 용이:** 실행 로드 및 실패 이력을 기록하여 문제 해결 가능
* **확장성:** 여러 작업을 DAG 내에서 관리하고, 병렬 실행 가능
* **재시도 및 오류 감지:** Task 실패 시 자동으로 재시도하여 안정적인 운영 가능

### 배치 워크플로우에서 DAG 처리 흐름 예시
* 병렬 태스크와 순차 태스크가 함께 구성된 DAG로, 데이터 로드부터 후속 처리까지 단계별로 실행 흐름을 정의할 수 있음
* 작업 간 의존성을 명확히 설정하고, 병렬 처리를 통해 전체 실행 시간을 단축할 수 있음

```
                Task_A1 → Task_A2
              ↗                    ↘
start → DAG 시작점(EmptyOperator)      end(모든 태스크 완료 후 종료 처리)
(DummyOperator) → Task_B1 → Task_B2  ↗
              ↘                    ↗
                Task_C1 → Task_C2
```
* 병렬 실행 가능한 작업들(예: 데이터 로드, 사전 처리) → 상위 태스크 완료 후 실행되는 후속 작업들

### DAG에서 CSV 파일 처리 작업 실행 (CSV 파일 읽기 & 변환)
* PythonOperator를 사용하여, CSV 파일 읽기 & 변환

```python
import pandas as pd

def process_csv():
    df = pd.read_csv("/opt/airflow/dags/input_data.csv")
    df.columns = [col.lower() for col in df.columns]  # 컬럼명 변경
    df["process_date"] = pd.Timestamp.now()             # 컬럼 추가
    df.to_csv("/opt/airflow/dags/output_data.csv", index=False)

process_task = PythonOperator(
    task_id="process_csv_task",
    python_callable=process_csv,
)
```

### DAG에서 BashOperator를 활용한 데이터 전처리 스크립트 실행
* Shell로 작성된 데이터 전처리 스크립트를 BashOperator로 실행

```python
preprocess_task = BashOperator(
    task_id="run_preprocessing",
    bash_command="bash /opt/airflow/dags/scripts/preprocess.sh",
)
```

### 병렬 실행 및 성능 최적화 방법
* 서로 의존성이 없는 Task는 리스트(`[task_1, task_2]`)로 묶어 병렬 실행되도록 구성해 전체 워크플로우 실행 시간을 단축
* Graph View에서 병렬 실행 구조를 시각적으로 확인 가능

---

## 4. Airflow Connections 및 Hooks

### Airflow Connection & Hook
* **Connection:** Airflow가 외부 시스템(DB, API, 클라우드 서비스 등)과 연결할 수 있도록 설정. Web UI 또는 환경 변수를 통해 관리 가능
* **Hook:** Connection을 사용하여 실제 데이터를 전송하거나, 외부 시스템과 상호작용하는 역할. Operator에서 Hook을 활용하여 작업 수행

| 연결 대상 | Connection Type | 사용 Hook |
| --- | --- | --- |
| MySQL | MySQL | MySqlHook |
| PostgreSQL | Postgres | PostgresHook |
| REST API | HTTP | HttpHook |
| AWS S3 | AWS | S3Hook |

### Web UI connection 설정
* Airflow Webserver 접속 → [Admin] → [Connections] → `[+]` 클릭하여 새로운 Connection 생성
* Connection Id, Connection Type, Host, Port, Schema, Login 등 정보 입력 후 저장

### PostgreSQL Hook 사용 예시
```python
from airflow.hooks.postgres_hook import PostgresHook

def fetch_postgres_data():
    hook = PostgresHook(postgres_conn_id="my_postgres_conn")
    conn = hook.get_conn()
    with conn.cursor() as cur:
        cur.execute("SELECT 1;")
        one = cur.fetchone()[0]
        cur.execute("SELECT version();")
        version = cur.fetchone()[0]
    if one == 1:
        print("Postgres 연결 됨")
        print("Postgres version:", version)
    else:
        raise Exception("Postgres 연결 실패")

with DAG(
    dag_id="postgres_hook_python_operator",
    start_date=pendulum.datetime(2025, 8, 18, tz="Asia/Seoul"),
    schedule_interval="@daily",
    catchup=False,
    tags=["postgres", "hook"],
) as dag:
    fetch_data_task = PythonOperator(
        task_id="fetch_postgres_data",
        python_callable=fetch_postgres_data,
    )
```

### MySQL Hook 사용 예시
```python
from airflow.hooks.mysql_hook import MySqlHook

mysql_hook = MySqlHook(mysql_conn_id="my_mysql_conn")
df = mysql_hook.get_pandas_df(sql="SELECT * FROM users")
print(df)
```

---

## 5. DAG에서 SparkSubmitOperator 활용법

### SparkSubmitOperator 개요 및 역할
* Apache Airflow에서 Spark 애플리케이션을 실행하기 위한 전용 연산자
* Spark 클러스터(YARN, Kubernetes 등)에 `.py`, `.jar`, `.scala` 파일 등을 제출(submit)
* 복잡한 Spark 작업을 Airflow DAG 내에 통합하여 자동화된 데이터 파이프라인 구성 가능
* DAG 태스크로서 Spark 작업 실행을 스케줄링 및 추적 가능
* `spark-submit` 명령어를 Python 코드로 대체하여 운영 효율성 확보

### SparkSubmitOperator 주요 파라미터
| 파라미터 | 설명 |
| --- | --- |
| application | 실행할 Spark 애플리케이션 경로(.py, .jar 등) |
| conf | Spark 설정(예: `"spark.executor.memory": "2g"`) |
| executor_memory, driver_memory | 리소스 지정 |
| application_args | 애플리케이션에 전달할 인자 목록 |
| conn_id | Spark 클러스터 연결 정보(예: `spark_default`) |
| name | Spark 작업 이름 |

### docker-compose.yaml에 Spark 서비스 추가하기
```yaml
spark-master:
  build:
    context: .
    dockerfile: Dockerfile.spark
  container_name: spark-master
  environment:
    - SPARK_MODE=master
    - SPARK_MASTER_HOST=spark-master
    - SPARK_RPC_AUTHENTICATION_ENABLED=no
    - SPARK_RPC_ENCRYPTION_ENABLED=no
    - SPARK_LOCAL_STORAGE_ENCRYPTION_ENABLED=no
    - SPARK_SSL_ENABLED=no
  ports:
    - "8081:8080"
    - "7077:7077"

spark-worker:
  build:
    context: .
    dockerfile: Dockerfile.spark
  container_name: spark-worker
  environment:
    - SPARK_MODE=worker
    - SPARK_MASTER_URL=spark://spark-master:7077
    ...
```

### JAVA_HOME 세팅 및 경로 설정
```yaml
environment:
  AIRFLOW_CORE_EXECUTOR: CeleryExecutor
  AIRFLOW_DATABASE_SQL_ALCHEMY_CONN: postgresql+psycopg2://airflow:airflow@postgres/airflow
  AIRFLOW_CELERY_RESULT_BACKEND: db+postgresql://airflow:airflow@postgres/airflow
  AIRFLOW_CELERY_BROKER_URL: redis://:@redis:6379/0
  AIRFLOW_CORE_FERNET_KEY: ''
  AIRFLOW_CORE_DAGS_ARE_PAUSED_AT_CREATION: 'true'
  AIRFLOW_CORE_LOAD_EXAMPLES: 'false'
  AIRFLOW_API_AUTH_BACKENDS: 'airflow.api.auth.backend.basic_auth,airflow.api.auth.backend.session'
  AIRFLOW_CORE_DEFAULT_TIMEZONE: Asia/Seoul
  JAVA_HOME: /usr/lib/jvm/java-17-openjdk-amd64
```
* Spark 작업을 실행하려면 Airflow 컨테이너 안에도 Java(JDK)가 설치되고 `JAVA_HOME`이 지정되어 있어야 함

### Docker 네트워크 설정
* Airflow 컨테이너와 Spark 컨테이너가 같은 Docker 네트워크에 속해야 `spark://spark-master:7077` 주소로 서로 통신 가능

### DAG/scripts 경로 & volume 마운트 설정
```yaml
volumes:
  - ${AIRFLOW_PROJ_DIR:-.}/airflow/dags:/opt/airflow/dags
  - ${AIRFLOW_PROJ_DIR:-.}/logs:/opt/airflow/logs
  - ${AIRFLOW_PROJ_DIR:-.}/config:/opt/airflow/config
  - ${AIRFLOW_PROJ_DIR:-.}/plugins:/opt/airflow/plugins
  - ${AIRFLOW_PROJ_DIR:-.}/airflow/dags/scripts:/opt/airflow/dags/scripts  # scripts 폴더 추가
```

### Airflow UI에서 Spark Connection 추가
* [Admin] → [Connections] → `[+]` 클릭
* Connection Id: `spark_default`, Connection Type: `Spark`, Host: `spark://spark-master`, Port: `7077`, Deploy mode: `client`, Spark binary: `spark-submit`

### SparkSubmitOperator를 통한 Spark Job 실행
```python
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime

with DAG(
    dag_id="spark_submit_example",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["spark"],
) as dag:

    submit_job = SparkSubmitOperator(
        task_id="spark_submit_task",
        application="/opt/airflow/dags/scripts/spark_wordcount.py",
        conn_id="spark_default",
        conf={"spark.master": "spark://spark-master:7077"},
        verbose=True,
    )
```

**spark_wordcount.py 예시**
```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg

spark = SparkSession.builder.appName("DataFrameTest").getOrCreate()

data = [("Alice", "Math", 90), ("Bob", "Math", 80), ("Alice", "English", 85)]
columns = ["name", "subject", "score"]

df = spark.createDataFrame(data, columns)
avg_scores = df.groupBy("name").agg(avg("score").alias("average_score"))
avg_scores.show()

spark.stop()
```
* 실행 후 Airflow Web UI의 Graph View에서 `spark_submit_task`가 성공(success) 상태로 표시되며, Spark UI(`http://<spark-master>:8081`)에서도 제출된 Application(예: `DataFrameTest`)의 Executor Summary, 실행 상태(FINISHED) 등을 확인할 수 있음

---

## 6. DAG 실행 오류 확인 (Airflow Web UI 로그)

* DAG 실행 중, 에러 발생할 경우 각 Task에 관한 상세 로그 확인 가능
* DAG 실행 로그는 Airflow 환경에 저장됨 — Logs 폴더에 실행했던 DAG 로그가 DAG 별로 저장되어 있음
* 해당 DAG의 실패 Task에 대해서 로그를 확인하여로 실패 지점(트레이스백/에러 메시지)을 특정할 수 있음
* DAG 이름, Task 이름으로 로그를 찾을 수 있음
* 로그 마지막의 트레이스백(Traceback) 정보를 통해 어떤 예외(Exception)가 발생했는지 확인 가능(예: 파일 경로 오류, 패키지 임포트 오류 등)

---

## 핵심 요약
* **배치 처리**는 일정량의 데이터를 모아 정해진 주기로 처리(느림, 저비용)하고, **실시간 처리**는 데이터 발생 즉시 처리(빠름, 고비용)한다는 점에서 대비된다.
* **Apache Spark**는 In-Memory Computing 기반의 분산 처리 프레임워크로, RDD(불변 분산 데이터셋) → DataFrame(구조화된 테이블) → Spark SQL 순으로 추상화 수준이 높아지며, Transformation은 지연 실행(Lazy)되고 Action 호출 시 실제 연산이 수행된다.
* **Airflow DAG**는 배치 워크플로우의 자동화·의존성 관리·재시도를 담당하며, PythonOperator/BashOperator로 전처리를, 서로 독립적인 Task는 리스트로 묶어 병렬화해 실행 시간을 단축한다.
* **Connection & Hook**은 Airflow가 외부 시스템(DB, API, 클라우드 등)과 연결하고 실제 데이터를 주고받기 위한 메커니즘으로, MySqlHook/PostgresHook/HttpHook/S3Hook 등이 있다.
* **SparkSubmitOperator**는 Airflow DAG에서 Spark 애플리케이션을 제출(submit)하는 전용 연산자로, `application`/`conf`/`conn_id` 등을 지정해 실행하며, 실행을 위해서는 Airflow-Spark 컨테이너 간 Docker 네트워크 연결, `JAVA_HOME` 설정, Spark Connection 등록이 선행되어야 한다.
* DAG 실행 오류는 Airflow Web UI의 Task 로그(DAG명/Task명별로 저장)에서 트레이스백을 확인해 원인을 특정한다.
