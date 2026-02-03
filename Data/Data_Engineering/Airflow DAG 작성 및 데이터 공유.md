# Airflow DAG 작성 및 데이터 공유 정리

Airflow의 **Operator**(BashOperator, PythonOperator, BranchOperator 등)와 분기 처리, **Cron 스케줄링 & Task 연결**, 그리고 Task 간 데이터 공유 방법인 **XCom과 Variable**을 정리한 문서입니다.

---

## 1. Operator 기본

### 오퍼레이터(Operator)란?
* Airflow에서 Task를 실행하는 역할을 수행하는 객체
* DAG 내에서 개별 Task로 사용되며, 다양한 실행 방식을 제공
* Python 기반으로 확장 가능하며, 기본 제공(내장) 오퍼레이터와 사용자 정의 오퍼레이터가 있음

### 오퍼레이터 종류
| 오퍼레이터 종류 | 설명 | 예제 |
| --- | --- | --- |
| Action Operators(기본 실행 오퍼레이터) | 특정 동작을 수행하는 오퍼레이터 | PythonOperator, BashOperator, EmailOperator |
| Sensor Operators(센서 오퍼레이터) | 특정 이벤트를 감지할 때까지 대기 | FileSensor, HttpSensor, S3KeySensor |
| Transfer Operators(데이터 전송 오퍼레이터) | 한 위치에서 다른 위치로 데이터 이동 | S3ToGCSOperator, MySQLToGCSOperator |
| Database Operators(데이터베이스 관련 오퍼레이터) | DB에서 SQL을 실행하는 오퍼레이터 | PostgresOperator, MySqlOperator, SnowflakeOperator |
| Big Data & ML Operators(빅데이터 & 머신러닝 오퍼레이터) | Spark, Hive, Dataproc, ML 관련 오퍼레이터 | SparkSubmitOperator, DataflowOperator |
| Docker & Kubernetes Operators | 컨테이너 환경에서 실행 | DockerOperator, KubernetesPodOperator |
| Empty Operators | Task 흐름을 설정하는 데 사용 | DummyOperator(Empty Operator) |

### Action Operators
* Task는 특정 오퍼레이터를 기반으로 정의됨: **BashOperator**(Bash 명령어 실행) / **PythonOperator**(Python 함수 실행) / **EmailOperator**(이메일 전송)
* 각 오퍼레이터를 조합하여 다양한 워크플로우를 구성할 수 있음

**BashOperator — 컨테이너 외부의 쉘 스크립트 수행**
```bash
#!/bin/bash
FRUIT=$1
if [ $FRUIT == APPLE ]; then
    echo "You selected Apple!"
elif [ $FRUIT == ORANGE ]; then
    echo "You selected Orange!"
elif [ $FRUIT == GRAPE ]; then
    echo "You selected Grape!"
else
    echo "You selected other Fruit!"
fi
```
* 셸 스크립트를 만들고, `docker-compose.yaml`에서 `volumes`의 plugins 경로를 실제 파일 경로로 수정하여 컨테이너 외부 스크립트를 실행할 수 있게 함

```python
task_t1 = BashOperator(
    task_id="bash_t1",
    bash_command="bash /opt/airflow/plugins/select_fruit.sh APPLE",
)
```

**PythonOperator — Python 함수를 실행하는 기능**
```python
def my_function():
    print("Hello, Airflow")

python_t1 = PythonOperator(
    task_id="python_t1",
    python_callable=my_function,
)
```

### EmptyOperator
* Task 실행 없이 DAG 구조를 설정하는데 사용됨
* DAG의 논리적 흐름을 구성하는 용도로 활용(예: 시작/종료 지점 표시)

### BranchOperator (분기 처리)
* Airflow에서 DAG 실행 흐름을 조건에 따라 분기할 수 있도록 하는 오퍼레이터
* 특정 조건을 평가하여 어떤 Task를 실행할지 동적으로 결정
* 선택되지 않은 Task는 자동으로 **Skipped** 상태가 됨
* DAG 실행을 최적화하고 불필요한 작업을 줄이는데 유용
* **BranchPythonOperator:** Python 함수를 사용하여 실행 Task 결정
* **BranchDagRunOperator:** 다른 DAG 실행을 분기 처리하는 오퍼레이터

```python
def choose_branch(value):
    if value == "A":
        return "task_A"
    else:
        return "task_B"

branching = BranchPythonOperator(
    task_id="branching",
    python_callable=choose_branch,
    op_kwargs={"value": "A"},
)
```
* Value 값이 "A"이기 때문에 `task_A`가 실행되고, `task_B`는 정상 Skipped 처리됨

### API 응답값 기반으로 분기 처리 방식
* API 결과값이 15 이하이면 `task_cold` 실행, 그렇지 않으면 `task_hot` 실행 — 결과 값이 1~10이면 `task_cold` 정상 수행, `task_hot`은 skipped 처리

---

## 2. Airflow Decorators (배치 작업 최적화)

* Python 함수 데코레이터로 태스크를 간편하게 Airflow 태스크로 변환
* **`@task`:** `@task` 데코레이터를 사용하여 Python 함수가 자동으로 Airflow 태스크로 변환
* **데이터 전달:** 함수의 리턴값이 자동으로 XCom을 사용하여 데이터를 전달하므로 XCom을 수동으로 사용하지 않아도 됨
* **의존성 설정:** 함수 간의 의존성은 함수를 호출하는 방식으로 간단하게 설정 가능

**전통적인 방식 (PythonOperator + XCom 수동 처리)**
```python
def extract_data():
    print("Extracting data...")
    return "raw_data"

def transform_data(data):
    print(f"Transforming data: {data}")
    return "transformed_data"

def load_data(data):
    print(f"Loading data: {data}")
    return f"loaded_{data}"

with DAG(dag_id="example_dag", schedule_interval="@daily", start_date=datetime(2025, 1, 1)) as dag:
    extract_task = PythonOperator(task_id="extract_data", python_callable=extract_data)
    transform_task = PythonOperator(task_id="transform_data", python_callable=lambda **kwargs: transform_data(kwargs["ti"].xcom_pull(task_ids="extract_data")))
    load_task = PythonOperator(task_id="load_data", python_callable=lambda **kwargs: load_data(kwargs["ti"].xcom_pull(task_ids="transform_data")))

    extract_task >> transform_task >> load_task
```

**`@task` 데코레이터 방식 (간결)**
```python
@dag(schedule_interval="@daily", start_date=datetime(2025, 1, 1))
def example_dag():

    @task
    def extract_data():
        print("Extracting data...")
        return "raw_data"

    @task
    def transform_data(data):
        print(f"Transforming data: {data}")
        return f"transformed_{data}"

    @task
    def load_data(data):
        print(f"Loading data: {data}")
        return f"loaded_{data}"

    # DAG 실행
    raw = extract_data()
    transformed = transform_data(raw)
    load_data(transformed)

example_dag()
```

---

## 3. DAG Scheduling 및 Task 연결

### DAG 내 Task 간 의존성(Dependency)이란?
* Airflow에서는 DAG 내에서 Task 간 실행 순서(의존성)를 정의해야 함
* Task 간 의존성을 설정하면 특정 Task가 완료된 후 다음 Task가 실행됨
* 연산자 `>>` 또는 `<<`를 활용하여 Task 간 의존성을 설정 가능

### Task 연결 원리
* DAG 내에서 Task 의존성(Dependency)을 설정하여 실행 순서를 설정
* Task간의 실행 관계를 명확하게 지정해야 DAG가 올바르게 동작함
* Task 연결 방식은 순차 실행(Sequential Execution), 병렬 실행(Parallel Execution)으로 나뉨

| 종류 | 설명 |
| --- | --- |
| Upstream Task | 현재 Task 이전에 실행되는 Task |
| Downstream Task | 현재 Task 이후에 실행되는 Task |
| Linear Dependency | 순차적으로 Task를 실행(Task A → Task B → Task C) |
| Branching | 특정 조건에 따라 Task 실행 흐름을 분기 |
| Parallel Execution | 여러 Task를 병렬로 실행 |

**기본 연결(순차 실행)**
```python
# task_1이 완료된 후 task_2 실행
start >> task_1 >> task_2 >> end
```

**다중 Task 연결(병렬 실행)**
```python
# task_1 실행 후 task_2와 task_3 병렬 실행
start >> [task_1, task_2]  # task_1, task_2가 완료되어야 task_3 실행
[task_1, task_2] >> task_3
task_3 >> [task_4, task_5]  # task_3이 완료된 후 task_4, task_5 병렬 실행
[task_4, task_5] >> end
```

**다중 Task 종속 단계**
```python
# 시작 → 병렬 실행 → end
start >> [task_1, task_2, task_3] >> end
```

### Trigger Rule(트리거 규칙)
* Task가 실행되기 위한 조건을 설정하는 기능
* 기본적으로 모든 Upstream Task가 성공해야 실행됨(`all_success`)
* 특정 Task의 실행 결과에 따라 실행 조건을 다르게 설정할 수 있음

| Trigger Rule | 설명 |
| --- | --- |
| all_success (기본값) | 모든 Upstream Task가 성공(Success) 시 실행 |
| all_failed | 모든 Upstream Task가 실패(Fail) 시 실행 |
| all_done | 모든 Upstream Task가 성공, 실패, 스킵 여부와 관계없이 실행 |
| one_failed | 최소 1개의 Upstream Task가 실패하면 실행 |
| one_success | 최소 1개의 Upstream Task가 성공하면 실행 |
| none_failed | Upstream Task 중 실패가 없는 경우 실행(성공 또는 스킵) |
| none_failed_or_skipped | Upstream Task 중 실패와 스킵이 없는 경우 실행(모두 성공) |
| none_skipped | Upstream Task가 스킵되지 않았다면 실행 |

---

## 4. Cron 스케줄링 및 Task 관련 시간 개념

DAG 실행을 이해하려면 Airflow의 `start_date`, `logical_date`, `schedule_interval`, `data_interval` 등의 시간 개념을 정확히 이해해야 함

### DAG 스케줄
```
                schedule_interval
              ┌──────────────┐
2024.05.01  2024.05.02      Task     2024.05.03  2024.05.04
13:00:00    13:00:00                 13:00:00    13:00:00
start_date  data_interval_start   data_interval_end
              logical_date
```

### start_date
* DAG이 처리하기 시작할 데이터의 기준 시점
* 첫 번째 `data_interval`의 시작점을 정의함
* DAG은 `start_date` 직후에 실행되지 않고, 해당 구간이 끝난 후 처음 실행됨
* 예: `start_date = 2025-01-01` → 첫 번째 실행은 2025-01-02 00:00에 발생
* **주의:** `start_date`는 미래 시점으로 설정하지 말아야 하며, 미래 시점으로 설정하면 DAG이 예상대로 실행되지 않을 수 있음

### logical date
* 각 DAG run을 식별하는 논리적 날짜
* `data_interval_start`와 동일하며, 어떤 데이터를 처리하는 run인지 나타냄
* 실제 실행 시각과는 다름(예: `logical_date=1월1일` → 실행은 1월2일 00:00 이후)

**logical date 예시** — DAG가 매일 실행되는 경우(`schedule_interval = "@daily"`)
| Logical Date | DAG이 실행되는 실제 시간(run_at) |
| --- | --- |
| 2025-03-01 | 2025-03-02 00:00:00 |
| 2025-03-02 | 2025-03-03 00:00:00 |
| 2025-03-03 | 2025-03-04 00:00:00 |
* Logical Date가 2025-03-01이면, DAG는 2025-03-02에 실행됨
* 항상 Logical Date보다 늦게 실행되므로 데이터 정합성을 유지하는데 중요함 — 데이터가 모두 적재된 이후에 실행되므로 데이터 정합성을 보장

### schedule_interval(스케줄 간격)
* DAG이 실행되는 주기를 결정하는 설정값
* DAG이 얼마나 자주 실행될지를 정의하는 간격
* Cron 표현식 또는 예약어를 사용하여 주기를 정의
* DAG 실행 주기가 DAG의 Execution Date를 결정함

**Schedule_interval 설정 방법**
| 설정값 | 설명 |
| --- | --- |
| None | 수동 실행(자동 실행 없음) |
| @once | DAG을 한 번만 실행 |
| @hourly | 매시간 실행 |
| @daily | 매일 자정(00:00)에 실행 |
| @weekly | 매주 일요일 00:00 실행 |
| @monthly | 매월 1일 00:00 실행 |
| @yearly | 매년 1월 1일 00:00 실행 |
| `"0 12 * * *"` | 매일 낮 12시 정각 실행(cron 표현식 사용 가능) |
* Cron 표현식을 사용하여 맞춤형 실행 주기 설정 가능
* `Schedule_interval=None` 설정 시 DAG는 자동 실행되지 않음

### Cron Schedule 표현
```
{분}{시}{일}{월}{요일}
```
**Examples**
```
30 5 * * *      : 매일 05시 30분
0 * * * *       : 매시 정각
1 * * * *       : 매시 1분
0 0 10 * *      : 매월 10일 0시 0분
30 9 * * 0      : 매주 일요일 09시 30분
*/5 * * * *     : 5분마다 (0, 5, 10…)
(/ 특수 문자는 간격 지정을 의미)
5 9 * 1-5       : 월요일부터 금요일까지 09시 5분에 배치
0 9-15/1 * * *  : 09시부터 15시까지 1시간 간격
30 23 L * *     : 매월 마지막 날 23시 30분에 배치
0 9 * * 3#4     : 매월 4번째 수요일 9시 0분에 배치
```

### 빈도 설정
* cron 식은 특정 빈도로 스케줄을 정의할 수 없음(3일에 한 번 실행 등)
* `timedelta` 인스턴스를 사용해 빈도 기반 스케줄을 정의

```python
dag = DAG(
    dag_id="run_every_3_days_timedelta",
    start_date=datetime(2025, 3, 1),
    schedule_interval=timedelta(days=3),  # 정확히 3일 간격으로 실행
    catchup=False,
)
```

### Backfill이란?
* 특정 과거 Execution Date에 대해 DAG를 수동 실행하여 데이터를 복구하는 작업
* 과거 실행이 누락되었거나, 데이터 오류가 발생했을 때 필요
* `airflow dags backfill` 명령어를 사용하여 특정 날짜 범위의 DAG 실행 가능

```bash
airflow dags backfill -s 2025-08-11 -e 2025-08-14 example_dag
```

### Catchup이란?
* DAG 시작 날짜(`start_date`) 이후 누락된 실행을 보완하기 위해 과거의 Execution Date를 채우는 기능
* `catchup=True` 설정 시, 과거 미실행 DAG를 자동으로 실행하여 누락된 데이터를 처리
* `catchup=False` 설정 시, DAG가 가장 최신 실행 시간부터만 실행됨

### 수동 실행 vs 자동 실행
| 비교 항목 | 자동 실행(Scheduler 기반) | 수동 실행(Manual Trigger) |
| --- | --- | --- |
| 실행 시점 | `data_interval`이 끝난 후 자동 실행 | 사용자가 직접 실행 |
| Logical Date | 스케줄에 따라 자동 계산(예: 2025-03-01) | 기본값: 트리거 시각(직접 지정 가능) |
| Data Interval | 스케줄에 따라 자동 생성(3/1 00:00 ~ 3/2 00:00) | 현재 시각 중심으로 생성 |
| 데이터 정합성 | 데이터가 모두 적재된 뒤 실행되어 정합성 해칠 수 있음 | 수집 중 데이터 포함 가능하며 정합성 깨질 수 있음 |
| 활용 목적 | 정기 워크플로우 운영 | 테스트, 긴급 실행, 백필(backfill) |
| 명령 방식 | Airflow Scheduler 자동 실행 | `airflow dags trigger my_dag`, Web UI 수동 trigger 클릭 |

---

## 5. XCom과 Variable

### XCom이란?
* Cross-Communication의 약자로, Airflow에서 Task 간 데이터를 주고 받기 위한 기능
* 각 Task가 독립적으로 실행되기 때문에, Task 간 데이터 공유를 위해 XCom을 활용
* DAG Run 내에서만 존재하며, 다른 DAG Run과는 공유되지 않음
* DataFrame과 같은 대용량 데이터는 지원하지 않으며, 주로 문자열, 숫자 등 작은 크기의 데이터를 공유함
* PythonOperator를 사용할 경우, 해당 함수의 return 값이 자동으로 XCom에 등록됨

### XCom을 이용한 데이터 전달 원리
**XCom 데이터 저장(xcom_push)**
* Task 실행 중 데이터를 저장할 때 사용
* `task_instance.xcom_push(key, value)`를 사용하여 특정 키로 값 저장

```python
def push_xcom_value(**kwargs):
    kwargs["ti"].xcom_push(key="message", value="Hello from push_task")
```

**XCom 데이터 조회(xcom_pull)**
* Task 실행 시 이전 Task에서 저장한 데이터를 가져올 때 사용
* `task_instance.xcom_pull(task_ids, key)`를 사용하여 특정 Task의 데이터를 가져옴

```python
def pull_xcom_value(**kwargs):
    message = kwargs["ti"].xcom_pull(task_ids="push_task", key="message")
    print("XCom에서 받은 값:", message)
```
* Key-Value 형식으로 저장됨. 해당 DAG의 실행 내에서만 사용 가능

### Xcom 사용 방법
* PythonOperator Return 값을 이용한 Xcom
* Push-pull을 이용한 Xcom
* Jinja template을 이용한 Xcom
* `@task` 데코레이터 사용 시 반환값으로 자동 XCom 저장

**BashOperator with XCom**
```python
push_task = BashOperator(
    task_id="push_task",
    bash_command="echo 'Hello from BashOperator!'",
    do_xcom_push=True,
)

pull_task = BashOperator(
    task_id="pull_task",
    bash_command="echo '{{ ti.xcom_pull(task_ids=\"push_task\") }}'",
)

push_task >> pull_task
```

**Python & BashOperator with XCom**
```python
def push_message(**kwargs):
    kwargs["ti"].xcom_push(key="xcom_value", value="Hello from PythonOperator")

push_task = PythonOperator(
    task_id="push_task",
    python_callable=push_message,
)

pull_task = BashOperator(
    task_id="pull_task",
    bash_command="echo '{{ ti.xcom_pull(task_ids=\"push_task\", key=\"xcom_value\") }}'",
)

push_task >> pull_task
```

### 전역 공유 변수(Variable)란?
* Airflow에서 여러 DAG 및 Task 간에 데이터를 공유하기 위한 변수
* 모든 DAG가 공유할 수 있음 — 협업 환경에서 표준화된 dag를 만들기 위해 사용되며, 상수로 지정해서 사용할 변수를 세팅
* Variable에 등록한 key, value는 메타 데이터베이스에 저장
* 변수 값은 Airflow UI, CLI, API를 통해 관리 가능

**전역 공유 변수(Variable) 등록하기**
1. Webserver에 접속하고 [Admin] → [Variables] 클릭
2. `[+]` 클릭해서 새로운 Variable 생성
3. Key, Value 값을 입력하고 [Save] 클릭

**전역 공유 변수(Variable) 사용하기**
* Variable 라이브러리의 `get` 함수를 사용하여 값 사용
* `var.value`에 꺼내고 싶은 key 값을 입력

```python
from airflow.models import Variable

def print_variable():
    my_var = Variable.get("my_variable", default_var="default")
    print(f"Airflow Variable 값: {my_var}")

print_var_task = PythonOperator(
    task_id="print_variable_task",
    python_callable=print_variable,
)
```

### 전역 공유 변수(Variable) vs XCom
| 비교 항목 | XCom | Variable |
| --- | --- | --- |
| 데이터 유지 기간 | DAG 실행 단위로 유지됨 | Airflow 전체에서 지속적으로 유지됨 |
| 데이터 저장 위치 | Airflow 메타데이터 DB | Airflow 메타데이터 DB |
| 사용 목적 | Task 간 데이터 전달 | DAG 실행 간 설정값 저장 |
| 데이터 호출 방식 | `xcom_push()`, `xcom_pull()` 사용 | `Variable.get()`, `Variable.set()` 사용 |
| 저장 가능한 데이터 유형 | JSON 직렬화 가능한 작은 데이터(문자열, 숫자, 리스트, 딕셔너리) | 문자열 및 JSON 직렬화 가능한 데이터 |
| 범위 | DAG 실행 내에서만 사용 가능 | 모든 DAG에서 전역적으로 사용 가능 |
| 자동 저장 여부 | PythonOperator의 return 값이 자동 저장됨 | 자동 저장되지 않음, 명시적으로 설정 필요 |
| 보안 고려 사항 | Task 간 민감한 데이터 전달 시 사용하지 않음 | API 키, 비밀번호 등은 Connection을 활용하는 것이 더 안전함 |

---

## 핵심 요약
* Airflow의 **Operator**는 Task의 실행 방식을 정의하며, BashOperator/PythonOperator 같은 Action Operator부터 Sensor/Transfer/Database/BigData Operator까지 다양하다. **BranchOperator**로 조건에 따라 Task 흐름을 분기할 수 있고, 선택되지 않은 Task는 `Skipped` 처리된다.
* `@task` 데코레이터를 사용하면 PythonOperator + 수동 XCom push/pull 없이도 함수 호출만으로 태스크 정의 및 데이터 전달, 의존성 설정이 가능해 코드가 간결해진다.
* Task 연결은 `>>`/`<<` 연산자로 순차·병렬 실행을 정의하며, **Trigger Rule**(`all_success`, `all_failed`, `all_done` 등)로 Upstream 결과에 따른 실행 조건을 세밀하게 제어한다.
* DAG 스케줄은 **start_date(첫 데이터 기준 시점) → schedule_interval(실행 주기, Cron/timedelta) → logical_date(논리적 실행 식별 날짜)** 순으로 이해해야 하며, 실제 실행은 항상 logical_date/data_interval 종료 이후 이루어져 데이터 정합성을 보장한다. 과거 데이터 복구는 `backfill`, 시작일 이후 누락분 자동 처리는 `catchup`으로 구분한다.
* Task 간 데이터 공유는 **XCom**(DAG Run 내, 소용량 데이터, `xcom_push`/`xcom_pull`)과 **Variable**(Airflow 전역, 설정값 저장, `Variable.get`/`Variable.set`)로 나뉘며, 용도에 맞게 선택해야 한다.
