# Airflow 기본 구조와 세팅 정리

워크플로우 오케스트레이션 도구 **Apache Airflow**의 개념, 아키텍처, 주요 컴포넌트와 Docker 기반 설치, 그리고 DAG의 기본 구조 및 실행 방법을 정리한 문서입니다.

---

## 1. 워크플로우 오케스트레이션과 Airflow 개요

### 워크플로우 오케스트레이션이란?
* 작업(Task)을 정해진 순서에 따라 실행하고 자동화하는 기술. 데이터 파이프라인, 머신러닝, CI/CD 등 다양한 분야에서 활용
* **주요 기능:** 작업 자동화(반복적인 수작업 제거), 의존성 관리(특정 작업이 완료된 후 다른 작업 실행), 실패 처리 및 모니터링(장애 발생 시 재시도 및 알림), 스케줄링 기능(특정 시간/이벤트 기반 실행)

### Apache Airflow란?
* Python을 기반의 데이터 파이프라인 자동화 및 스케줄링 도구. 배치 프로세스를 효율적으로 관리하는 워크플로우 오케스트레이션 도구
* 다양한 시스템 및 데이터베이스와 통합 가능(ETL, MLOps, 클라우드 등)
* **역할:** 데이터 파이프라인 자동화, DAG(Directed Acyclic Graph)로 작업 정의, 실행 및 장애 관리, 모니터링 기능 제공

```
task_1 → task_2 → task_3
```

### Airflow의 주요 활용 사례
| 사례 | 설명 |
| --- | --- |
| Business Operations | 데이터 중심 애플리케이션 및 업무 자동화, 정기 리포트 생성 및 배포, 고객 데이터 파이프라인 및 CRM 연동 |
| ETL / ELT | 데이터 수집(Extract), 변환(Transform), 적재(Load) 자동화. 여러 데이터 소스를 통합해 분석 가능. Data Warehouse(BigQuery, Snowflake 등)와 연계 |
| Infrastructure Management | 클라우드 리소스 자동 프로비저닝(AWS, GCP, Azure 등), CI/CD 파이프라인 자동 실행, Kubernetes 및 서버 배포 자동화 |
| MLOps | 데이터 수집 → 모델 학습 → 평가 → 배포 과정 자동화, 머신러닝 워크플로우 오케스트레이션, 모델 재학습 및 모니터링 자동화 |

### Why Airflow? (기존 방식의 한계)
* **기존 방식(Cron Job, Bash Script)의 한계:** 작업 간 의존성 관리 어려움, 장애 발생 시 원인 추적 및 복구 어려움, 로그 관리 및 모니터링 부족
* **Airflow 도입 시 장점:** DAG 기반 의존성 관리(Task 실행 순서 설정 가능), Web UI 제공(직관적인 모니터링 가능), 재시도 및 알림 기능(장애 발생 시 자동 대응), 확장성 및 유연성(다양한 실행 환경 지원)

### Airflow의 장점과 단점
* **장점:** Python 기반(개발자가 쉽게 접근 가능), 강력한 UI 제공(직관적인 모니터링 가능), 태스크 간 의존성 관리 용이(DAG 구조 활용), 확장성 높음(다양한 Executor 지원), 장애 복구 기능 제공(재시도 및 알림 설정 가능)
* **단점:** 초기 설정 및 학습 곡선이 가파름, 실시간 데이터 처리에는 적합하지 않음, 복잡한 DAG의 경우 성능 튜닝 필요

---

## 2. Airflow 주요 컴포넌트

| 컴포넌트 | 설명 |
| --- | --- |
| **Scheduler** | DAG 실행 스케줄 관리. DAG 파일을 파싱하고, Task 및 DAG를 모니터링하며 실행을 스케줄링하는 핵심 컴포넌트. DAG Run과 Task Instance 상태를 관리하고 Executor에게 실행을 요청 |
| **Executor** | 태스크 실행 방식(Local, Celery, Kubernetes 등) 담당. Scheduler에서 생성하는 서브 프로세스로, Queue에 들어온 Task Instance를 실제로 실행하는 역할 |
| **Worker** | 실제 태스크를 실행하는 프로세스 |
| **Metadata Database** | DAG 실행 정보 저장. Airflow의 DAG, DAG Run, Task Instance, Variables, Connections 등 여러 컴포넌트에서 사용하는 데이터를 저장 |
| **Web UI** | DAG 및 태스크 상태 모니터링. Meta Database와 통신하며 관련 데이터를 가져와 웹에서 보여주고 유저와 상호작용할 수 있게 함 |

### Dag Directory
* 파이썬으로 작성된 DAG 파일을 저장하는 공간(`dag_folder`, `dags_folder`라고도 불림). 기본적으로 `$AIRFLOW_HOME/dags/`로 설정됨
* **DAG 파일 처리 과정:** ① DAG 파일 검색 및 로드 → ② DAG 파일 파싱 및 해석 → ③ DAG 등록 및 실행 준비
* DAG를 작성한 후 DAG Directory에 저장하면, Airflow Scheduler가 주기적으로 Dag Directory를 스캔한 후 DAG를 파싱함

### Executor의 종류
* **단일 프로세스형(Single-Process):** Sequential Executor — 한 번에 하나의 Task만 순차적으로 실행하며, 개발·테스트용으로 사용됨
* **로컬 병렬형(Local Multi-Process):** Local Executor — Scheduler 내부에서 여러 Task를 병렬로 실행 가능(멀티프로세싱 기반)
* **분산형(Distributed):** Celery Executor(여러 워커 노드에 Task를 분산 실행, 대규모 분산 환경에 적합), Kubernetes Executor(각 Task를 독립적인 Pod로 실행하여 완전한 격리와 자동 확장을 지원)

---

## 3. Airflow 설치 (Docker Compose 기반)

공식 문서: `https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html`

```bash
# 1. docker-compose.yaml 다운로드
curl -LfO 'https://airflow.apache.org/docs/apache-airflow/2.10.5/docker-compose.yaml'

# 2. Airflow User 세팅
mkdir -p ./dags ./logs ./plugins ./config
echo -e "AIRFLOW_UID=$(id -u)" > .env

# 3. docker airflow 초기화
docker compose up airflow-init

# 4. Airflow 실행
docker compose up
```
* 설치 완료 후 `http://localhost:8080` 접속 → 로그인 화면에서 기본 계정(`airflow` / `airflow`)으로 로그인 → DAGs 목록 화면 확인 가능

### 개발 환경 구성 — Airflow Library 설치
* 로컬 개발/자동완성 환경을 위해 `apache-airflow` 패키지를 PyPI로 설치 가능 (constraints 파일 기준 버전 고정 권장)

---

## 4. DAG와 Task 개념

### DAG(Directed Acyclic Graph)란?
* Airflow에서 작업(Task)들의 실행 순서를 정의하는 그래프
* **방향성(Directed):** 작업이 정해진 순서로 실행됨
* **비순환(Acyclic):** 순환(Loop) 구조가 없어 무한 실행 방지

```
   ┌─→ c ─┐
a ─┤       ├─→ d
   └─→ b ─┘
```

### Task란?
* Task는 워크플로우를 구성하는 개별 작업 단위. ETL, 데이터 변환, 머신러닝 모델 실행, 파일 이동 등의 작업 수행 가능
* 특정 연산을 수행하는 **Operator**를 사용하여 정의 (BashOperator, PythonOperator, SQLExecuteOperator 등)
* DAG 내에서 하나의 노드(Node)로 존재하며, 서로 의존성을 설정하여 순차적 실행(Sequential) 또는 병렬 실행(Parallel)이 가능함
* 재시도(Retry) 및 실행 시간 제한 설정 가능

### Task 주요 속성
| 속성 | 설명 |
| --- | --- |
| task_id | Task의 고유 식별자(DAG 내에서 유일해야 함) |
| operator | Task가 수행할 작업을 정의(BashOperator, PythonOperator 등) |
| depends_on_past | 이전 실행 결과에 따라 Task 실행 여부 결정 |
| retries | Task 실패 시 재시도 횟수 설정(기본값: 0) |
| execution_timeout | Task 실행 시간 제한 설정(지정된 시간 내 미완료 시 실패 처리) |
| start_date / end_date | DAG 실행 시작·종료 날짜 및 시간 설정 |
| schedule_interval | Task 실행 주기 설정(예: `@daily`, `@hourly`, `0 12 * * *`) |
| priority_weight | Task의 실행 우선순위 설정(높을수록 우선 실행) |
| task_concurrency | 특정 Task의 병렬 실행 가능 개수 제한 |

### Task LifeCycle
```
none → scheduled → queued → running → success
                       ↓          ↓
                    up_for_retry  failed → up_for_retry(재시도) / failed(최종 실패)
```
* Task는 상태 전이를 거치며 실행되고, 재시도(retry) 조건이 충족되면 `up_for_retry` 상태를 거쳐 다시 `scheduled`로 돌아감

### DAG 및 Task 실행 상태 종류
| 상태 | 설명 |
| --- | --- |
| deferred | Task 실행이 특정 이벤트 대기 중(예: Sensor 대기) |
| failed | Task 실행이 실패하여 실행 종료됨 |
| queued | Task가 실행 대기 중(리소스 확보 대기) |
| removed | Task가 DAG에서 삭제되어 (일시적으로) 더 이상 보이지 않음 |
| restarting | Task가 재시작 중 |
| running | Task가 실제 실행 중 |
| scheduled | Task가 실행될 준비가 됨(예약된 상태) |
| shutdown | Task 실행 중지됨(Airflow 시스템에 의해 강제 중지) |
| skipped | Task가 건너뛰어짐(조건에 따라) |
| success | Task가 정상적으로 완료됨 |
| up_for_reschedule | Sensor가 일정 대기 방식일 때 이 상태로 감 |
| up_for_retry | Task가 실패했지만 재시도 예정 |
| upstream_failed | 이전(Upstream) Task 실패로 실행되지 않은 상태 |
| no_status | Task에 대한 정보가 없는 상태(아직 생성된 적 없음) |

---

## 5. DAG 파일 생성 및 배치

* `dags` 폴더를 생성하고, 하위에 `dags_bash_operator.py` 파일 생성
* Airflow 컨테이너에서 로컬 디렉토리를 활용하기 위한 볼륨 마운팅 작업(`docker-compose.yaml` 수정)

```yaml
volumes:
  - ${AIRFLOW_PROJ_DIR:-.}/dags:/opt/airflow/dags
  - ${AIRFLOW_PROJ_DIR:-.}/logs:/opt/airflow/logs
  - ${AIRFLOW_PROJ_DIR:-.}/config:/opt/airflow/config
  - ${AIRFLOW_PROJ_DIR:-.}/plugins:/opt/airflow/plugins
```

**DAG 예시 (`dags_bash_operator.py`)**
```python
from airflow.models.dag import DAG
from airflow import dataset
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="dags_bash_operator",
    schedule="0 0 * * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=60),
    tags=["example"],
) as dag:

    bash_t1 = BashOperator(
        task_id="bash_t1",
        bash_command="echo whoami",
    )

    bash_t2 = BashOperator(
        task_id="bash_t2",
        bash_command="echo $HOSTNAME",
    )

    bash_t1 >> bash_t2
```
* 선행 단계를 완료한 후, Airflow Web UI에서 `dags_bash_operator` DAG가 정상 업로드 되었는지 목록에서 확인 가능

---

## 6. Airflow 스케줄링

* **Schedule:** DAG를 실행하는 주기를 설정하는 방식
* **Schedule interval:** DAG이 실행되는 시간 간격을 결정하는 속성

| 설정 방식 | 설명 | 예제 |
| --- | --- | --- |
| None | DAG이 자동 실행되지 않음(수동 실행만 가능) | `schedule_interval=None` |
| 예약어 사용 | 실행 주기를 간편하게 지정 | `@daily`(매일), `@hourly`(매시간) |
| Cron 표현식 사용 | 세부적인 실행 주기 설정 가능 | `"0 9 * * *"` (매일 오전 9시 실행) |
| Timedelta 사용 | 특정 시간 간격마다 실행 | `schedule_interval=timedelta(hours=6)` |

```python
dag = DAG(
    dag_id="example_schedule",
    start_date=datetime(2025, 3, 1),
    schedule_interval="0 9 * * *",  # 매일 오전 9시 실행
    catchup=False,
)
```

---

## 7. Airflow Web UI

* Airflow Web UI는 DAG 및 Task의 실행 상태를 시각적으로 모니터링하고, DAG 실행을 제어할 수 있는 웹 기반 인터페이스
* **주요 기능:** DAG 및 Task의 실행 상태 확인, DAG의 수동 실행 및 중지, Task의 재시도 및 강제 실행, 실행 로그 확인 및 실패 원인 분석, DAG 및 Task의 의존성 시각화

### Airflow Web UI 주요 메뉴 구성
| 메뉴 | 설명 |
| --- | --- |
| DAGs View | 등록된 모든 DAG을 조회할 수 있는 기본 화면. DAG 상태(활성, 비활성, 일시정지) 확인 가능, 특정 DAG을 클릭하여 세부 정보 확인 가능, "Trigger DAG" 버튼을 눌러 DAG을 수동 실행 가능 |
| Graph View(그래프 뷰) | DAG 내부의 Task 간 관계를 시각적 그래프로 표현. Task들의 의존성(Upstream/Downstream) 구조 확인 가능, 각 Task의 실행 상태를 색상으로 구분(성공, 실패, 진행 중 등) |
| Tree View(트리 뷰) | DAG의 실행 이력을 날짜별 트리 구조로 제공. DAG 실행 시 Task가 어떤 상태였는지 기록 확인 가능, 특정 실행 날짜의 DAG 상태를 쉽게 추적 가능 |
| Task Instance Details(태스크 실행 상세 정보) | 특정 Task를 클릭하면 실행 정보와 로그를 확인 가능. 실패한 Task를 다시 실행하거나, 강제로 종료할 수 있음 |
| Gantt Chart(간트 차트) | DAG 실행 시 각 Task의 실행 시간을 시각화. Task 실행 시간 비교 및 병렬 실행 분석 가능, 실행 시간이 오래 걸리는 Task를 찾아 최적화할 수 있음 |
| Code View(코드 뷰) | DAG의 소스 코드를 UI에서 직접 확인 가능. DAG를 수정하려면 별도의 파일 편집이 필요하지만, UI에서 DAG 정의를 쉽게 검토 가능 |
| Log View(로그 뷰) | 실행된 Task의 로그를 확인하여 오류 분석 및 디버깅 가능. 실패한 Task의 원인을 파악하고, 재시도를 결정하는 데 활용 |

### DAG 실행 방법
* **자동 실행:** DAG에 설정된 스케줄(interval, cron 등)에 따라 실행
* **수동 실행:** Web UI 또는 CLI(Command Line Interface)를 통해 즉시 실행
* **이벤트 기반 실행:** 특정 트리거(예: API 요청, 파일 업로드 등)로 실행

---

## 핵심 요약
* **Airflow**는 Python 기반의 워크플로우 오케스트레이션 도구로, **DAG(방향성+비순환)** 구조로 Task 실행 순서를 정의하고 스케줄링·모니터링·재시도를 지원합니다.
* 핵심 컴포넌트는 **Scheduler(스케줄 관리) → Executor(실행 방식 결정) → Worker(실제 실행)**, 그리고 이를 뒷받침하는 **Metadata Database**와 **Web UI**로 구성됩니다.
* Executor는 Sequential(단일 프로세스) → Local(로컬 병렬) → Celery/Kubernetes(분산) 순으로 확장성이 커지며, 환경 규모에 맞게 선택합니다.
* 설치는 공식 `docker-compose.yaml` 기반으로 `docker compose up airflow-init` → `docker compose up` 순서로 진행하며, `localhost:8080`에서 Web UI로 DAG 상태·Graph/Tree/Gantt View·로그를 확인합니다.
* DAG는 `dags` 폴더(볼륨 마운트된 `$AIRFLOW_HOME/dags`)에 Python 파일로 작성하며, `schedule_interval`(None/예약어/Cron/timedelta)로 실행 주기를, Task의 `retries`/`execution_timeout` 등 속성으로 재시도·실행 제한을 설정합니다.
