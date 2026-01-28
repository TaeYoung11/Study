# Spark 기본 구조와 세팅 정리

이 문서는 **Apache Spark**가 등장한 배경과 전체 실행 구조(아키텍처), 그리고 로컬 환경에서 Spark를 설치하고 실행하는 방법을 정리한 가이드입니다. 이후 다룰 **RDD**, **DataFrame/Spark SQL** 문서의 기반이 되는 내용입니다.

---

## 1. Spark 개요

### 빅데이터 처리의 흐름: MapReduce에서 Spark로

* **Hadoop MapReduce:** 대용량 데이터를 분산 처리하는 초기 프레임워크. 연산 단계마다 중간 결과를 **디스크(HDFS)** 에 기록하기 때문에 반복 연산(머신러닝, 그래프 처리 등)에서 I/O 비용이 크게 발생합니다.
* **Apache Spark:** 중간 결과를 **메모리(In-Memory)** 에 유지하며 연산하는 클러스터 컴퓨팅 엔진으로, 반복적인 연산에서 MapReduce보다 훨씬 빠른 속도를 냅니다.

### Spark의 특징

* **인메모리 연산(In-Memory Computing):** 디스크 I/O를 최소화하여 빠른 처리 속도 제공
* **통합 분석 엔진(Unified Engine):** 배치 처리, SQL, 스트리밍, 머신러닝(MLlib), 그래프 처리(GraphX)를 하나의 엔진으로 통합 지원
* **다양한 언어 지원:** Scala, Java, Python(PySpark), R, SQL
* **다양한 실행 환경:** Standalone, Hadoop YARN, Kubernetes, 클라우드 등 어디서든 실행 가능

### Spark 생태계 구성 요소

| 구성 요소 | 역할 |
| --- | --- |
| **Spark Core** | RDD 기반의 기본 실행 엔진, 스케줄링/메모리 관리 담당 |
| **Spark SQL** | 구조화된 데이터를 SQL/DataFrame으로 처리 |
| **Spark Streaming** | 실시간 스트림 데이터 처리 |
| **MLlib** | 분산 환경에서의 머신러닝 라이브러리 |
| **GraphX** | 그래프 구조 데이터 처리 |

---

## 2. Spark 실행 구조 (아키텍처)

Spark는 **Driver - Cluster Manager - Executor** 구조로 동작하는 **Master-Slave** 아키텍처를 가집니다.

* **Driver Program:** 사용자가 작성한 Spark 애플리케이션(main 함수)이 실행되는 프로세스
  * `SparkContext`(또는 `SparkSession`)를 생성하여 클러스터와 통신
  * 애플리케이션 코드를 **Job → Stage → Task** 단위로 분할하고 스케줄링
* **Cluster Manager:** 클러스터의 자원(CPU, 메모리)을 관리하고 Executor에 자원을 할당
  * 종류: Standalone(Spark 자체 제공), YARN, Kubernetes, Mesos
* **Executor:** 각 워커 노드에서 실제 Task를 실행하는 프로세스
  * Task 실행 결과를 메모리/디스크에 캐시하고, 최종 결과를 Driver로 반환

### 작업 단위 분할: Job → Stage → Task

1. **Job:** Action 연산 하나가 호출될 때마다 생성되는 작업 단위
2. **Stage:** Job 내에서 **셔플(Shuffle)** 이 발생하는 경계를 기준으로 나뉘는 단위
3. **Task:** Stage 내에서 파티션 단위로 실행되는 가장 작은 실행 단위 (Executor의 코어 하나가 Task 하나를 처리)

```
Application → Job(Action 1회) → Stage(Shuffle 경계) → Task(파티션 단위)
```

### 실행 모드

| 모드 | 설명 |
| --- | --- |
| **Local** | 클러스터 없이 로컬 머신 한 대에서 멀티스레드로 실행 (개발/테스트용) |
| **Standalone** | Spark 자체 클러스터 매니저 사용 |
| **YARN** | Hadoop 클러스터의 자원 관리자를 그대로 활용 |
| **Kubernetes** | 컨테이너 오케스트레이션 환경에서 Executor를 Pod 단위로 실행 |

---

## 3. Spark 설치 및 환경 설정

### 설치 준비물

Spark는 JVM 기반으로 동작하므로 **Java(JDK)** 가 필수이며, PySpark 사용 시 **Python**이 함께 필요합니다.

```bash
# Java 설치 확인
java -version

# Python 설치 확인
python --version
```

### PySpark 설치 (pip)

간단하게 파이썬 환경에서 Spark를 사용하려면 `pyspark` 패키지를 설치하는 것만으로 로컬 실행이 가능합니다.

```bash
pip install pyspark
```

### 환경 변수 설정

Spark를 직접 다운로드하여 설치하는 경우, 아래 환경 변수를 설정해 어디서든 `spark-submit`, `pyspark` 명령을 사용할 수 있도록 합니다.

```bash
export SPARK_HOME=/path/to/spark
export PATH=$SPARK_HOME/bin:$PATH
export JAVA_HOME=/path/to/jdk
```

### 실행 진입점

| 명령/객체 | 설명 |
| --- | --- |
| `pyspark` | 대화형 셸(REPL)로 즉시 코드를 실행하며 실습 가능 |
| `spark-submit app.py` | 작성한 Spark 애플리케이션 파일을 클러스터/로컬에 제출하여 실행 |
| `SparkSession` | Spark 2.x 이후 진입점으로 통합된 객체. `SparkContext`, `SQLContext` 기능을 모두 포함 |

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("MyFirstApp") \
    .master("local[*]") \
    .getOrCreate()

# 기존 RDD API 진입점은 spark.sparkContext 로 접근
sc = spark.sparkContext
```

* `master("local[*]")`: 로컬 머신의 사용 가능한 모든 코어를 사용해 실행
* `appName`: Spark UI(모니터링 웹 페이지, 기본 포트 4040)에 표시되는 애플리케이션 이름

---

## 핵심 요약
* Spark는 중간 결과를 메모리에 유지하는 **인메모리 연산**과, 배치/SQL/스트리밍/ML을 하나로 묶는 **통합 분석 엔진**이라는 점에서 기존 MapReduce와 차별화됩니다.
* 실행 구조는 **Driver(스케줄링) - Cluster Manager(자원 할당) - Executor(실제 연산)** 로 이루어지며, 작업은 **Job → Stage → Task** 단위로 쪼개져 처리됩니다.
* 개발 환경에서는 `pip install pyspark` 만으로 로컬 실행이 가능하며, `SparkSession`이 RDD/DataFrame/SQL 기능을 아우르는 통합 진입점 역할을 합니다.
