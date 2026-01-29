# Spark RDD 핵심 정리

Spark의 가장 기본적인 데이터 처리 단위인 **RDD(Resilient Distributed Dataset)**에 대한 개념 특징, 생성 방법 및 주요 연산(Transformation & Action)을 정리한 문서입니다.

---

## 1. RDD의 개념과 특징

### RDD란?

대용량 데이터를 분산 처리하고 분석하기 위한 Spark의 가장 기초적인 데이터 모델입니다.

* **Resilient (탄력적인):** 장애 발생 시 데이터를 자동으로 복구할 수 있는 능력.
* **Distributed (분산된):** 클러스터의 여러 노드에 데이터가 나누어 저장됨.
* **Dataset (데이터셋):** 처리할 데이터의 집합.

### RDD의 주요 특징

1. **탄력성(Resilient) & 불변성(Immutable):**
* RDD는 한 번 생성되면 내용을 변경할 수 없습니다.
* 데이터가 손실되어도 계보(Lineage)를 통해 다시 계산하여 복구할 수 있습니다.

2. **타입 안정성 (Type safety):**
* 컴파일 시점에 데이터 타입을 검사하여 오류를 방지합니다. (Scala, Java 해당 / Python은 동적 타입 언어이므로 제외)

3. **정형 & 비정형 데이터 지원:**
* 텍스트와 같은 비정형 데이터는 `sc.textFile()`로 로딩 후 가공하며, 컬럼이 있는 정형 데이터는 DataFrame이나 RDD의 Map연산을 활용합니다.

4. **지연 평가 (Lazy Evaluation):**
* Action 연산이 호출되기 전까지는 실제 연산을 수행하지 않고 실행 계획만 세웁니다.
* 이를 통해 쿼리 최적화가 가능하며 불필요한 리소스 낭비를 줄입니다.



---

## 2. RDD 생성 방법

### 1) 메모리 데이터 활용 (`parallelize()`)

주로 테스트나 작은 규모의 데이터셋을 다룰 때 사용합니다.

```python
# Python 예제
wordsRDD = sc.parallelize(["fish", "cats", "dogs"])

```

### 2) 외부 파일 로드 (`sc.textFile()`)

실무에서 가장 많이 사용하는 방법으로, 로컬 파일 시스템, HDFS, S3 등에서 데이터를 읽어옵니다.

```python
#텍스트 파일 로드 예제
linesRDD = sc.textFile("/path/to/README.md")

```

---

## 3. RDD Transformation (변환 연산)

새로운 RDD를 반환하며, 지연 평가(Lazy Evaluation) 방식으로 동작합니다.

### 주요 변환 함수

* **map(f):** 각 요소에 함수를 적용하여 1:1로 변환합니다.
* **flatMap(f):** 각 요소에 함수를 적용한 후 결과를 평탄화(Flatten)합니다. (1:N 변환 가능)
* **filter(f):** 조건이 참인 요소만 골라냅니다.
* **keyBy(f):** 특정 기준에 따라 Key-Value 쌍(Pair RDD)을 생성합니다.
* **groupBy(f):** 함수 결과값을 기준으로 데이터를 그룹화합니다.
* **groupByKey():** 동일한 키를 가진 값들을 그룹화합니다.
* **reduceByKey(f):** 동일한 키를 가진 값들을 병합합니다. (**성능상 groupByKey보다 권장**)
* **join(other):** 같은 키를 가진 두 RDD를 조인합니다.
* **union(other):** 두 RDD를 합칩니다. (중복 제거 안함)
* **distinct():** 중복을 제거합니다.
* **sample(withReplacement, fraction): ** 통계적 샘플을 추출합니다.

### 파티션 제어
* **repartition(n):** 파티션 수를 늘리거나 줄일 때 사용 (셔플 발생).
* **coalesce(n):** 파티션 수를 줄일 때 사용 (셔플 최소화).
* **mapPartitions(f):** 각 파티션 단위로 함수를 적용하여 오버헤드를 줄입니다.

## 4. RDD Action (실행 연산)

실제 계산을 시작하고 결과를 드라이버 프로그램으로 반환하거나 저장합니다.

| 연산 | 설명 | 예제 결과 |
| --- | --- | --- |
| **collect()** | 모든 데이터를 리스트로 반환 | `[1, 2, 3, 4]` |
| **count()** | 요소의 총 개수 반환 | `4` |
| **reduce(f)** | 요소를 이진 연산으로 결합 | `10` (합계 시) |
| **sum()** | 모든 요소의 합계 | `10` |
| **mean()** | 모든 요소의 평균 | `2.5` |
| **countByKey()** | 키별 등장 횟수를 맵(Map) 형태로 반환 | `{'A': 2, 'B': 1}` |

---

## 5. 실습 코드 정리

### 지연 평가(Lazy Evaluation) 예시

```python
from pyspark import SparkContext
sc = SparkContext("local", "Example")

rdd = sc.parallelize (["apple", "banana", "spark", "data"])
upper_rdd = rdd.map(lambda x: x.upper())
filtered_rdd upper_rdd.filter(lambda x: "SPARK" in x)

# 여기까지는 실행되지 않음
result = filtered_rdd.collect() # 여기서 실제 연산 수행

```

### Word Counting 비교

#### 1) groupByKey 활용 (비효율적일 수 있음)

```python
words = sc.parallelize (['one', 'two', 'two', 'three', 'three', 'three'])
wordCounts = words.map(lambda w: (w, 1)) \
                  .groupByKey()\
                  .map(lambda pair: (pair[0], sum(pair[1])))

```

#### 2) reduceByKey 활용 (권장)

셔플 전 로컬 집계(Local Aggregation)를 수행하여 네트워크 전송량을 줄입니다.

```python
wordCounts = words.map(lambda w: (w, 1)) \
                  .reduceByKey (lambda a, b: a + b)

```