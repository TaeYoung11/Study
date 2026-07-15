# Spark DataFrame과 Spark SQL 정리

이 문서는 Spark의 핵심 데이터 모델인 **DataFrame**과 이를 SQL 방식으로 다룰 수 있게 해주는 **Spark SQL**에 대한 이론 및 실습 코드를 정리한 가이드입니다.

---

## 1. Spark DataFrame과 Spark SQL 개요

### RDD(Resilient Distributed Dataset)의 복습과 한계
* **정의:** 데이터를 병렬 처리하는 핵심 역할로, 빠르고 안정적인 동작을 지원합니다.
* **한계:**
* **스키마의 부재:** 데이터 값은 표현할 수 있으나, 메타데이터(스키마)에 대한 명시적 표현 방법이 없습니다.
* **최적화의 어려움:** 스파크가 람다 표현식 내부를 알 수 없어 연산 최적화가 불가능합니다.
* **데이터 타입 인식 부족:** 특히 PySpark에서 데이터 타입을 제대로 인식하지 못하고 일반 객체(Iterator)로 처리하여 성능 저하 및 직렬화 비용이 발생합니다.



### DataFrame과 Spark SQL의 등장

* **DataFrame:** 스키마를 가진 분산 데이터 컬렉션으로, 데이터를 행(Row)과 열(Column)로 구성된 표 형태로 관리합니다.
* **Spark SQL:** 구조화된 데이터를 SQL처럼 처리할 수 있게 해주는 모듈로, 내부적으로 **Catalyst Optimizer**를 통해 쿼리를 최적화합니다.

---

## 2. DataFrame API 상세

### 데이터 타입

Spark DataFrame은 정교한 데이터 처리를 위해 다양한 타입을 지원합니다.

* **기본 타입:** Byte, Short, Integer, Long, Float, Double, String, Boolean, Decimal
* **정형화 타입:** Binary, Timestamp, Date, Array, Map, Struct, StructField

### 스키마(Schema) 정의

스키마를 미리 정의하면 스파크가 데이터 타입을 추측하는 부담을 덜고, 오류를 조기에 발견할 수 있습니다.

```python
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, IntegerType

schema = StructType([
    StructField("name", StringType(), True),
    StructField("scores", ArrayType(IntegerType()), True)
])

```

---

## 3. RDD vs DataFrame vs Dataset

| 구분 | RDD | DataFrame | Dataset |
| --- | --- | --- | --- |
| **데이터 표현** | 값만 표현 가능 | 명확한 스키마 존재 | 타입 안정성 보장 (객체 중심) |
| **최적화** | 어려움 (직접 제어) | **Catalyst Optimizer** 자동 최적화 | 자동 최적화 |
| **사용 언어** | 모든 언어 지원 | 모든 언어 지원 | Java, Scala만 지원 |
| **오류 발견** | 런타임 시점 | 런타임 시점 (분석 단계) | **컴파일 시점** (타입 체크) |

---

## 4. 데이터 조회 및 기본 검사 (Inspect Data)

### 데이터 읽기 및 생성

```python
#JSON 로드
df spark.read.json("filename.json")
#Parquet 로드
df = spark.read.load("filename.parquet")

#Inspect Data 함수
df.show()               # 내용 표시
df.printSchema()        # 스키마 구조 출력
df.columns              # 컬럼 이름 반환
df.count()              # 행 개수 계산
df.describe().show()    # 요약 통계
df.explain()            # 실행 계획 출력

```

---

## 5. DSL vs SQL 실습 코드 비교

Spark는 **DSL (Domain Specific Language)** 방식과 **SQL** 방식 두 가지를 모두 지원합니다. SQL 사용을 위해서는 먼저 'createOrReplaceTempView`를 통해 뷰를 등록해야 합니다.

### 뷰 등록 (SQL 사용 전 필수)

```python
df.createOrReplaceTempView("my_table")

```

### (1) 데이터 선택 및 필터링 (Select & Filter)

* **DSL:**
```python
df.select(col("column1"), (col ("column2") + 1).alias("plus1")).show()
df.filter(col ("column1") > "A").show()

```


* **SQL:**
```sql
SELECT column1, column2 + 1 AS plus1 FROM my_table WHERE column1 > 'A'

```



### (2) 조건문 처리 (When / Case When)

* **DSL:**
```python
from pyspark.sql.functions import when
df.select("column1", when (df.column2 > 100, 1).otherwise (0).alias("flag")).show()
```


* **SQL:**
```sql
SELECT column1, CASE WHEN column2 > 100 THEN 1 ELSE 0 END AS flag FROM my_table

```



### (3) 문자열 및 범위 처리 (Like, Between, Substring)

* **DSL:**
```python
# 'A'로 시작하는지 확인
df.select(col("column1").startswith("A")).show()
# 50~150 사이 값
df.filter(col ("column2").between (50, 150)).show()
# 문자열 추출 (2번째부터 3자)
df.select(df.column1.substr(2, 3)).show()

```

* **SQL:**
```sql
SELECT * FROM my_table WHERE column1 LIKE 'A%'
SELECT * FROM my_table WHERE column2 BETWEEN 50 AND 150
SELECT SUBSTRING(column1, 2, 3) FROM my_table

```



### (4) 집계 및 정렬 (Group By & Sort)

* **DSL:**
```python
df.groupBy("column1").count().show()
df.orderBy(["column1", "column2"], ascending=[True, False]).show()

```


* **SQL:**
```sql
SELECT column1, COUNT(*) FROM my_table GROUP BY column1
SELECT * FROM my_table ORDER BY column1 ASC, column2 DESC

```


---

## 6. 결측값 처리 및 구조 변환

### 결측값 처리 (Missing Values)

* **DSL:**
```python
df.na.fill({"column1": "Unknown", "column2": 0}).show() #값 채우기
df.na.drop().show() # NULL 포함 행 삭제

```


* **SQL:**
```sql
SELECT COALESCE (column1, 'Unknown'), COALESCE(column2, 0) FROM my_table

```



### 구조 변환

```python
rdd_version = df.rdd                # RDD로 변환
pandas_df = df.toPandas()           # Pandas DataFrame으로 변환
json_str = df.toJSON().first()      # JSON 문자열로 변환

```

---

## 7. Spark SQL의 내부 동작: Catalyst Optimizer

Spark SQL이 효율적인 이유는 **Catalyst Optimizer** 덕분입니다.

1. **Analysis:** SQL 파싱 및 DataFrame API 해석
2. **Logical Plan:** 논리적 실행 계획 수립
3. **Optimization:** 논리 계획 최적화
4. **Physical Plan:** 실제 물리적 실행 계획 생성 및 최적의 경로 선택

**결론:** DataFrame과 Spark SQL은 동일한 최적화 엔진을 공유하므로, 사용자의 편의에 따라 어떤 방식을 사용하더라도 성능상 이점을 동일하게 누릴 수 있습니다.