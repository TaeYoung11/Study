# Pandas 심화 정리

이 문서는 **Pandas 기초**에서 다룬 DataFrame/Series를 넘어, **NumPy와의 연동**, 그룹 단위 **데이터 처리와 집계**, 여러 데이터프레임을 하나로 합치는 **병합 및 변환** 방법을 정리한 가이드입니다.

---

## 1. Pandas와 NumPy

### NumPy는 왜 함께 쓰나요?

Pandas의 DataFrame/Series는 내부적으로 **NumPy 배열(ndarray)** 위에 구축되어 있습니다. 따라서 대량의 수치 연산이 필요할 때는 NumPy의 벡터화(Vectorization) 연산을 그대로 활용할 수 있어 반복문보다 훨씬 빠릅니다.

```python
import numpy as np
import pandas as pd

df = pd.DataFrame({"a": [1, 2, 3], "b": [10, 20, 30]})

# DataFrame <-> ndarray 변환
arr = df.to_numpy()          # DataFrame -> NumPy 배열
df2 = pd.DataFrame(arr, columns=["a", "b"])   # ndarray -> DataFrame

# 벡터화 연산 (반복문 없이 전체 컬럼에 적용)
df["c"] = np.sqrt(df["a"])
df["d"] = np.where(df["b"] > 15, "High", "Low")   # 조건에 따른 값 매핑
```

### 주요 NumPy 연계 함수

| 함수 | 설명 |
| --- | --- |
| `np.where(cond, A, B)` | 조건이 참이면 A, 거짓이면 B (컬럼 단위 if-else) |
| `np.select(conds, choices)` | 여러 조건을 순서대로 검사해 값을 선택 |
| `np.nan` | Pandas 결측치(NaN)의 실제 구현체 |
| `df.values` / `df.to_numpy()` | DataFrame의 값을 NumPy 배열로 추출 |

> Pandas가 결측치를 `NaN`으로 표현할 수 있는 것도, `NaN` 자체가 NumPy에서 정의된 부동소수점 특수값이기 때문입니다.

---

## 2. 데이터 처리와 집계 (GroupBy)

### Split - Apply - Combine

Pandas의 그룹 연산은 **분할(Split) → 적용(Apply) → 결합(Combine)** 3단계로 동작합니다.

1. **Split:** 특정 기준(컬럼 값)으로 데이터를 그룹으로 나눔
2. **Apply:** 각 그룹에 집계/변환 함수를 적용
3. **Combine:** 결과를 다시 하나의 데이터프레임으로 결합

```python
titanic.groupby("Pclass")["Fare"].mean()           # 객실 등급별 평균 요금
titanic.groupby(["Pclass", "Sex"])["Age"].mean()    # 다중 기준 그룹화
```

### 집계 함수 (Aggregation)

```python
titanic.groupby("Pclass")["Fare"].agg(["mean", "sum", "count", "max"])

# 컬럼마다 다른 집계 함수 적용
titanic.groupby("Pclass").agg({"Fare": "mean", "Age": "max"})

# 그룹별 통계 요약
titanic.groupby("Sex")["Age"].describe()
```

* **`agg()`:** 하나 이상의 집계 함수를 한 번에 지정해 적용
* **`transform()`:** 집계 결과를 원본과 동일한 shape으로 되돌려, 그룹 평균 대비 편차 등을 구할 때 사용

```python
# 그룹(성별) 평균과의 차이 컬럼 추가
titanic["age_diff"] = titanic["Age"] - titanic.groupby("Sex")["Age"].transform("mean")
```

### 피벗 테이블 (Pivot Table)

```python
pd.pivot_table(
    titanic,
    index="Pclass",       # 행 기준
    columns="Sex",        # 열 기준
    values="Survived",    # 집계 대상
    aggfunc="mean",       # 집계 함수
)
```

`pivot_table`은 `groupby` + `unstack`을 합쳐 놓은 형태로, 두 개 이상의 기준으로 데이터를 교차 집계할 때 편리합니다.

---

## 3. 데이터 병합 및 변환

### `concat`: 단순 연결

행 방향(위아래) 또는 열 방향(좌우)으로 데이터프레임을 이어 붙입니다.

```python
df_all = pd.concat([df1, df2], axis=0, ignore_index=True)   # 행 방향 연결 (위+아래)
df_wide = pd.concat([df1, df2], axis=1)                       # 열 방향 연결 (좌+우)
```

### `merge`: 키(Key) 기준 결합

SQL의 JOIN과 동일한 개념으로, 공통 컬럼(키)을 기준으로 두 데이터프레임을 결합합니다.

```python
pd.merge(orders, customers, on="customer_id", how="left")
```

| `how` 옵션 | 설명 |
| --- | --- |
| `inner` (기본값) | 양쪽에 키가 모두 존재하는 행만 결합 |
| `left` | 왼쪽 데이터프레임 기준으로 결합, 오른쪽에 없으면 NaN |
| `right` | 오른쪽 데이터프레임 기준으로 결합 |
| `outer` | 양쪽 키의 합집합으로 결합, 없는 값은 NaN |

* 컬럼명이 서로 다르면 `left_on`, `right_on`으로 각각 지정
* `df.join()`은 인덱스를 기준으로 결합하는 `merge`의 축약형

### `melt` / `pivot`: 넓은 형태 ↔ 긴 형태 변환

```python
# Wide -> Long : 여러 컬럼을 하나의 변수/값 컬럼으로 풀어냄
long_df = pd.melt(wide_df, id_vars=["name"], var_name="subject", value_name="score")

# Long -> Wide : melt의 반대, 값을 다시 컬럼으로 펼침
wide_df = long_df.pivot(index="name", columns="subject", values="score")
```

* **Wide(넓은) 형태:** 사람마다 과목별 점수가 각각의 컬럼으로 존재 (사람 수 = 행 수)
* **Long(긴) 형태:** `이름-과목-점수` 3개 컬럼으로만 구성되어 행 수가 늘어나는 대신 분석/시각화 라이브러리(seaborn 등)에 넣기 좋은 형태

---

## 핵심 요약
* Pandas의 DataFrame/Series는 NumPy 배열 위에서 동작하므로, `to_numpy()`·`np.where()`처럼 NumPy와 넘나들며 벡터화 연산을 활용할 수 있습니다.
* 그룹 연산은 **Split → Apply → Combine** 흐름이며, `groupby().agg()`로 집계, `transform()`으로 그룹 기준 값을 원본 shape 그대로 되돌려 받을 수 있습니다.
* 여러 데이터프레임은 `concat`(단순 이어붙이기)과 `merge`(키 기준 SQL식 JOIN)로 병합하며, `melt`/`pivot`으로 Wide ↔ Long 형태를 자유롭게 변환할 수 있습니다.
