# Pandas 기초 정리

이 문서는 데이터 분석의 개념과 역할을 살펴보고, 파이썬 데이터 분석 라이브러리 **Pandas**의 기본 데이터 구조와 표(Tabular) 데이터를 다루는 방법을 정리한 가이드입니다.

---

## 1. 데이터 분석 개론

### 데이터 분석은 왜 하나요?
데이터 분석은 결국 **합리적인 의사 결정**, 즉 갖고 있는 자원에서 최적의 선택을 하기 위해 수행합니다.
* 카페를 차리고 싶은데 어느 지역에 차려야 할까?
* 온라인 쇼핑몰에서 어떤 상품이 잘 팔릴까?
* 신규 배달 서비스는 어느 시간대에 집중해야 할까?
* 신규 채용 시 어떤 부서에 인력이 필요한가?
* 제조 공정 중 불량률이 높은 공정은 어디인가?

### 데이터 분석 프로세스
1. **문제 정의:** 분석으로 풀고자 하는 질문을 구체화 (예: "노인 치매환자 관리를 어떻게 효율화할까?")
2. **데이터 기획:** 문제와 관련된 데이터 항목 설계 (인구 현황, 병원 수, 소득 수준 등)
3. **데이터 수집**
4. **데이터 전처리**
5. **데이터 시각화**
6. **분석 및 인사이트 도출**

### 실전 사례 (쏘카 데이터 분석)
* **관찰:** 특정 지점(뚝섬)에서 시간대별 대여량이 급증, 차량이 부족해짐
* **분석:** 인근 지역(한양대) 대여량과 비교 → 뚝섬 인근 대학 학사일정과 상관관계 확인
* **결론:** 차량 재배치가 필요한 곳은 뚝섬이고, 한양대의 여유 차량을 뚝섬으로 옮겨야 한다는 인사이트 도출
* 이처럼 데이터 분석은 **막연한 추측을 정량적 근거로 검증**하는 역할을 합니다.

### 데이터 관련 직무와 역량
| 직무 | 설명 |
| --- | --- |
| 데이터 분석가 | 데이터 기반 인사이트 도출, 리포트 작성 |
| 데이터 사이언티스트 | 통계/머신러닝 기반 모델링 |
| 데이터 엔지니어 | 데이터 파이프라인/인프라 구축 |
| ML/DL 엔지니어 | 머신러닝·딥러닝 모델 개발 |
| PM 등 | 데이터 기반 의사결정 총괄 |

데이터 관련 직무에는 **Computer Science, Math & Statistics, Machine Learning, Business/Domain Expertise**가 교차하는 지점(Data Science, Data Analysis)의 역량이 요구됩니다.

### 파이썬 데이터 분석 생태계
* **머신러닝/통계:** scikit-learn
* **배열/선형대수:** NumPy, SciPy
* **데이터 핸들링:** Pandas
* **시각화:** Matplotlib, Seaborn
* **개발 환경:** Jupyter

---

## 2. Pandas 개요

* **정의:** 관계형(relational) 혹은 레이블(labeling)된 데이터를 효율적으로 다루기 위해 설계된 Python 기반 데이터 분석 라이브러리
* **특징:**
  * 빠르고 유연하며 표현력이 풍부한 데이터 구조 제공
  * 다양한 데이터 분석 작업을 손쉽게 처리할 수 있는 고수준 빌딩 블록(high-level building blocks) 제공
  * 오픈 소스 라이브러리로 누구나 자유롭게 사용 가능
  * NumPy를 기반으로 개발되어 과학 계산 및 머신러닝 라이브러리들과 호환성/통합성이 좋음
* **다룰 수 있는 데이터 타입:**
  * SQL table, Excel spreadsheet 같은 표 형식의 데이터 (Tabular data)
  * 순서가 있거나(Ordered) 없는(Unordered) 데이터
  * 시계열(Time series) 데이터
  * 행과 열이 있는 임의의 행렬 데이터
  * 레이블이 없어도 유연하게 처리 가능한 관측/통계 데이터셋

### 핵심 데이터 구조: DataFrame · Series · Index

| 구조 | 차원 | 설명 |
| --- | --- | --- |
| **Series** | 1차원 | 한 줄짜리 데이터 목록. 모든 값이 같은 타입 |
| **DataFrame** | 2차원 | 여러 Series(열)의 모음. 열마다 타입이 다를 수 있고 크기 조절 자유로움 |
| **Index** | - | DataFrame/Series의 고유한 Key 값 객체 (행을 식별) |

```python
import pandas as pd

# Series: 1개의 Column 값으로만 구성된 1차원 데이터
ages = pd.Series(data=[22, 35, 58], name="Age")

# DataFrame: Columns x Rows 2차원 데이터
df = pd.DataFrame(
    {
        "Name": ["Braund, Mr. Owen Harris", "Allen, Mr. William Henry", "Bonnell, Miss. Elizabeth"],
        "Age": [22, 35, 58],
        "Sex": ["male", "male", "female"],
    }
)
```

---

## 3. 데이터프레임 생성 및 조작

### 데이터 읽기
```python
import pandas as pd

titanic = pd.read_csv("./data/titanic.csv")   # csv 파일 로드
data_df = pd.DataFrame(d1)                     # dict → DataFrame으로 변환
```

### 데이터 훑어보기 (Inspect)
```python
titanic.head()          # 상위 n개 행 (기본 5개)
titanic.tail()           # 하위 n개 행
titanic.sample(5)        # 무작위 n개 행 추출
titanic.shape             # (행, 열) 크기 튜플, 예: (891, 12)
titanic.columns           # 컬럼명 목록
titanic.index              # 인덱스 정보
titanic.info()             # 컬럼명, 데이터 타입, 결측치(Non-Null Count) 정보
titanic.describe()         # 수치형 컬럼의 평균/표준편차/사분위 등 요약 통계
titanic["Embarked"].value_counts()               # 값별 개수 (기본 dropna=True → NaN 제외)
titanic["Embarked"].value_counts(dropna=False)   # NaN 포함하여 집계
```

### 컬럼 추가/삭제
```python
# 새 컬럼 추가 (일괄 값 할당)
titanic["Age_new"] = 0

# 컬럼 삭제 (axis=1) / 행 삭제 (axis=0)
titanic.drop("Age_new", axis=1, inplace=True)
# 여러 행 삭제 시 라벨을 리스트로 전달: titanic.drop([0, 1, 2], axis=0)
```
* `inplace=False`(기본값): 원본은 유지하고 결과가 반영된 새 DataFrame을 반환
* `inplace=True`: 원본 DataFrame에 바로 반영 (반환값을 별도로 받을 필요 없음). 다만 메모리 절약 효과는 없고, 내부적으로도 복사(copy) 후 재대입하는 방식이라 성능상 이점은 없음

---

## 4. 데이터 정제 및 전처리

데이터 전처리는 모델에 넣기 전 데이터를 알맞게 가공하는 과정으로, **어떤 전처리를 적용하느냐에 따라 원본 데이터가 다르게 변형되고 분석 결과도 달라질 수 있습니다.**

### 전처리의 주요 항목
* 데이터의 형식 맞추기 (예: 날짜 형식 통일)
* 빈 칸(결측치) 채우기
* 데이터 열 추가 (연관 데이터 추가)
* 데이터 열 추가 (이상치 판별용 플래그 추가)

### 결측치 확인 및 처리
```python
titanic.isna()                       # 결측치(NaN) 여부를 True/False로 반환
titanic.isna().sum()                  # 컬럼별 결측치 개수
titanic["Embarked"].isna().sum()      # 특정 컬럼의 결측치 개수

# 결측치 제거 (dropna)
clean_df = titanic.dropna(subset=["Embarked"], how="any")

# 결측치 채우기 (fillna)
most_freq = titanic["Embarked"].value_counts().idxmax()
filled_df = titanic.fillna({"Embarked": most_freq})
```
* `isna()`: 데이터프레임/배열에서 결측값(NaN) 여부를 True/False로 반환
* `fillna()`: 결측값(NaN)을 지정한 값으로 대체
* `dropna()`: 결측값(NaN)이 포함된 행 또는 열을 제거

### 이상치(Outlier) 처리
| 유형 | 예시 | 해결 방법 |
| --- | --- | --- |
| 존재할 수 없는 값 | 생년월일이 미래 날짜, 신장/체중이 음수 등 비정상 값 | 논리적으로 존재할 수 없으므로 삭제 처리, 혹은 재조사·평균값으로 대체 |
| 극단적인 값 | 정상 범위를 크게 벗어난 값 (통계적 Outlier) | 박스플롯 등 기준 확인 후 반영 여부 판단. IQR·표준편차 등을 이용해 어디까지가 정상치인지 판단 |

---

## 5. 데이터 인덱싱과 필터링

### 세 가지 접근 방식

1. **`[ ]` (대괄호 인덱싱)**
   * 컬럼 단일 선택: `df['col1']` → Series 반환
   * 컬럼 여러 개 선택: `df[['col1', 'col2']]` → DataFrame 반환
   * Boolean Indexing과 함께 사용 가능: `df[df['col1'] > 10]` → 조건에 맞는 행 필터

2. **`loc[ ]` / `iloc[ ]`**
   * `loc`: 행/열의 **이름(Label)** 으로 접근
   * `iloc`: **정수 위치(Position)** 기반 인덱싱 (0부터 시작하는 숫자 인덱스 사용)

3. **Boolean Indexing**
   * 조건식을 `[ ]` 안에 기입하여 간편하게 필터링 수행

### 코드 예시
```python
# [] 컬럼 선택
series = titanic["Name"]                 # Series 반환
subset = titanic[["Name", "Age"]]          # DataFrame 반환

# loc: 라벨 기반
value = titanic.loc[5, "Pclass"]
subset = titanic.loc[5:7, ["Name", "Age", "Embarked"]]

# iloc: 위치 기반
value = titanic.iloc[0, 1]
subset = titanic.iloc[0:3, 1:4]
subset = titanic.iloc[0, [1, 2, 3]]

# Boolean Indexing
titanic_boolean = titanic[titanic["Age"] >= 60]      # 60세 이상
adult_names = titanic.loc[titanic["Age"] > 35, "Name"]  # 조건 + 컬럼 동시 지정 (loc[])
class_23 = titanic[(titanic["Pclass"] == 2) | (titanic["Pclass"] == 3)]  # 여러 조건 결합
```

> `loc`는 라벨(이름) 기준이라 슬라이싱 시 **끝 인덱스를 포함**하고, `iloc`는 정수 위치 기준이라 파이썬 슬라이싱처럼 **끝 인덱스를 포함하지 않는** 점이 차이입니다.

---

## 핵심 요약
* 데이터 분석은 결국 **의사결정을 위한 근거 마련**이 목적이며, 문제 정의 → 기획 → 수집 → 전처리 → 시각화 → 인사이트 도출의 프로세스를 따릅니다.
* Pandas는 **Series(1차원) / DataFrame(2차원) / Index**를 기본 골격으로 표 형태 데이터를 다룹니다.
* `head/tail/shape/info/describe/value_counts` 등으로 데이터를 빠르게 훑어보고, `isna/fillna/dropna`로 결측치를, 통계적 기준으로 이상치를 처리합니다.
* 데이터 선택은 `[]`, `loc[]`(라벨 기반), `iloc[]`(위치 기반), Boolean Indexing 네 가지 방식을 상황에 맞게 조합해 사용합니다.
