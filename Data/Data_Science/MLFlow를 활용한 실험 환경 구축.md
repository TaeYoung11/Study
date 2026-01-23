# MLFlow를 활용한 실험 환경 구축 정리

머신러닝 실험 환경이 왜 필요한지부터, MLFlow Tracking을 활용한 **실험 관리 전략**, 하이퍼파라미터 튜닝, 그리고 **모델 관리 및 배포(REST API 서빙)** 까지 실습 흐름을 정리한 문서입니다.

---

## 1. 머신러닝 실험 환경의 필요성

### 머신러닝 실험이란?
* 모델의 성능을 개선하거나 다양한 설정을 비교/분석하기 위해 반복적으로 수행하는 **과정 중심의 실험 활동**
* 즉, "어떤 데이터를 써서", "어떤 알고리즘에", "어떤 하이퍼파라미터를 적용했더니", "어떤 결과가 나왔는지"를 체계적으로 비교하는 작업
* 왜 실험이 중요한가? 머신러닝에서는 하나의 정답이 존재하지 않고, 여러 조건에 따라 성능이 달라지기 때문에 실험이 필수적

| 요소 | 설명 |
| --- | --- |
| 데이터셋 | 전처리 방식에 따라 결과가 크게 달라질 수 있음 |
| 모델 구조 | 동일한 데이터여도 모델 구조가 성능에 영향을 줌 |
| 하이퍼파라미터 | 학습률, 에폭 수, 정규화 강도 등 |
| 성능 지표 | Accuracy, F1 score, AUC 등 목적에 따라 다름 |

### 반복 실험의 문제점
| 문제 | 설명 |
| --- | --- |
| 재현성(Reproducibility) 문제 | "이 모델이 왜 잘 나왔는지, 다시 만들 수 있을까?" 코드와 결과는 있지만 어떤 하이퍼파라미터로 학습했는지 기억이 안나는 경우. 다시 똑같은 성능을 재현할 수 없음 |
| 버전 관리의 문제 | "이 모델, 어떤 코드 버전에서 나온 거였지?" 같은 train.py 파일인데 중간에 코드 바뀜, 파일명을 복사해서 train2.py, train_final_final2.py 식으로 쓰게 되는 상황 |
| 협업의 문제 | "동료가 만든 모델, 어떻게 학습된 건지 모르겠어요" 실험 결과 공유가 슬랙/구두/스크린샷에 의존, 모델은 공유되지만 학습 파라미터, 평가 방식, 데이터가 없음 |
| 비효율적인 반복 작업 | "똑같은 실험, 이미 했던 상황" 기존 실험 기록이 없어 같은 실험을 다시 시도, 불필요한 리소스 낭비(시간, 전력, 인프라) |
| 성과 측정 및 비교의 어려움 | "결국 어떤 모델이 제일 좋았는지 알 수 없는 경우" 다양한 모델을 실험 했지만, 성능 비교 테이블이 없음, 지표 종류마다 따로 기록되어, 비교 자체가 어려움 |

**그래서 필요한 것이 실험 관리 도구(MLFlow 등)**

### 실험 추적 및 관리 도구
* **실험 추적 도구의 필요성:** 반복 실험에서 발생하는 문제를 해결하기 위함. 실험마다 사용된 코드, 데이터, 하이퍼파라미터, 성능 지표를 자동으로 기록. 실험 결과를 비교 분석할 수 있는 인터페이스 필요
* **실험 환경 자동화의 필요성:** 실험은 단순 로깅을 넘어서, 재현 가능한 환경에서 실행되어야 함
* **실험 추적 + 모델 저장 + 서빙까지 지원하는 통합 도구: MLFlow** — 오픈 소스로 제공되는 머신러닝 실험 관리 플랫폼. 다양한 프레임워크와 호환성이 좋음. 실험 로그 관리 뿐 아니라 모델 저장, 등록, 서빙까지 한번에 가능

---

## 2. MLFlow Tracking을 활용한 실험 관리 전략

### 실험 관리 전략이란?
* 실험을 체계적으로 계획하고 수행하고 비교 분석하는 전 과정
* 실험 목표 수립 → 실험 단위 정의 → 비교 가능한 실험 설계 → 자동화
* 예시로 실험 목적에 따른 실험 공간을 분리해서 관리할 수 있음

### 실험 결과 저장 위치 구조
* MLFlow는 실험 데이터를 기본적으로 `mlruns/` 디렉토리에 저장
* 로컬 저장 기준이며, 백엔드 저장소를 바꾸면 DB/S3 등에 저장 가능
* 추가로 MLFlow UI에 접속하여 해당 Run을 클릭하면 파일 구조를 확인 가능

```
mlruns/
└── 0/                         # Experiment ID (0: default)
    └── <run_id>/              # Run ID 디렉토리
        ├── metrics/
        ├── params/
        ├── artifacts/
        └── meta.yaml
```

| 디렉토리 | 역할 |
| --- | --- |
| Metrics/ | 실험 성능 지표 파일들 |
| Params/ | 하이퍼파라미터 저장 |
| Artifacts | 모델, 이미지 등 결과물 |
| Meta.yaml | 사용한 데이터 버전 |

### 실험 단위 구분
* **실험 단위 구분 전략:** 실험 그룹/Run 이름 정리법
  * 실험 그룹: `baseline_rf_exp1`, `tuned_rf_exp2`
  * Run 이름: `max_depth=5`, `max_depth=10`
  * 모델/데이터/전처리 조합에 따라 Run 그룹화
  * 파일 이름 형식도 "모델명_전처리 방법_데이터셋 버전"으로 하면 명확함

```
실험 그룹: rf_w_smote_v2
├── Run1: max_depth=5
├── Run2: max_depth=10
└── Run3: max_depth=10, n_estimators=100
```

### 실험 정보 구조화
MLFlow는 실험 정보를 4가지 요소로 구분해 기록하며, 각각의 정보는 비교, 분석, 재현성에서 중요한 역할을 가짐

| 항목 | 설명 | 예시 |
| --- | --- | --- |
| Params | 실험 설정값(하이퍼파라미터) | Learning_rate=0.01, max_depth=5 |
| Metrics | 성능 결과 | Accuracy=0.89, auc=0.91 |
| Tags | 실험 설명, 날씨 등 메타정보 | Exp_purpose=baseline_test, author=kim |
| Artifacts | 결과 파일, 시각화, 모델 등 | 모델 파일, confusion_matrix.png, 로그 |

### 실험 태그 및 노트 관리 방법
* MLFlow는 메타 정보 태그화. UI와 API에서 태그 기반 검색이 가능함 → 실험 분류/필터링에 유용

| 태그 키 | 용도 |
| --- | --- |
| Author | 작성자 이름 |
| Description | 실험 목적 |
| Notes | 참고 사항 |
| Data_version | 사용한 데이터 버전 |

```python
mlflow.set_tag("author", "kim")
mlflow.set_tag("description", "XGBoost with SMOTE")
mlflow.set_tag("data_version", "v2.1")
```

### 커스텀 로깅 전략
* 중요한 정보를 직접 로깅하는 방법. MLFlow는 기본 로깅 외에도 내가 원하는 정보를 자유롭게 추가 가능
* 모델의 의미 있는 지표를 따로 로깅 가능(val_f1, recall 등)
* 태그에 실험을 담아 나에서 실험의 목적, 주요 이벤트도 기록이 가능

```python
mlflow.log_param("boosting_type", "gbtree")
mlflow.log_metric("val_f1", val_f1_score)
mlflow.set_tag("description", "XGBoost with SMOTE")
mlflow.log_artifact("roc_curve.png")
```

### 고급 실험 로깅
* GridSearch나 반복 실험에서 각 결과를 자동으로 MLFlow에 로깅하면 관리가 편함
* 실험별 파라미터, 지표 로깅, ROC curve, confusion matrix 이미지를 생성 후 저장
* 반복 실험이 많을수록 자동 로깅 코드 구조화가 필수
* 시각화 파일도 artifact로 저장하면 UI 상에서 바로 확인이 가능함

### 실험 비교 전략 / 검색 예시
* MLFlow UI에서는 다양한 실험을 필터링, 정렬, 시각화하며 비교 가능
* 파라미터 조합별 성능 비교가 쉬워짐. 성능이 아니라, 실험 설정 자체에 주목 가능

| 실험 목적 | 분리할 Experiment 예시 |
| --- | --- |
| 필터링 | 특정 조건(max_depth=10)으로 run 선택 |
| 지표 정렬 | Accuracy 등 성능 기준으로 정렬 |
| 그래프 비교 | 여러 Run의 성능 변화 시각화(선형 그래프, 히트맵) |
| 다운로드 | CSV로 성능 결과 저장 가능 |

```python
# Python API를 활용한 실험 검색 - 기본 실험 ID = 0 (Default experiment), 사용자 지정 실험 생성 시 ID 증가
runs = mlflow.search_runs(
    experiment_ids=["4"],
    filter_string="params.max_depth = '5'",
    order_by=["metrics.f1 DESC"],
    output_format="pandas",
)

best_run = df.iloc[0]
print("Best Run ID:", best_run.run_id)
print("F1 score:", best_run["metrics.f1_score"])
print("Used params:", best_run["params.max_depth"], best_run["params.n_estimators"])
```

### 실험 실패/중단 상황 관리
* try…finally / try…except + `mlflow.end_run()`
* 실험이 끝나면 `mlflow.end_run()` 명령어를 통해 명시적으로 종료해줘야 로그가 깔끔하게 남음. 하지만 에러가 나면 종료가 누락될 수 있음

```python
try:
    run_experiment(params)
except Exception as e:
    with open("failures.log", "a") as f:
        f.write(f"{params}: {e}\n")
    runs = mlflow.search_runs(filter_string="status = 'FAILED'")
    failed_run_ids = runs.run_id.tolist()
```

### 실험 자동화 흐름 예시
* Config.yaml에 실험 조건 정의: 반복할 실험의 조건(예: 하이퍼파라미터)을 YAML 파일에 정의해두면, 실험을 체계적으로 구성하고 재사용 가능
```yaml
learning_rate: [0.01, 0.001]
batch_size: [16, 32]
optimizer: ['adam', 'sgd']
```
* itertools.product 또는 반복문으로 조합 생성: 모든 실험 조합을 만들기 위해 Python의 `itertools.product`를 사용하거나 중첩 반복문을 사용할 수 있음
```python
import itertools
import yaml

with open("config.yaml", "r") as f:
    config = yaml.safe_load(f)

keys, values = zip(*config.items())
combinations = list(itertools.product(*values))   # 모든 하이퍼파라미터 조합 생성

for comb in combinations:
    params = dict(zip(keys, comb))
    run_experiment(params)   # 실험 실행 함수 호출
```
* `mlflow.start_run()` 블록 안에서 파라미터와 결과를 자동으로 로깅. 각 조합에 대해 하나의 실험(run)이 생성되며, 비교/시각화에 활용
```python
import mlflow

def run_experiment(params):
    with mlflow.start_run():
        mlflow.log_params(params)
        model, metrics = train_model(params)
        mlflow.log_metrics(metrics)
        mlflow.sklearn.log_model(model, "model")
```

### 실험 관리 전략 팁
* 실험을 전략적으로 관리: MLFlow는 실험 전략 실행 플랫폼. 실험 이름/구조 설계 → 자동화된 로깅 → 정리된 비교 → 재현성 확보. 단순 로그 기록이 아니라, 모델 실험을 반복 가능한 연구 활동으로 만드는 도구
* **실험 관리 시 유용한 팁 3가지:** 실험명 관리(실험 목적, 날짜 포함), Jupyter 요약 정리(`mlflow.search_runs()`로 테이블 생성), 중단 대비 로깅(try-finally로 중간 로그 남기기)

### MLFlow Tracking 구조 다시 보기
* 하나의 Experiment에는 여러 개의 Run이 존재. 각 Run은 실험 한 번 실행 시 생성되는 단위. 파라미터, 성능지표, 모델 등을 저장
* Experiment 단위로 실험을 분류: 실제 실험을 반복하다보면 다양한 실험 목적 발생

| 실험 목적 | 분리할 Experiment 예시 |
| --- | --- |
| 베이스라인 모델 성능 측정 | Baseline_model_exp |
| 하이퍼파라미터 튜닝 | Rf_tuning_exp |
| 전처리 기법 적용 실험 | Smote_test_exp |
| 다른 모델 비교 실험 | Model_comparison_exp |

* Run ID 기반 추적: MLFlow는 각 Run에 대해 고유한 Run ID를 부여. 이 Run ID는 실험 결과의 정확한 출처를 추적하거나, 특정 모델을 재사용할 때 사용. 저장된 모델과 실험 로그를 연결할 수 있어 재현성과 추적성 확보
```python
run_id = "2bc6dabc123..."   # UI에서 확인 가능
model_uri = f"runs:/{run_id}/model"
model = mlflow.sklearn.load_model(model_uri)
```

### MLFlow 하이퍼파라미터 튜닝
* 하이퍼파라미터는 학습 알고리즘의 학습 방식에 직접적인 영향을 미침. 잘 튜닝된 모델 vs 기본값 모델 → 성능 차이가 상당함. GridSearch, RandomSearch, HyperOpt 등의 사용

**Grid Search vs Random Search**
| 항목 | Grid Search | Random Search |
| --- | --- | --- |
| 탐색 방식 | 지정된 모든 하이퍼파라미터 조합을 완탐함 | 임의의 조합 일부만 무작위 탐색 |
| 개산 비용 | 조합 수가 많을수록 커짐 | 조합 수를 지정 가능 |
| 사용 시기 | 조합 수가 적거나, 중요한 파라미터가 명확히 제 하려는 때 | 조합 수가 많거나, 중요한 파라미터를 아직 잘 모를 때 |
| 장점 | 최적 조합을 놓치지 않음, 관리하고 직관적 | 빠르고 효율적, 넓은 차원의 탐색 공간에서 유리함 |
| 단점 | 비효율적 | 최적값을 정확히 못 찾을 가능성 있음 |

**Hyperopt란?** 머신러닝 모델의 하이퍼파라미터 튜닝을 자동으로 수행해주는 라이브러리. 핵심 목표는 최적의 파라미터 조합을 효율적으로 찾는 것
* **특징:** 목적 기반 최적화(단순 반복이 아닌, 성능을 기반으로 검색), 이전 결과 학습(과거 실험 결과를 기반으로, 다음 탐색 위치를 정교하게 조정), 계산량 절약(수백 개 실험을 거치지 않아도 좋은 조합에 빠르게 수렴), 다양한 공간 지원(실수형, 정수형, 범주형, 조건부 파라미터 등 복잡한 탐색 구조 지원)

```python
def objective(params):
    result = train_model(
        params,
        epochs, model,
        train_x=train_x, train_y=train_y,
        valid_x=valid_x, valid_y=valid_y,
        test_x=test_x, test_y=test_y,
    )
    return result

space = {
    "lr": hp.loguniform("lr", np.log(1e-5), np.log(1e-1)),
    "momentum": hp.uniform("momentum", 0.0, 1.0),
}
```

---

## 3. MLFlow 모델 관리 및 배포

### 모델 관리 및 배포 개요
* **Stage란?** MLFlow Model Registry에서 하나의 모델 버전이 어떤 배포 단계에 있는지를 표시하는 상태 값

| 상태 | 설명 |
| --- | --- |
| None | 초기 등록 |
| Staging | 테스트 또는 검증 단계 |
| Production | 실제 서비스에 배포된 단계 |
| Archived | 더 이상 사용하지 않는 이전 버전 모델 |

* 흐름: 모델 개발 및 실험 → None → 성능 검증 후 테스트 환경 배포 → Staging → QA 통과 후 서비스 배포 → Production → 새 모델로 교체되면 이전 모델 → Archived

### MLFlow Models 구조
* MLFlow 구성요소 및 개념: 다양한 프레임워크 지원(sklearn, keras, xgboost 등), 모델 포맷(Flavor): run ID 기반으로 저장, 환경 정보: conda.yaml, MLmodel, requirement.txt 자동 생성
```
model/
├── MLmodel
├── conda.yaml
└── model.pkl
```
* 모델 저장의 필요성: Pickle, joblib은 간단하지만 환경 정보 없음, 코드 추적 불가. **MLFlow Models: 모델뿐 아니라 실험 환경, 입력 형태, 프레임워크 정보까지 저장 가능**. 핵심은 실험 결과를 재현하고 협업하려면 단순 .pkl 파일로는 부족

### 모델 저장 방법
* 모델 저장 위치 설정(로컬 vs 원격): 로컬 디스크(기본 설정, 간단한 실험에 적합), S3, GCS(팀 프로젝트, 클라우드 기반 실험에 적합), 데이터베이스(SQLite, MySQL 등, 실험 메타데이터 저장 용도로도 활용 가능)
```python
import mlflow.sklearn
mlflow.sklearn.log_model(model, "model")
```
```
mlflow server \
  --backend-store-uri sqlite:///mlflow.db \
  --default-artifact-root s3://my-bucket/mlruns
```
* 저장된 모델 불러오기: 모델 검증 시, 추론 테스트를 위해 저장된 모델 불러오기. 모델 서빙 시, API 배포에 활용. `mlflow.pyfunc.load_model()`을 사용하면 공통 인터페이스로 불러올 수 있음
```python
model_uri = f"runs:/{run_id}/model"
loaded_model = mlflow.sklearn.load_model(model_uri)
```

### 모델 경량화 및 구조 분석
* 저장된 MImodel 파일에는 모델 flavor, 입력/출력 signature 정보가 있음
* Signature를 정의하면 추론 시 입력 형태를 자동 검증 가능
```python
from mlflow.models.signature import infer_signature

signature = infer_signature(X_train, y_pred)
mlflow.sklearn.log_model(model, "model", signature=signature)
```

### 모델 배포 전략
* 모델 관리 배포의 흐름: **실험이 완료 → 모델 저장(log_model) → Run ID 기반으로 불러와서 예측 결과를 평가하고 다시 로깅**
* 로컬 환경에서 API로 서빙하기: MLFlow 서빙 명령어를 통해 배포 가능
```bash
mlflow models serve -m runs:/<run_id>/model -p 5000
```
* 기본 구성: RESTful API 형식, `/invocations` 엔드포인트로 JSON 데이터로 POST
```bash
curl -X POST http://localhost:5000/invocations \
  -H "Content-Type: application/json" \
  -d '{"columns":["feat1", "feat2"], "data":[[1, 2]]}'
```

### MLFlow 모델 서빙
* MLFlow 서빙은 훈련된 머신러닝 모델을 운영 환경에 쉽게 배포할 수 있도록 도와주는 도구
* **서빙 엔진 비교**

| 항목 | FastAPI (기본) | MLServer (고급/확장형) |
| --- | --- | --- |
| 용도 | 로컬 테스트, 일반 서비스용 | 고성능, 대규모 서비스용 |
| 설치 | MLflow 설치 시 기본 포함 | 추가 설치 필요 |
| 서빙 프레임워크 | FastAPI(ASGI 기반, 빠름) | MLServer(Seldon/Kserve와 연동) |
| 성능 | 비동기 처리 지원, 빠름 | 병렬 추론, 일괄 처리 요구 극대화 |
| 확장성 | 단일 서버 중심(수평 확장 어려움) | Kubernetes 네이티브 확장 지원(오케스트레이션) |
| 추론 API | `/invocations` 엔드포인트 사용 | 동일한 `/invocations` 사용 |

* **MLFlow Models의 확장성:** 다양한 프레임워크 지원(scikit-learn, tensorflow, xgboost, pytorch 등과 연동 가능), 이식성(Databricks, AWS SageMaker 등 다양한 플랫폼에 배포 가능), 모델 관리 연계(추후 Model Registry, 서빙 인프라(API Gateway 등)와 자연스럽게 연결됨), 통합 구조(모델 저장 → 로딩 → 배포까지 하나의 포맷으로 처리 가능: MLmodel + artifact)

### 실습 흐름: 실행 비교, 모델 선택, REST API 배포
1. **Wine 데이터 전처리** — 입력 정규화를 위한 평균과 분산 계산 → Dense 2층 구조의 회귀 모델 → 출력은 하나(와인 품질 예측). SGD 옵티마이저로 학습률과 모멘텀을 적용, 손실 함수: MSE, Metric: RMSE
```python
import mlflow
from mlflow.models import infer_signature

data = pd.read_csv("https://raw.githubusercontent.com/mlflow/mlflow/master/tests/datasets/winequality-white.csv", sep=";")
train, test = train_test_split(data, test_size=0.2, random_state=42)

train_x = train.drop(["quality"], axis=1).values
train_y = train[["quality"]].values.ravel()
test_x = test.drop(["quality"], axis=1).values
test_y = test[["quality"]].values.ravel()

signature = infer_signature(train_x, train_y)
```

2. **하이퍼파라미터 최적화 및 모델과 로그저장** — Hyperopt로 튜닝, 성능 좋은 모델 로그 저장
```python
def objective(params):
    result = train_model(params, epochs, train_x, train_y, valid_x, valid_y, test_x, test_y)
    return result

space = [
    hp.loguniform("lr", np.log(1e-5), np.log(1e-1)),
    hp.uniform("momentum", 0.0, 1.0),
]

best = fmin(fn=objective, space=space, algo=tpe.suggest, max_evals=n, trials=trials)
mlflow.log_params(best)
mlflow.tensorflow.log_model(best_run["model"], "model", signature=signature)
```

3. **모델 실행 비교** — `http://127.0.0.1:5000/` 접속하여 Compare Runs 화면에서 Plot별로 확인 가능, RMSE가 가장 낮은 행 선택

4. **모델 선택 및 등록** — Register Model 클릭하면, 모델을 정식 등록. 등록을 통해 1) 버전 관리 가능 2) 설명, 태그 추가 가능 3) 어떤 실험(run)에서 이 모델이 나왔는지 연결 4) 운영 배포, A/B 테스트 등에서 사용 가능

5. **REST API 배포** — 로컬에서 실행 중인 MLflow 모델 서버(Port 5002)에 예측 요청을 보내는 HTTP POST 요청
```bash
curl -d '{"dataframe_split": {
  "columns": ["fixed acidity","volatile acidity","citric acid","residual sugar","chlorides","free sulfur dioxide","total sulfur dioxide","density","pH","sulphates","alcohol"],
  "data": [[7.0,0.27,0.36,20.7,0.045,45,170.0,1.001,3.0,0.45,8.8]]}}' \
  -H "Content-Type: application/json" -X POST localhost:5002/invocations
```
* `-d`: 데이터를 담아서 요청하는 curl 명령어. `"columns"`: 모델이 기대하는 입력 데이터의 열 이름(특성 feature). `"data"`: 각각의 입력 데이터 행(여기서는 와인 1잔에 대한 특성들). `localhost:5002/invocations`: 모델 서버가 실행중인 주소로 Post 요청을 보냄

```python
import requests
import pandas as pd

data = pd.DataFrame([
    [7.0, 0.27, 0.36, 20.7, 0.045, 45.0, 170.0, 1.001, 3.0, 0.45, 8.8]
], columns=["fixed acidity", "volatile acidity", "citric acid", "residual sugar", "chlorides",
            "free sulfur dioxide", "total sulfur dioxide", "density", "pH", "sulphates", "alcohol"])

response = requests.post(
    "http://localhost:5001/invocations",
    headers={"Content-Type": "application/json"},
    json={"dataframe_records": data.to_dict(orient="records")},
)
print(response.json())
```

---

## 핵심 요약
* 머신러닝 실험은 재현성·버전관리·협업·비교 문제 때문에 체계적인 관리가 필요하며, **MLFlow Tracking**은 실험을 Experiment/Run 단위로 구조화하고 Params/Metrics/Tags/Artifacts로 기록합니다.
* 실험 자동화는 `config.yaml` + `itertools.product`로 하이퍼파라미터 조합을 생성하고 `mlflow.start_run()` 내부에서 자동 로깅하는 방식으로 구현하며, Grid/Random Search와 Hyperopt로 튜닝을 자동화할 수 있습니다.
* 모델은 **Stage(None→Staging→Production→Archived)** 로 배포 단계를 관리하며, `mlflow.models.signature`로 입출력 형식을 검증하고, `mlflow models serve`로 REST API(`/invocations`)를 통해 손쉽게 서빙할 수 있습니다.
* 대규모 서비스에는 FastAPI 대신 MLServer(Seldon/Kserve 연동, Kubernetes 확장)를 사용하는 등, 규모에 맞는 서빙 엔진을 선택할 수 있습니다.
