# MLFlow 기본 구조와 세팅 정리

머신러닝 운영을 자동화하는 **MLOps**의 개념과, 그 실현 도구인 **MLFlow**의 4대 컴포넌트, 그리고 실제 설치/실습 과정을 정리한 문서입니다.

---

## 1. MLOps 개념 및 MLFlow의 역할

### MLOps란?
* **MLOps(Machine Learning Operations)란?** 머신러닝 모델의 개발과 운영을 통합하여 모델의 개발, 배포, 모니터링, 유지 보수 등의 전 과정을 자동화하고 효율적으로 하는 방법론
* MLOps를 사용하면 조직은 ML 수명 주기 전반의 프로세스를 자동화하고 표준화 가능
* MLOps는 협업 기능이며, 주로 데이터 사이언티스트, DevOps 엔지니어, IT로 구성 (Machine Learning ∩ DevOps ∩ Data Engineering의 교집합)

### 기존 머신러닝 프로세스의 한계
* **데이터 품질과 양에 대한 의존성 문제:** 머신러닝 모델은 대량의 고품질 데이터를 필요로 하는데, 대부분의 실제 데이터는 노이즈와 불완전한 데이터가 많아 모델의 성능을 저하시킴
* **특징 추출의 어려움:** 특정 도메인 전문가가 수동으로 특징을 추출해야 하므로 비용과 시간이 많이 소요
* **새로운 상황에 대한 대응 문제:** 머신러닝 모델이 학습한 데이터의 범위 내에서만 정확한 예측이 가능
* **모델 해석의 어려움:** 복잡한 모델일수록 내부 동작 방식을 이해하기 어려움
* **데이터 분포 변화에 대한 민감성:** 시간에 따라 변화하는 데이터의 분포에서 기존에 학습한 모델은 성능 저하를 일으킴
* 이러한 한계점을 극복하기 위해 **딥러닝과 MLOps 등의 새로운 접근법이 개발**

### MLOps의 필요성
* **반복 실험의 체계화:** 수많은 하이퍼파라미터 조합 실험, 데이터 전처리 방식, 피처 선택 변화, 실험 결과 비교를 위한 자동 기록
* **협업 효율성 향상:** 여러 팀원이 같은 실험을 반복하지 않음, 실험 결과를 다른 사람과 쉽게 공유, 버전 관리로 변경 사항 추적 용이
* **재현성 확보:** 실험이 시간이 지나도 재현 가능한 구조, 동일한 코드 + 데이터 + 환경 → 동일한 결과
* **추적성 확보:** 어떤 실험에 어떤 성능이 나왔는지 확인, 실험 조건·환경·결과가 모두 기록

### MLOps 주요 단계 (핵심 4단계)
```
데이터 준비 → 모델 학습 및 검증 → 모델 배포 → 모델 모니터링 관리
```
* 데이터 준비: 데이터 수집, 데이터 전처리, 특징 엔지니어링
* 모델 학습 및 검증: 모델 개발, 모델 학습, 모델 검증
* 모델 배포: 배포 준비, 배포, 배포 자동화
* 모델 모니터링 관리: 성능 모니터링, 데이터 드리프트 감지, 모델 업데이트

### 자동화 구조로서의 MLOps (CI / CT / CD)
| 단계 | 설명 |
| --- | --- |
| CI (Continuous Integration) | 코드 변경 시 자동 테스트 → 코드 품질 보장. 코드(모델 스크립트, 파이프라인) 변경 시 자동으로 통합 및 테스트, 데이터 전처리 코드, 모델 구성 변경 사항 포함 |
| CT (Continuous Training) | 데이터 변화에 따른 재학습 → 최신 모델 유지. 새로운 데이터가 들어오면 모델을 자동으로 재학습, 정기적 스케줄 또는 이벤트 기반으로 작동. 실험 추적 도구(MLFlow)와 함께 사용시 강력함 |
| CD (Continuous Delivery) | 모델 자동 배포 → 운영 자동화, 실시간 대응. 학습이 완료된 모델을 자동으로 서빙 환경에 배포, REST API 형태, 클라우드 환경 등 다양하게 적용 가능 |

### MLFlow란?
* MLFlow는 실험의 전체 생애주기(Lifecycle)를 관리하는 오픈소스 플랫폼
* 머신러닝 모델을 만들고 실험하고 배포하기까지의 과정을 체계적으로 재현 가능하게 관리해주는 도구
* **등장 배경:** 2018년도 Databricks에서 개발. 다양한 프레임워크·언어에 독립적이고, 설치도 간편한 오픈소스. 실험 기록, 모델 저장, 버전 관리, 배포를 하나로 지원
* MLFlow는 **"머신 러닝 실험의 Git"** 과 **"운영 자동화 도구"**

**비유:** 붕어빵 장사를 한다고 가정하면, 레시피(1) 반죽 100g/팥 100g/굽는 시간 3분 → 레시피(2) 반죽 50g/팥 100g/굽는 시간 3분 → 레시피(3) 반죽 150g/팥 50g/굽는 시간 2분처럼 여러 시행 착오를 거치며(=머신러닝 모델에서 많이 실험하는 행위, 예: 반죽/팥/굽는 시간 등 하이퍼파라미터) 가장 맛있는 붕어빵 레시피를 발견(=MLFlow를 통해 가장 성능 좋은 모델 선택)하고, 실험하면서 만들어진 붕어빵(=Artifact, 이미지, 로그 등)과 각 붕어빵에 대한 정보(맛, 재료, 칼로리 등 = MLFlow에서 기록하는 메타데이터)를 남겨, 팥·슈크림·김치 붕어빵 판매(=여러 모델 운영)까지 이어지는 과정을 자동화하는 것이 MLFlow의 역할

---

## 2. MLFlow의 주요 구성 요소

### MLFlow 4대 컴포넌트
MLFlow는 기존 머신러닝 운영의 문제점을 MLFlow의 핵심 기능 4가지로 해결합니다.

| 문제점 | MLFlow 컴포넌트 | 설명 |
| --- | --- | --- |
| 실험 추적의 어려움 | **Tracking (실험 기록)** | 모델 학습에 사용된 데이터를 로깅하여 실험을 체계적으로 추적 |
| 재현성 부족 | **Projects (코드 패키징)** | 코드, 환경 설정, 의존성 등을 표준화해 패키징해 실험의 재현성 보장 |
| 모델 배포의 복잡성 | **Models (모델 포맷 통일)** | 다양한 배포 환경에 맞게 모델을 패키징하고, 손쉽게 배포 지원 |
| 모델 관리의 비효율성 | **Model Registry (모델 버전 관리)** | 모델의 버전 관리, 상태 추적, 주석 추가 등을 중앙 집중식으로 모델 관리 |

### 1) MLFlow Tracking
* 머신러닝 실험과 실행을 체계적으로 관리하기 위한 API와 UI를 제공하는 컴포넌트
* Tracking은 머신러닝 실험의 실행으로 구성되며, 실험의 각 실행으로 발생하게 된다
* Tracking은 실험의 **재현성, 투명성, 효율성 향상**을 제공
* **주요 기능:** Parameter(파라미터, 모델 학습에 사용된 입력 데이터를 키-값 쌍으로 저장 및 추적), Metric(지표, 모델의 성능 지표 예: 정확도·손실 함수 값 등을 숫자 형태로 저장 및 추적), Artifact(산출물, 모델 파일·이미지·데이터 파일 등 실험 결과로 생성된 모든 형식의 출력 파일 저장 및 추적), 코드 버전(소스코드, 실행에 사용된 Git 커밋 해시 등을 기록하여 코드 버전 추적), Tags(실험을 설명하는 커스텀 라벨)

### 실험 기록을 어디에 저장할지 - 3가지 방식
| 방식 | 설명 |
| --- | --- |
| 1. 로컬 호스트 사용(기본값) | MLFlow는 기본값으로 현재 폴더에 실험 기록을 저장. 메타데이터 & 모델 파일 모두 내 PC 폴더에 저장 |
| 2. 로컬 데이터베이스 연결 | 기록은 로컬로 하고 실험 정보만 SQLite와 같은 데이터베이스에 저장. Tracking server 실행 시 `--backend-store-uri`로 DB 위치 지정. `mlruns` 폴더는 유지 및 저장 |
| 3. MLFlow 추적 서버 구성(원격 추적) | `mlflow server` 명령어로 실행. 여러 사람이 함께 실험할 수 있도록 서버를 하나 두고 모두가 여기에 실험 기록을 저장. Artifact는 S3, GCS, 서버 디렉토리 등에 저장. 서버를 통해 접근 권한 관리도 가능 |

### 2) MLFlow Projects
* Projects은 머신러닝 코드를 재사용하고 재현성 있게 패키징하기 위한 표준 형식을 제공
* 데이터 과학자들이 일관된 환경에서 코드를 실행하고 공유할 수 있음
* MLFlow Projects의 기능을 통해 머신러닝 실험의 재현성 향상
* **주요 특징:** 각 프로젝트의 구조는 코드, 데이터, 환경 설정 등을 포함하는 디렉토리 또는 Git 저장소로 구성. `MLproject` 파일 내에서 실행 가능한 스크립트와 파라미터를 정의. MLFlow Project는 다양한 실행 환경을 지원

### 3) MLFlow Models
* MLFlow Models는 머신러닝 모델을 다양한 환경에서 일관되게 패키징하고 배포하기 위한 표준 형식을 제공
* 이를 통해 모델의 재현성과 호환성을 보장하고 다양한 라이브러리와 프레임워크 지원
* **주요 특징:** 모델 포맷(MLFlow는 모델을 여러가지 'Flavor'로 저장하여, 다양한 도구와 호환성을 유지), 저장 구조(각 모델은 디렉토리 형태로 저장되며, 루트에는 'MLmodel' 파일이 위치하며, 파일의 메타데이터가 포함), 저장한 모델을 명령어 한 줄로 Serving할 수 있음

```yaml
artifact_path: model
flavors:
  sklearn:
    sklearn_version: 1.2.2
    pickled_model: model.pkl
  python_function:
    loader_module: mlflow.sklearn
```

### 4) MLFlow Model Registry
* MLFlow Model Registry는 머신러닝 모델의 전 생애 주기를 체계적으로 관리하기 위한 중앙 집중화된 저장소
* 이를 통해 모델의 생성, 버전 관리, 배포 단계 전환, 주석 추가 등 다양한 작업을 수행
* 모델을 코드처럼 버전 관리 + 승인 워크플로우가 적용될 수 있게 해주는 도구
* **주요 기능:** 모델 개발 추적(각 모델이 어떤 실험/실행에서 생성되었는지 추적 가능), 버전 관리(모델의 각 버전을 체계적으로 관리하여, 특정 버전의 모델을 재현하거나 비교 가능), 단계 전환(모델의 상태를 'Staging'에서 'Production'으로 전환하는 등 배포 단계 관리 지원), 주석 및 태그 추가(각 모델의 버전에 설명이나 태그를 추가 가능)

### MLFlow 아키텍처
* MLFlow 아키텍처란? 실험 기록, 모델 저장, 프로젝트 실행, 모델 배포 등 MLOps 전 과정을 지원하는 아키텍처
* MLflow Tracking Server(실험 기록) → Model Registry(모델 등록 및 상태 관리) → 리뷰 및 배포 연동(CD 툴이나 자동화 스크립트와 배포 연동) → Serving 및 활용(배포된 모델은 REST API로 실시간 서빙, 다운스트림 시스템에 연동되어 사용)
* 흐름: **실험 → 모델 관리 → 배포**

### MLFlow vs 다른 MLOps 도구
| 항목 | MLFlow | Kubeflow | Metaflow | W&B |
| --- | --- | --- | --- | --- |
| 출시 기업 | Databricks | Google | Netflix | Weights & Biases |
| 주요 기능 | 실험 추적, 모델 관리, 배포 | 파이프라인, 서빙, 전체 워크플로우 | 파이프라인, 스토리지 관리 | 실험 추적, 시각화, 협업 |
| 설치 난이도 | 쉬움(로컬도 가능) | 복잡함(Kubernetes 필요) | 중간(로컬 & 클라우드 모두 지원) | 쉬움(클라우드 기반) |

---

## 3. MLFlow 설치와 실습

### 가상환경 생성 및 MLFlow 설치
```powershell
python -m venv mlflow-env
.\mlflow-env\Scripts\Activate
```
```
tensorflow==2.15.0
numpy==1.26.0
pandas==2.2.3
typing-extensions==4.13.0
scikit-learn==1.6.1
mlflow==2.21.3
hyperopt==0.2.7
```

### MLFlow 버전 확인 및 UI 실행
```powershell
mlflow --version
mlflow ui              # UI 실행, http://127.0.0.1:5000에서 확인 가능 (ctrl 좌클릭으로 링크 이동 가능)
```

### MLFlow Tracking Server 실행
```powershell
mlflow server `
  --backend-store-uri sqlite:///mlflow.db `
  --default-artifact-root ./mlruns `
  --host 127.0.0.1 `
  --port 5000
```
* `--backend-store-uri`: 실험 메타데이터를 저장할 DB 지정
* `--default-artifact-root`: 모델 등록 등 Artifact를 저장할 기본 경로
* `--host`: 서버가 수신할 호스트 (기본값 0.0.0.0으로 설정하면 외부에서 서버 접근이 가능하므로, 보안에 유의해야함)
* `--port`: 서버가 수신할 포트 번호 지정(기본값: 5000)

### 간단한 분류 모델 로깅 실습 (Iris 데이터셋)
```python
import mlflow
import mlflow.sklearn
from sklearn import datasets
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import accuracy_score, precision_score, recall_score, f1_score

X, y = datasets.load_iris(return_X_y=True)
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

mlflow.set_experiment("MLflow Quickstart")   # 실험을 생성하지 않으면 Default 실험에 기록

with mlflow.start_run():
    params = {
        "solver": "lbfgs",
        "max_iter": 1000,
        "multi_class": "auto",
        "random_state": 8888,
    }
    lr = LogisticRegression(**params)
    lr.fit(X_train, y_train)

    y_pred = lr.predict(X_test)
    accuracy = accuracy_score(y_test, y_pred)

    mlflow.log_params(params)          # 하이퍼파라미터 전체 기록(딕셔너리 형태로 전달)
    mlflow.log_metric("accuracy", accuracy)   # 모델 평가 지표 기록
    mlflow.set_tag("Training Info", "Basic LR model for iris data")   # 메모 또는 상황 남기기

    signature = mlflow.models.infer_signature(X_train, lr.predict(X_train))   # 모델의 입력/출력 형식을 자동으로 인식해서 정형화
    model_info = mlflow.sklearn.log_model(
        sk_model=lr,
        artifact_path="iris_model",
        signature=signature,
        input_example=X_train,
        registered_model_name="tracking-quickstart",   # 모델 객체 저장, 저장 위치 지정, Model Registry 등록
    )
```

### MLFlow에 저장된 모델 불러와서 예측 수행
```python
loaded_model = mlflow.pyfunc.load_model(model_info.model_uri)
predictions = loaded_model.predict(X_test)

iris_feature_names = datasets.load_iris().feature_names
result = pd.DataFrame(X_test, columns=iris_feature_names)
result["actual_class"] = y_test
result["predicted_class"] = predictions
```

### CLI를 통한 실험 생성 및 확인
```powershell
$env:MLFLOW_TRACKING_URI = "http://127.0.0.1:5000"
mlflow experiments create --experiment-name "local_experiment"
# Created experiment 'local_experiment' with id 1
```
1. 새로운 터미널 열기 (Tracking Server 실행중)
2. 폴더 이동 및 가상 환경 활성화
3. 폴더 이동 후, 명령어를 통해 실험 생성
4. `localhost:5000` 접속 후, 실험 확인

### ML 프로젝트 생성 및 실행 실습
**실습 디렉토리 구조**
```
~/mlflow/
├── mlflow.db          # 실험 기록용 DB (Tracking 서버용)
├── mlruns/             # 실험 기록 자동 저장 디렉토리
└── src/
    ├── iris_train.ipynb   # 기존 노트북 코드
    └── iris_project/
        ├── train.py       # 실행 코드 (.ipynb에서 변환)
        ├── MLproject      # 프로젝트 정의 파일
        └── conda.yaml     # 실험 환경 정의 (선택)
```

**train.py (실험 실행 코드)** — Iris 데이터셋을 로지스틱 회귀로 학습. Argparse로 `-C` 파라미터 입력 받음. MLFlow로 파라미터, 정확도, 모델 자동 기록
```python
import argparse
import mlflow
import mlflow.sklearn
from sklearn.datasets import load_iris
from sklearn.linear_model import LogisticRegression
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score

parser = argparse.ArgumentParser()
parser.add_argument("--C", type=float, default=1.0)   # 커맨드 라인에서 파라미터를 받을 수 있게 함
args = parser.parse_args()

with mlflow.start_run():
    iris = load_iris()
    X_train, X_test, y_train, y_test = train_test_split(iris.data, iris.target, test_size=0.2, random_state=42)

    model = LogisticRegression(C=args.C, max_iter=200)
    model.fit(X_train, y_train)
    preds = model.predict(X_test)

    mlflow.log_param("C", args.C)                              # 파라미터, metric, model 로그
    mlflow.log_metric("accuracy", accuracy_score(y_test, preds))
    mlflow.sklearn.log_model(model, "model")
    print("Accuracy:", accuracy_score(y_test, preds))
```

**MLproject** — 프로젝트 실행 방법 정의. `mlflow run` 실행 시 이 파일 기준으로 실행
```yaml
name: IrisProject
conda_env: conda.yaml
entry_points:
  main:
    parameters:
      C: {type: float, default: 1.0}
    command: "python train.py --C {C}"
```

**conda.yaml (선택)** — 실행 환경 자동 구성
```yaml
name: iris_env
channels:
  - defaults
dependencies:
  - python=3.10
  - scikit-learn
  - pip
  - pip:
    - mlflow
```

**프로젝트 실행**
```powershell
mlflow run src/iris_project --env-manager=local -P C=0.5
```
* `--env-manager=local` 명령어를 사용하여 conda 없이 현재 가상환경 사용
* `-P C=0.5`: 파라미터 C값 설정
* `http://localhost:5000` 접속 후, 실험 결과 시각화. 이후에도 추가로 파라미터 C값을 변경해서 생성된 결과를 확인해볼 수 있음
* Experiment ID와 Run_id가 일치 → 로컬 파일에 성공적으로 저장되었다는 것을 알 수 있음
* MLFlow 여러 C값을 변경하여 비교 가능

---

## 핵심 요약
* **MLOps**는 데이터 준비 → 모델 학습/검증 → 배포 → 모니터링의 전 과정을 CI(코드 통합)/CT(재학습)/CD(배포 자동화) 구조로 자동화·표준화하는 방법론입니다.
* **MLFlow**는 이 MLOps를 실현하는 오픈소스 플랫폼으로, **Tracking(실험 기록)·Projects(코드 패키징)·Models(모델 포맷 통일)·Model Registry(버전 관리)** 4대 컴포넌트로 구성됩니다.
* 실험 기록은 로컬 호스트/로컬 DB 연결/원격 Tracking Server 3가지 방식으로 저장할 수 있으며, `mlflow.log_param/log_metric/log_model`로 파라미터·지표·모델을 기록하고 `mlflow ui`로 결과를 비교합니다.
* `MLproject` + `conda.yaml`로 실행 환경과 커맨드를 표준화해두면, `mlflow run`으로 하이퍼파라미터를 바꿔가며 재현 가능한 실험을 반복할 수 있습니다.
