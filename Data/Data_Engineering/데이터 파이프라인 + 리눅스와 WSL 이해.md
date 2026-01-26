# 데이터 파이프라인 + 리눅스와 WSL 이해 정리

데이터 엔지니어링의 출발점인 **데이터 파이프라인**의 개념/설계와, 실습 환경으로 사용하는 **리눅스와 WSL**, 그리고 기본 리눅스 명령어를 정리한 문서입니다.

---

## 1. 데이터 파이프라인

### 데이터의 시대
* **데이터가 중요해진 이유:** 빅데이터(데이터의 양(Volume), 다양성(Variety), 증가 속도(Velocity) 향상 + 신뢰성(Veracity), 가치(Value)). 데이터를 수집, 가공, 활용할 수 있는 기술의 대두 ⇒ **데이터 파이프라인**
* "데이터는 미래 경쟁력을 좌우하는 21세기의 원유" - 미국 시장조사 기관 '가트너'

### 데이터 사이언스와 엔지니어링
* **데이터 엔지니어의 주요 활동:** 데이터를 안정적으로 수집하고 가공하여 전달, 분석과 모델링이 가능하도록 데이터 흐름을 자동화, 신뢰성 있고 재사용 가능한 파이프라인 구축
* 주로 하는 일: 다양한 시스템에서 데이터 수집, 정제 및 변환(ETL/ELT 설계)
* **"좋은 모델은 좋은 파이프라인에서 나온다"** — 데이터 엔지니어는 모델의 품질을 받쳐주는 기반을 설계합니다.

### 데이터 파이프라인이란?
* 데이터를 추출하고 정제하고 저장, 분석, 시각화하는 일련의 자동화 과정
```
데이터 추출 → 데이터 가공 → 데이터 저장
```

### OLAP와 OLTP
| 구분 | 설명 |
| --- | --- |
| OLTP (Online Transaction Processing) | 운영 데이터 처리 시스템. 실시간 트랜잭션(주문, 결제, 예약 등) 처리. 행(Row) 단위 저장 구조. 빠른 입력, 수정, 삭제에 최적화 |
| OLAP (Online Analytical Processing) | 분석 데이터 처리 시스템. OLTP 등에서 수집된 데이터를 기반으로 통계·리포트 분석. OLAP 시스템은 분석 성능을 위해 컬럼 기반 저장 구조를 사용하는 경우가 많음. 집계, 요약, 예측 분석에 최적화 |

### ETL vs ELT
| 구조 | 순서 | 설명 |
| --- | --- | --- |
| ETL (Extract, Transform, Load) | 추출 → 가공 → 저장 | 데이터를 가공한 후 저장. 전통적인 방식 |
| ELT (Extract, Load, Transform) | 추출 → 저장 → 가공 | 데이터를 저장한 후 가공. 클라우드 시대에 많이 쓰이는 방식 |

| 항목 | ETL (전통 방식) | ELT (클라우드 중심) |
| --- | --- | --- |
| 순서 | 추출 → 가공 → 저장 | 추출 → 저장 → 가공 |
| 환경 | 온프레미스 DW | 클라우드, 데이터 레이크 |
| 장점 | 정제된 데이터 보장 | 연산 가공, 확장성 우수 |
| 단점 | 느림, 유연성 부족 | 처리 비용 증가 가능성 |

**"ETL은 정제 우선, ELT는 속도와 유연성 중심" — 환경에 따라 적합한 방식 + 혼합된 방식 선택**

### 데이터 처리 방식 - 배치와 스트리밍
| 항목 | 배치 처리 (Batch Processing) | 스트리밍 처리 (Data Stream Processing) |
| --- | --- | --- |
| 처리 방식 | 일정 주기로 대량 처리 | 실시간으로 지속 처리 |
| 예시 | 하루 1회 통계 리포트 | 실시간 사용자 클릭 분석 |
| 장점 | 안정적, 대규모 처리 적합 | 즉시 대응, 실시간 분석 가능 |
| 단점 | 지연 발생 가능 | 복잡한 설계 필요 |

**"배치는 정확성과 안정성 중심, 스트리밍은 실시간성과 즉시성 중심"** — 데이터의 속도·목표에 따라 적절한 방식을 선택해야 합니다.

### 데이터 파이프라인의 기본 구조
```
데이터 소스 → 수집 → 가공 → 저장 → 분석/제공
```

---

## 2. 데이터 저장소

### 데이터 저장소의 중요성
* 저장소는 분석을 위한 인프라. 데이터를 단순히 저장하는 것이 아니라 분석·활용을 위한 설계가 필요
* 저장소에 따라 처리 방식과 유연성이 달라짐. 파이프라인에서 중요한 핵심 축

### 데이터 웨어하우스 vs 데이터 레이크 vs 데이터 마트
* **데이터 웨어하우스:** 정형 데이터를 저장하는 구조, 기본 저장구조
* **데이터 레이크:** 원본 데이터를 저장하는 구조, 수집 후 재가공하여 활용
* **데이터 마트:** 특정한 목적을 위해 데이터 웨어하우스의 내용을 다시 추출하여 저장
```
원천 데이터 --ETL--> 장기보관 데이터 --ETL--> 분석용 데이터 --SQL-->
```

| 구분 | 데이터 레이크 (Data Lake) | 데이터 웨어하우스 (Data Warehouse) |
| --- | --- | --- |
| 주요 목적 | 다양한 원천 데이터를 원형 그대로 저장, 추후 분석·활용을 위한 유연한 데이터 저장소 | 비즈니스 의사결정을 위한 정제된 데이터 저장소, 리포팅과 분석 업무 최적화 |
| 데이터 형태 | 정형, 반정형, 비정형 데이터 모두 수용 가능(예: 로그, 이미지, 오디오, JSON 등) | 정형 데이터 위주(관계형 테이블 기반, 스키마 존재) |
| 데이터 품질 요구 | 품질 보장보다 유연성과 포괄성 중시, 노이즈 포함 가능성 존재 | 정합성·신뢰성 중요, 높은 품질 기준 충족 필요 |
| 비용 및 확장성 | 상대적으로 저렴하고 확장성 높음(HDFS, S3 등 파일 기반 저장소 사용) | 저장 비용이 상대적으로 높음(고가의 RDBMS, 분석 엔진 활용) |
| 분석 방식 | 머신러닝, AI, 통계 분석 등 고급 분석에 활용, 탐색적 분석 중심 | 표준화된 리포트 및 대시보드 중심, 운영 보고서, 경영 분석 등 |
| 운영 및 거버넌스 | 데이터 거버넌스 체계 수립 필요, 메타데이터 관리 및 품질 통제 체계 중요 | 엄격한 데이터 품질관리 체계, 보안·접근 제어 체계 정비 |
| 스키마 적용 시점 | Schema-on-Read: 조회 시점에 스키마 적용, 다양한 데이터 활용 가능 | Schema-on-Write: 적재 시점에 스키마 적용, 정형화된 구조 필수 |

* **데이터 레이크하우스:** 데이터 레이크(정형+비정형 데이터 모두 저장, schema-on-read)와 데이터 웨어하우스의 장점을 결합. 대용량 로그/센서 데이터도 수용 가능

### 데이터 처리/저장/모니터링 도구
| 분류 | 도구 | 특징 |
| --- | --- | --- |
| 수집 | **Kafka** | 분산 메시지 큐 시스템. 대용량 데이터를 빠르고 안정적으로 전달. 실시간 스트리밍 수집에 강점 |
| 처리(가공) | **Spark** | 대규모 배치 처리 프레임워크. ETL/머신러닝 통합 가능. DAG 기반 처리로 안정성과 확장성 확보 |
| 처리(가공) | **Flink** | 스트리밍 처리 전문 프레임워크. 이벤트 기반 실시간 분석에 최적화. 상태 기반 연산 및 복잡한 처리 가능 |
| 저장 | RDBMS(PostgreSQL, Oracle 등) | 고급 기능을 지원하는 오픈소스 관계형 데이터베이스. 정형 데이터 저장에 적합 |
| 저장 | Elasticsearch | 실시간 검색과 분석에 강력한 NoSQL DB. 로그, 텍스트 분석, 모니터링 등 다양한 사용처 |
| 저장 | Hadoop (HDFS) | 대용량 비정형 데이터 저장용 HDFS 기반 저장소. 정형·비정형 데이터 통합 저장 가능 |
| 모니터링 | Airflow | 워크플로우 스케줄러(DAG 기반). 파이프라인의 각 단계를 자동화 및 모니터링 |
| 모니터링 | Grafana | 실시간 시각화 대시보드. 다양한 데이터 소스와 연결 가능(Prometheus, Elasticsearch 등) |
| 모니터링 | Prometheus | 시계열 기반 모니터링 도구. 지표 수집, 알림, 시각화 연동 기능 제공 |
| 분석 | Power BI / Tableau | 데이터를 시각적으로 분석하거나 리포트를 만들기 위한 도구. 엑셀의 Pivot 기능이나 시각화 기능과 같은 기능을 좀 더 전문적으로 다루는 도구 |

---

## 3. 데이터 파이프라인 설계

* 파이프라인 설계는 환경마다 다르기에 정답은 없다. (온프레미스 vs 클라우드) 요구사항에 따라 다양각색으로 구현하나, 실시간 수집이 필요한지 여부에 따라 파이프라인 설계 구분 가능
  * 수집 데이터 배치 처리 → 배치 Only
  * 데이터 실시간 처리 → 배치+스트림 or 스트림

### 람다(Lambda) 아키텍처 & 카파(Kappa) 아키텍처
* 실시간 수집이 필요한 경우 참조할 수 있는 아키텍처가 존재. 대표적으로 람다(Lambda) 아키텍처와 카파(Kappa) 아키텍처가 존재 (2011년에 제시된 아키텍처)

**람다 아키텍처:** 실시간 수집이 필요할 경우 배치 처리와 스트림 처리를 모두 이용 가능
```
Client/Mobile Client/Client → Queue → Batch Layers(Batch Engines) → Serving Layers(Serving Engines) → Query
                                    → Stream Layers(Stream Engines) ↗
Raw Data → Process Data
```
* **배치 Layer & 스트림 Layer:** 배칫 Layer에 저장된 데이터가 특정 기준 데이터라면 스피드 Layer에는 당일 데이터가 저장/정제되어 저장하는 공간. 배치 Layer에서 데이터 갱신이 완료되면 스트림 Layer는 그 이후 데이터부터 저장 및 정제. 람다 아키텍처는 컨셉만 제공
* **Serving Layer:** 배치 Layer에 저장된 데이터를 빠르게 보여주기 위한 서비스 계층. 사용자가 쿼리할 수 있도록 함. 필요에 따라 스피드 Layer에 있는 데이터를 결합하기도 함

**카파 아키텍처:** 배치 Layer를 제거하되 배치 Layer에서 하던 일을 모두 스피드 Layer에서 수행하는 구조(전처리 후 필요한 테이블로 재구성)
* 데이터 소스는 주로 메시지 큐를 의미. 메시지 큐에는 여러 솔루션이 존재하지만 Kafka를 개발한 Jay Kreps가 만든 카파 아키텍처에서 소스는 사실상 Kafka의 Cluster를 의미
* 카파 아키텍처에서 대표적으로 데이터는 Kafka로 수집함. 그러나 일반적으로 배치 파이프라인도 많이 활용
* 람다 아키텍처나 카파 아키텍처만 가능한가? — 구조화된 아키텍처는 참고를 위한 아키텍처일 뿐 모든 데이터를 해당 아키텍처 기반의 파이프라인으로 만들 필요는 없음. 아키텍처 수용 여부는 파이프라인마다 데이터의 활용 요건으로 결정. 데이터 활용 요건을 분석 후 아키텍처를 따를지 어떤 데이터 뷰를 활용할지 결정

### 데이터 파이프라인 전체 구조
```
Sources → Ingestion and Transformation → Storage → Historical → Predictive → Output
                                                                              ↓
                                                          Metadata Management / Quality and Testing / Entitlements and Security / Observability (지원 시스템)
```
* **Sources(데이터 소스):** 데이터가 유입되는 소스 (고객정보, 제품정보, 행동 정보 로그 등)
* **Ingestion and Transformation(데이터 수집 및 변환):** 데이터 소스에서 내용을 추출하고 저장에 적절한 형태로 변환
* **Storage(데이터 저장):** 데이터를 저장하는 시스템 (예: 고객ID/제품ID/제품 카테고리/가격/구매여부 테이블 → Data Warehouse)
* **Historical(과거 데이터 분석):** 과거 데이터를 활용한 분석 단계 (Data Warehouse 기반 "금주 인기 제품", "실시간 인기 제품" 등 도출)
* **Predictive(예측 분석):** 데이터를 바탕으로 머신러닝 및 예측을 하는 단계 (예: 유저별 추천 제품 - User A: 99, 17, 33 / User B: 111, 26, 54)
* **Output(출력):** 데이터 분석 결과를 시각적으로 표현하거나 시스템에 제공
* **지원 시스템(Metadata Management, Quality and Testing, Entitlements and Security, Observability):** 데이터 파이프라인을 관리하고 보완 및 모니터링 하는 시스템

---

## 4. 리눅스의 개념

* **리눅스(Linux):** 무료(Free) 유닉스 개념. 유닉스와 거의 동일한 운영체제이면서 무료, 어떤 면에서는 유닉스보다 뛰어남
* **유닉스(UNIX):** 리눅스가 탄생하기 이전 운영체제(OS). 지금도 많이 사용되는 운영체제 중 하나이지만 높은 비용 지불 필요 (IBM의 AIX, HP의 HP/UX, 오라클의 Solaris, DEC의 Digital Unix, SCO의 SCO Unix 등)

### 리눅스의 구성
* **커널(Kernel):** 운영체제의 핵심 구성 요소로, 하드웨어와 응용 프로그램 사이를 중재하는 역할을 함
* **리눅스 커널의 역사:** 리누스 토르발스(Linus Torvalds)가 1991년, 리눅스 커널 0.01 버전을 개발. 1992년, 0.02 버전 소스를 공개하며 오픈소스 운동 본격화 → 리눅스의 시작. 리눅스 배포판은 토르발스가 만든 커널 + 다양한 오픈소스 프로그램으로 구성

### 리눅스의 장점
* **무료 & 오픈소스(Free & Open Source):** 누구나 자유롭게 사용, 수정, 배포 가능. 라이선스 비용 부담 없이 교육/개발에 적합
* **가볍고 빠른 성능:** 구형 하드웨어에서도 작동 가능, 불필요한 GUI 없이 CLI 중심 운영 가능
* **서버로서의 점유율:** 전 세계 웹 서버의 70% 이상이 리눅스 기반. 클라우드, 데이터센터, 웹 호스팅에서 필수 OS
* **개발 환경:** Git, Docker, Python, Node.js 등 대부분 리눅스 친화적. 패키지 설치, 자동화, 백엔드 개발에 최적

### 우분투 리눅스(Ubuntu Linux)
* 데비안 기반 배포판, 다양한 플랫폼(Desktop, Server, IoT 등)
* 릴리스 주기: 일반 버전(6개월), LTS 버전(2년)

---

## 5. WSL (Windows Subsystem for Linux)

* **WSL이란?** Windows 환경에서 리눅스를 실행할 수 있도록 도와주는 도구. 윈도우에서 리눅스를 가상 머신 없이 실행. 명령어, 파일 시스템, 리눅스 도구 사용 가능
* **WSL의 장점:** 별도 리눅스 컴퓨터가 없어도 Windows에서 바로 리눅스 사용 가능, Docker, Python, Git 등 리눅스 친화 도구 활용이 쉬움, VM 대비 가볍고 빠르며, 설치가 간편함(재부팅 없이 가능)

### WSL 버전 별 차이점
| 항목 | WSL 1 | WSL 2 |
| --- | --- | --- |
| 핵심 구조 | Windows 커널 위 리눅스 API 구현 | 가상화된 리눅스 커널 내장 |
| 성능 | 빠른 파일 접근 | 높은 시스템 호환성 |
| Docker 사용 | 불가능 | **가능** |
| 네트워크 | 윈도우와 동일 | 분리된 IP 사용(WSL 네트워크) |

**Docker를 이용하려면 꼭 WSL 2여야 합니다!**

### WSL 설치 방법
```powershell
# 필수 Windows 기능 활성화 (WSL1 핵심 구성요소 설치 / 가상화 기반 플랫폼 활성화하기)
dism.exe /online /enable-feature /featurename:Microsoft-Windows-Subsystem-Linux /all /norestart
dism.exe /online /enable-feature /featurename:VirtualMachinePlatform /all /norestart
```
1. Microsoft Store를 통해서 "Ubuntu 22.04.5 LTS" 다운로드
2. 기본 사용자 계정 생성 (ID/PW 설정)
3. (docker 사용 및 최신 호환을 위해) WSL2로 설정하기: `wsl --set-default-version 2`
4. 전부 설치가 끝난 후 기본 배포판으로 설정: `wsl --set-default Ubuntu-22.04`

**현재 설치된 wsl 목록 확인하기**
```powershell
wsl --list --verbose

# 만약 다른 버전이 있을 경우 (예시: Ubuntu-24.04)
wsl --terminate Ubuntu-24.04     # bash
wsl --unregister Ubuntu-24.04

Get-AppxPackage *Ubuntu* | Remove-AppxPackage   # powershell
```

### VS Code와 WSL 연동
1. VS code 실행 후 extension 확인 (WSL install 하기)
2. `Ctrl+Shift+P` → 'WSL' 입력 → "WSL: Connect to WSL in New Window"
3. 우분투 환경 다운로드 후 연결. 폴더 경로가 우분투 환경과 동기화 된 것을 확인 가능

### 리눅스 시작과 종료
* Ubuntu 22.04를 통해 실행 가능 (VS code를 통해서도 가능)
* WSL 종료: `exit`
* `poweroff`, `reboot`, `shutdown` 사용 불가 — WSL 특성상 자체적 부팅 구조가 아니기 때문에 해당 명령어는 무시됨

---

## 6. 리눅스 기본 명령어

### root 사용자란?
* **root 사용자:** 최고 권한(Superuser)을 가진 계정. 시스템의 모든 파일, 설정, 사용자 계정 등에 제약 없이 접근 가능. Windows의 Administrator에 대응되는 개념

| 작업 | 일반 사용자 | root 사용자 |
| --- | --- | --- |
| 시스템 파일 수정 | X | O |
| 새로운 프로그램 설치 | X | O |
| 다른 사용자 계정 관리 | X | O |
| 커널 모듈 수정 | X | O |

* **root 권한 이용시 주의점:** 실수로 중요한 시스템 파일 삭제 가능, 잘못된 명령어로 OS 자체를 망가뜨릴 위험, 외부 공격자가 root 권한을 얻으면 시스템 전체를 장악 가능. 그래서 보통은 sudo 명령어로 필요한 작업만 권한을 임시로 위임받아 실행함

### root 사용자 권한 전환
```bash
sudo -i              # root 전환, 현재 계정에서의 비밀번호 입력
# 권한 확인 후 exit를 통해 기존 계정으로 복귀
```

### 리눅스 명령어 모음

**파일 및 디렉토리 관리**
| 명령어 | 설명 | 예시 | 약자 의미 |
| --- | --- | --- | --- |
| pwd | 현재 디렉토리 경로 확인 | pwd | Print Working Directory |
| ls | 현재 디렉토리 목록 보기 (`-l` 자세히, `-a` 숨김 포함) | ls -l, ls -a | List |
| cd | 디렉토리 이동 (`..`: 상위 디렉토리) | cd ~, cd .. | Change Directory |
| mkdir | 새 폴더 생성 (여러 개 가능: mkdir a b c) | mkdir my_folder | Make Directory |
| rmdir | 빈 폴더 삭제 (폴더가 비어 있어야 함) | rmdir my_folder | Remove Directory |
| rm -r | 폴더 포함 삭제 (실수 방지 주의 필요) | rm -r my_folder | Remove (recursive) |

**파일 생성, 편집, 복사, 삭제**
| 명령어 | 설명 | 예시 | 비고 |
| --- | --- | --- | --- |
| touch | 빈 파일 생성 | touch test.txt | 수정 시간 갱신용으로도 사용됨 |
| echo | 문자열 출력/파일에 저장 (`>>`: 이어쓰기) | echo "Hello" >> hi.txt | 메아리처럼 출력 |
| cat | 파일 내용 출력 | cat file.txt | 대용량 파일은 less, more 추천 |
| head, tail | 처음/끝 일부 출력 | head -n 3 file.txt | 로그 확인에 유용 |
| cp | 파일/폴더 복사 (`-r`: 디렉토리 전체 복사) | cp a.txt b.txt / cp -r dir1 dir2 | |
| mv | 이동 또는 이름 변경 | mv a.txt new.txt | 파일 위치 이동에도 사용 |
| rm | 파일 삭제 (`rm -rf`: 위험!) | rm test.txt | |

**검색과 필터링**
| 명령어 | 설명 | 예시 | 약자 의미 |
| --- | --- | --- | --- |
| grep | 특정 문자열 검색 | grep "ERROR" log.txt | Global Regular Expression Print, 로그 분석에서 필수 |
| find | 파일 검색 | find . -name "*.txt" | 위치별 조건 검색, Find |
| history | 명령어 기록 확인 | history | 이전 명령 복기, History |

**시스템 정보 및 프로세스**
| 명령어 | 설명 | 예시 | 약자 의미 |
| --- | --- | --- | --- |
| ps aux | 전체 프로세스 확인 | ps aux \| grep python | PS-process status, A-All users, U-User-oriented format, X-No controlling terminal |
| kill | 프로세스 종료 (`-9` 옵션은 강제 종료) | kill 1234 | Kill |
| top | 실시간 자원 모니터링 (htop은 GUI 버전) | top, q로 종료 | Top of processes |
| uptime | 시스템 가동 시간 | uptime | Up Time |
| whoami | 현재 사용자 확인 | whoami | Who am I |
| hostname | 호스트명 확인 | hostname | Host Name |

**사용자 권한 및 보안**
| 명령어 | 설명 | 예시 | 약자 의미 |
| --- | --- | --- | --- |
| sudo | 관리자 권한 명령 | sudo apt update | Superuser Do |
| sudo -i | root 전환(Interactive shell) | sudo -i, su - root | 환경 유지하며 root 전환, su-root는 root pw 지정 후 이용 |
| chmod | 파일 권한 변경 | chmod 755 run.sh | Change Mode, 실행 권한 등 설정 조심해서 사용 |
| chown | 파일 소유자 변경 | sudo chown user file.txt | Change Owner, 조심해서 사용 |

**CLI 환경 관련 유용 명령**
| 명령어 | 설명 | 예시 | 비고 |
| --- | --- | --- | --- |
| clear | 터미널 화면 초기화 | clear | 화면 정리용 |
| man | 매뉴얼 보기 | man grep, q로 종료 | 대부분 명령어 지원, Manual |

### chmod (Change Mode) 상세
* **리눅스 권한 구조:** 리눅스에서는 각 파일/디렉토리에 대해 다음 3가지 주체에 대한 권한을 따로 설정할 수 있다.
  * **주체:** u(user, 파일의 소유자), g(group, 파일이 속한 그룹), o(other, 그 외 사용자)
  * **권한:** r(read, 내용 보기 가능), w(write, 수정·삭제 가능), x(execute, 실행 가능, 디렉토리 접근 포함)

```bash
touch sample.sh
echo 'echo "Hello, Linux!"' > sample.sh
cat sample.sh
```
`sample.sh`는 644 권한을 가지고 있다 (사용자는 읽기+쓰기, 그룹은 읽기 전용, 기타 사용자도 읽기 전용).

```
   U          G          O
 r w x      r w x      r w x
 r w -      r - -      r - -
 4 2 0      4 0 0      4 0 0
```

```bash
chmod +x sample.sh
```
사용자는 `sample.sh`에 실행 권한을 추가해서 755의 권한을 주게 됨.
```
   U          G          O
 r w x      r w x      r w x
 r w x      r - x      r - x
 4 2 1      4 0 1      4 0 1
```

---

## 핵심 요약
* 데이터 파이프라인은 **추출→가공→저장(ETL)** 또는 **추출→저장→가공(ELT)** 구조로, 배치/스트리밍 처리 방식과 데이터 웨어하우스/레이크/레이크하우스 저장소를 조합해 설계합니다.
* 실시간 처리가 필요한 경우 **람다 아키텍처**(배치+스트림 계층 병행) 또는 **카파 아키텍처**(스트림 계층 단일화, Kafka 중심)를 참고하며, 전체 구조는 Source→수집/변환→저장→분석→출력과 이를 지원하는 메타데이터/품질/보안/모니터링 체계로 구성됩니다.
* **WSL 2**는 리눅스 커널을 가상화해 Windows에서 Docker 등 리눅스 도구를 그대로 쓸 수 있게 해주며, `wsl --set-default-version 2`로 설정하고 VS Code와 연동해 사용합니다.
* 리눅스는 `pwd/ls/cd/mkdir/rm`, `grep/find`, `ps aux/kill/top`, `sudo/chmod/chown` 등 기본 명령어로 파일·프로세스·권한을 다루며, `chmod`의 `rwx(4/2/1)` 조합으로 사용자/그룹/기타 사용자별 권한을 세밀하게 제어합니다.
