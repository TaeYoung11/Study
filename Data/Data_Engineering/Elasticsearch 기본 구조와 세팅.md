# Elasticsearch 기본 구조와 세팅 정리

분산 검색 및 분석 엔진 **Elasticsearch**의 개요, Lucene과의 관계, 클러스터/노드/샤드로 구성되는 아키텍처, Docker 기반 설치, 그리고 REST API를 활용한 Document CRUD를 정리한 문서입니다.

---

## 1. Elasticsearch 개요

### 정보 검색이란?
* 대규모 데이터 속에서 사용자가 원하는 정보를 찾아 제공하는 기술. 웹 문서, 이미지, 동영상, 연구 논문 등 다양한 데이터 유형을 대상으로 함. Google, Microsoft 등 글로벌 기업에서 핵심적으로 활용
* **검색 시스템에서의 요구사항:** 빠르고 정확한 검색 결과, 사용자 경험, 보안 및 개인 정보, 확장성 및 유지보수

### 정보 검색의 핵심 기술
* **데이터 수집(Data Collection):** 웹 크롤링(Crawling, 웹 문서를 자동으로 수집하여 색인하는 방식), 스크래핑(Scraping, 특정 웹사이트에서 데이터를 추출하여 가공하는 방식)
* **데이터 저장(Data Storage) - 역색인(Inverted Index):** 키워드와 해당 키워드가 포함된 문서 정보를 저장하는 구조. 문서 검색 시 키워드를 기반으로 관련 문서를 빠르게 찾아줌
* **검색 알고리즘(Search Algorithm):** TF-IDF(Term Frequency-Inverse Document Frequency, 특정 키워드가 문서 내에서 가지는 상대적 중요도를 평가하는 기법), BM25(TF-IDF를 개선한 가중치 기반 검색 알고리즘으로, 검색 정확도를 높임)

### 기존 RDB 검색의 문제점
* "삼성 블루투스 이어폰"이라는 타이틀을 가진 상품 검색: `SELECT title FROM product WHERE title LIKE '%삼성 블루투스 이어폰%'`
* **문제점:** 쿼리가 복잡해짐, 성능(Performance)에 문제 발생(대량 데이터에서 LIKE 연산 비효율), 스펠링 오류·유사 검색 불가

### Elasticsearch란?
* 강력한 오픈소스 검색 및 분석 엔진. 수평적 확장, 안정성, 쉬운 관리를 위한 설계
* Apache Lucene 기반이며, Elastic Stack의 일부로 Elastic Stack은 Logstash, Beats, Kibana를 포함

### Elasticsearch와 Lucene의 관계
* **Elasticsearch:** 분산 검색 엔진, 데이터를 저장하고 빠르게 검색. Lucene을 내부 엔진으로 사용
* **Lucene:** 검색 라이브러리, Elasticsearch의 핵심 검색 기능 담당. Java로 작성된 고성능 텍스트 검색 엔진 라이브러리. 검색·색인(indexing) 기능을 제공. 단독으로 사용하려면 직접 애플리케이션 개발 필요
* **Elasticsearch vs Lucene:** Elasticsearch는 Lucene을 기반으로 동작하며 REST API 및 분산 환경 지원

### Lucene이란?
* 검색용 서비스의 핵심: 루씬. Elasticsearch에서의 검색관련 API의 대부분은 루씬 기반의 검색 API에서 출발
* 분산 처리, 캐싱, 샤드 기반 검색 등의 추가 기능을 제공하여 대규모 데이터 검색을 최적화

```
Elasticsearch index
  ├─ primary Elasticsearch shard (Lucene index) ─┬─ Lucene segment
  │                                                └─ Lucene segment
  └─ replica Elasticsearch shard (Lucene index) ─┬─ Lucene segment
                                                   └─ Lucene segment
```

### Lucene의 segment
* **세그먼트(Segment)란?** Lucene에서 색인된 문서들을 저장하는 최소 단위. 하나의 샤드(Shard)는 여러 개의 세그먼트로 구성. 세그먼트는 한 번 생성되면 수정되지 않음(Immutable) — 문서가 업데이트되면 새로운 세그먼트가 생성, 삭제된 문서는 "삭제 플래그"로 처리
* **세그먼트의 장점:** 동시성 확보(여러 세그먼트에서 동시에 검색 가능), 빠른 색인 처리(기존 세그먼트를 수정하지 않고 새로운 세그먼트 추가), 안정적인 검색(검색 시 기존 세그먼트는 그대로 유지되므로, 검색 중단 없이 색인 가능)

### Elasticsearch 데이터 파이프라인 구조
```
Data Aggregation & Processing → Indexing & Storage(elasticsearch) → Analysis & Visualization(kibana)
Data Collection → Buffering(beats/kafka) → Data Aggregation & Processing(logstash) → Indexing & Storage(elasticsearch) → Analysis & Visualization(kibana)
```

### Elasticsearch 특징
* **분산 구조(Distributed Nature):** Elasticsearch는 클러스터 내 사용 가능한 모든 노드에 데이터를 자동으로 분산하여 준실시간으로 대량의 데이터를 처리 가능하도록 함
* **전문 검색(Full-Text Search):** Elasticsearch는 고급 전문 검색 기능을 지원하며, HTTP 웹 인터페이스와 스키마가 없는 JSON 문서를 사용
* **확장성(Scalability):** 수백 대에서 수천 대의 서버로 확장 가능, 구조화된 데이터 및 비정형 데이터 수 페타바이트 규모까지 처리 가능
* **유연성(Flexibility):** 다양한 소스로부터의 이질적인 데이터 유형을 색인할 수 있으며, 복잡한 검색 기능 제공

### Elasticsearch 활용
* **기업 검색(Enterprise Search):** 기업에서 전체 디지털 콘텐츠를 색인하여 내부 네트워크 또는 웹사이트에서 고급 검색 기능을 제공하는 데 활용
* **로그 수집 및 분석(Logging and Log Analysis):** Logstash 및 Kibana와 함께 사용하여 로그 데이터를 분석하고 IT 운영, 성능, 상태 모니터링에 대한 인사이트 제공
* **보안 정보 및 이벤트 관리(SIEM):** 조직에서 보안 데이터를 실시간으로 분석하고 시각화하여 위협 감지 및 규정 준수 관리에 활용
* **데이터 분석(Data Analysis):** 빅데이터 분석을 위해 사용되며, 대량 데이터를 빠르고 다양한 방식으로 탐색할 수 있도록 지원
* **개인화 및 추천 시스템(Personalization and Recommendations):** 이커머스 웹사이트 등에서 사용자의 행동과 상호작용을 분석하여 맞춤형 상품 추천 및 동적 콘텐츠 제공에 활용

### 그 밖의 Elasticsearch 특징
* 유연한 JSON 데이터 관리(스키마리스 방식으로 다양한 데이터 저장), 정밀한 검색 및 필터링(다양한 검색 옵션과 필터 기능 제공), 다양한 검색 쿼리 지원(복잡한 검색, 정렬, 그룹화 기능 제공)
* 다양한 클라이언트 지원(Java, .NET, PHP, Python 등 SDK 제공), 확장성과 안정성(오토스케일링, 데이터 백업 및 복원 기능 제공), Kibana 데이터 시각화(리포팅 및 대시보드 활용 가능)

---

## 2. Elasticsearch 기본 요소 및 데이터 저장

### Elasticsearch 기본 요소
* **문서(Document):** Elasticsearch에서 문서는 색인될 수 있는 기본 정보 단위. 각 문서는 JSON(JavaScript Object Notation) 형식으로 표현되며, 가벼운 데이터 교환 형식
* **필드(Field):** 필드는 Elasticsearch에서 가장 작은 데이터 단위이며, 키-값 쌍(key-value pair)을 의미

| Elasticsearch | RDBMS |
| --- | --- |
| Index | Database |
| Document | Row |
| Field | Column |
| Mapping | Schema |

### Elasticsearch 데이터 저장 및 관리
* **인덱싱(Indexing):** Elasticsearch에서는 데이터를 Index 단위로 관리. 각 인덱스는 Database처럼 동작함. 문서는 JSON 형식으로 저장되며, 검색을 위해 최적화된 형태로 변환
* **샤딩(Sharding):** Index는 여러 개의 샤드로 나눌 수 있음. 데이터를 여러 노드에 분산 저장하여 성능을 향상시키고, 대용량 데이터와 높은 검색 요청을 효과적으로 처리
* **레플리카(Replica):** Replica Shard는 기본 샤드의 사본으로, 장애 발생 시 데이터 손실을 방지하고 검색 성능을 향상시키는 역할. 여러 복제본이 존재하면 검색 요청을 분산 처리할 수 있어 시스템의 안정성과 확장성이 높아짐

### Elasticsearch 검색 동작 원리
* **질의 처리(Query Processing):** 사용자가 질의를 입력하면, 해당 질의는 구문 분석(Parsing) 및 변환(Transforming) 과정을 거쳐 Lucene 인덱스에서 검색이 가능하도록 최적화된 형식으로 변환. 변환된 질의는 모든 관련 샤드(기본 샤드 및 복제 샤드)에 병렬로 실행되어 빠른 검색이 가능
* **연관성 점수 계산(Relevance Scoring):** Elasticsearch는 검색 결과의 연관성을 평가하기 위해 다양한 알고리즘을 활용. TF-IDF(단어 빈도·역문서 빈도) 및 BM25 등의 알고리즘을 사용하여 각 문서가 사용자의 질의와 부합하는지 계산. 해당 점수는 검색 결과의 순위를 결정하는 데 활용
* **준실시간 검색(Near Real-time, NRT):** Elasticsearch는 데이터를 검색하면서 동시에 색인할 수 있는 준실시간(NRT) 검색 기능을 제공. 메모리 버퍼를 활용해 새로운 문서를 저장하고, 일정 주기로 버퍼를 비워 색인 세그먼트를 생성하여 빠르게 검색 가능
* **준실시간 검색이 가능한 이유:** 메모리 기반 버퍼링으로 색인 속도 향상, 비동기 색인 처리로 검색과 색인을 동시에 수행, Lucene 엔진 최적화를 통한 빠른 색인 적용

---

## 3. Elasticsearch의 아키텍처와 분산처리

### 클러스터 및 노드 개념
* **클러스터(Cluster):** Elasticsearch 클러스터는 하나 이상의 노드(Node)로 구성된 그룹. 클러스터는 고유한 이름을 가지며, 데이터를 분산 저장하고 관리하는 역할을 수행
* **노드(Node):** Elasticsearch의 개별 실행 인스턴스를 노드(Node)라고 함. 각 노드는 데이터를 저장하며, 클러스터의 색인 및 검색 기능에 참여
* **안정성과 가용성 확보:** 장애 대비 및 데이터 보호를 위해 여러 지역에 노드를 분산 배치하고, 복제(replica) 기능을 활용
* **유연한 확장성과 성능 최적화:** 대량의 데이터도 빠르게 처리할 수 있으며, 필요에 따라 노드를 추가하여 확장 가능

### 노드 유형(Node Types)
| 노드 유형 | 역할 |
| --- | --- |
| 마스터 노드(Master Node) | 클러스터 전체의 작업을 관리하고, 인덱스 생성/삭제 및 노드 상태 관리 수행. 클러스터 내 인덱스 생성 및 삭제, 노드 관리, 샤드 분배 등 주요 작업을 결정. 클러스터 운영이 중단되지 않도록 최소한 하나 이상의 마스터 노드 필요. 고가용성을 위해 3개 이상의 마스터 노드 배치를 권장하여 장애 발생 시 안정성 확보 |
| 데이터 노드(Data Node) | 데이터를 저장하며, CRUD, 검색 및 집계(Aggregation) 작업 처리. 색인(Indexing), 검색(Query), 집계(Aggregation) 작업을 담당. CPU, I/O, 메모리 등 하드웨어 리소스 많이 소모 |
| 인제스트 노드(Ingest Node) | 색인(Indexing) 전에 필터, 변환 및 데이터 정제 작업 수행. 데이터 수집 및 사전 처리(색인 전에 필터링, 변환, 정제 작업 수행), 다양한 데이터 변환 지원(필드 추가/삭제, 날짜 변환, 텍스트 정규화 가능) |
| 코디네이팅 노드(Coordinating Node) | 클라이언트 요청을 라우팅하며, 여러 노드에 분산된 검색 로드를 균형 있게 처리. 검색 및 색인 요청을 적절한 데이터 노드로 분산하여 처리. 대규모 클러스터에서는 별도로 분리 운영하여 부하를 줄이는 것이 효과적 |
* 이 외에도 ml, remote_cluster_client, transform 노드 등도 존재

### 샤드(Shard)
* **Shard:** 데이터를 나누어 저장하는 작은 단위로, 노드 간 분산 저장을 통해 성능과 확장성 보장
* **Primary Shard:** 색인 생성 및 CRUD 작업 수행
* **Replica Shard:** 기본 샤드의 복사본으로 장애 복구 및 검색 성능 향상 역할
* `primary shard`는 기본적으로 "처음 인덱스 생성 시점"에서 설정한 이후에는 변경이 불가능하지만, `replica set`의 개수는 언제든지 변경할 수 있음

### Replication
* **Replication:** 데이터를 복제하여 중복성을 확보하고, 고가용성 및 장애 대응을 보장
* **Replication 동작 방식:** Primary-Replica 관계(Primary Shard는 0개 이상의 Replica Shard를 가질 수 있음), 쓰기(Write) 연산(Primary Shard에서 발생한 변경 사항이 모든 Replica Shard에 복제됨), 읽기(Read) 연산(Primary와 Replica Shard가 함께 검색 부하를 분산하여 성능 향상)
* **Replication의 장점:** 고가용성(Primary Shard가 포함된 노드가 다운되면 Replica Shard가 Primary로 승격되어 데이터 접근 가능), 부하 분산(검색 요청을 Primary 또는 Replica Shard로 분산 처리하여 성능 향상), 데이터 중복 저장(여러 노드에 데이터를 복사하여 장애 발생 시 데이터 손실 방지)
* **Replication 고려 사항:** 노드 개수(동일한 샤드의 Primary와 Replica를 같은 노드에 저장할 수 없으므로 충분한 노드 필요), 네트워크 및 저장 부담(복제본 유지로 인해 네트워크 트래픽 증가 및 저장 공간 추가 소모), 트레이드오프(복제본 수가 많을수록 데이터 안정성은 증가하지만, 디스크 및 네트워크 자원 소모도 커짐)

```json
PUT books
{
  "settings": {
    "index": {
      "number_of_shards": 5,
      "number_of_replicas": 1
    }
  }
}
```

### Elasticsearch의 검색 동작
* 클라이언트가 클러스터 내 아무 노드에 검색 요청(GET)을 전송
* 해당 노드는 Coordinate Node로 동작 — 쿼리를 모든 관련 샤드(Primary 또는 Replica 중 하나)에 전달
* 샤드별로 검색 실행 → 결과를 Coordinate Node에 다시 전달
* Coordinate Node는 결과를 취합, 정렬, 필터링 등 후처리 후 최종 결과를 클라이언트에게 응답

---

## 4. Elasticsearch 설치 및 환경 구성 (Docker)

### Docker를 통한 설치
* OS 환경에 영향 받지 않도록 Docker를 통한 설치를 권장
* Windows는 WSL을 통한 Docker Desktop 이용
* `docker compose -f docker-compose-elastic.yml up` 활용 (docker-compose.yml 명이 다를 때는 이와 같이 `-f` 옵션을 통해 명령어로 띄울 수 있음)

### Elasticsearch 환경 구성 (docker-compose.yml)
```yaml
services:
  es01:
    image: docker.elastic.co/elasticsearch/elasticsearch:8.17.1
    container_name: es01
    environment:
      - node.name=es01
      - cluster.name=elastic-docker-cluster
      ## 3개의 노드 실행 시
      # - discovery.seed_hosts=es02,es03
      # - cluster.initial_master_nodes=es01,es02,es03
      ## 노드 하나만 실행 시
      - discovery.seed_hosts=es01
      - cluster.initial_master_nodes=es01
      - node.roles=master,data,ingest
      - "ES_JAVA_OPTS=-Xms512m -Xmx512m"
      - xpack.security.enabled=false
      - network.host=0.0.0.0
    volumes:
      - shared_esdata:/usr/local/elasticsearch/data
    ports:
      - "9200:9200"
    networks:
      - elastic

  es02:
    ports:
      - "9201:9200"
    networks:
      - elastic
```

**주요 설정 항목**
| 설정 | 설명 |
| --- | --- |
| image | 사용할 Docker 이미지 지정(공식 Elasticsearch 8.17.1 버전 이미지) |
| container_name | 컨테이너 이름을 설정(예: `es01`로 고정) |
| node.name | 이 노드의 이름을 지정(클러스터 내에서 구분할 때 사용) |
| cluster.name | 클러스터의 이름을 설정(클러스터 이름이 일치하는 노드끼리 합쳐져서 클러스터를 이룸) |
| discovery.seed_hosts | 클러스터를 구성할 때 다른 노드의 "주소 목록"을 제공(예: es02, es03을 추석 해제하면 3개 노드를 연결. 하나만 실행할 땐 es01 자신만 바라보도록 설정) |
| cluster.initial_master_nodes | 초기 클러스터 마스터 노드를 지정(초기화할 때 반드시 필요한 설정) |
| node.roles | 이 노드의 역할을 설정(`master`: 클러스터 상태 관리, `data`: 데이터 저장 및 검색 처리, `ingest`: 데이터 전처리 파이프라인 실행. 즉, es01은 모든 역할을 수행하는 총합 노드) |
| jvm.options(ES_JAVA_OPTS) | Elasticsearch가 사용할 Java 힙 메모리 크기 지정(`-Xms`: 최소 힙 크기, `-Xmx`: 최대 힙 크기) |
| volumes | Docker 컨테이너상에 위임된 Elasticsearch 데이터 디렉토리를 공유 저장소에 연결(`shared_esdata` 볼륨을 `/usr/local/elasticsearch/data`에 마운트) |
| ports | 호스트의 9200번 포트를 컨테이너 9200 포트에 매핑(es02와 같은 경우는 docker를 띄운 호스트 9201로 해당 컨테이너 9200번에 매핑) |
| networks | `elastic`이라는 Docker 네트워크에 연결(이 네트워크를 통해 Kibana와 다른 Elasticsearch 노드들이 서로 통신) |

---

## 5. Elasticsearch REST API & Document CRUD

### Elasticsearch의 데이터 교환 방식
* Elasticsearch는 분산 검색 및 분석 엔진으로, 대량의 데이터를 빠르게 검색하고 분석하는 데 사용
* RESTful API를 통해 클라이언트와 통신

### Elasticsearch의 RESTful API
* Elasticsearch는 HTTP 요청을 통해 데이터를 처리하며, API 설계를 통해 다양한 기능을 제공
* 언어 독립성, 확장성, 직관적인 인터페이스 제공 등의 장점
* **REST API의 특징:** HTTP 기반으로 동작, 자원을 URL로 표현(`/users/1`, `/products/10` 등), HTTP 메서드를 활용하여 CRUD 작업 수행(GET, POST, PUT, DELETE), JSON·XML 등 다양한 데이터 형식을 사용

```
POST /products/_doc/1
Content-Type: application/json

{
  "name": "Samsung Galaxy S25 Ultra",
  "brand": "Samsung",
  "price": 1099,
  "category": "smartphone"
}
```

### Elasticsearch의 index 생성
1. Index를 Elasticsearch가 자동 매핑(dynamic mapping)으로 필드 타입을 추론해서 설정
2. 인덱스를 미리 정의된 설정으로 명시적으로 생성(mapping도 가능)

```python
# 방법 1: 자동 매핑
doc = {
    "name": "Samsung Galaxy S24 Ultra",
    "brand": "Samsung",
    "price": 1199.99,
    "category": "smartphone",
    "rating": 4.8,
}
response = es.index(index="products", id=1001, document=doc)
```

```python
# 방법 2: 명시적 설정
es.indices.create(
    index="products",
    body={
        "settings": {
            "index": {
                "number_of_shards": 3,
                "number_of_replicas": 1,
            }
        }
    },
)
```

### Document CRUD

**Create — POST를 활용한 문서 생성**
```
POST /products/_doc/1001
{
  "name": "Samsung Galaxy S24 Ultra",
  "brand": "Samsung",
  "price": 1199.99,
  "category": "smartphone",
  "rating": 4.8
}
```
응답: `{"_index": "products", "_id": "1001", "_version": 1, "result": "created", "_shards": {"total": 2, "successful": 1, "failed": 0}, "_seq_no": 0, "_primary_term": 1}`

**Read — GET을 활용한 문서 조회**
```python
response = es.get(index="products", id=1001)
```
```
GET /products/_doc/1001
```

**Update — POST를 통한 업데이트**
```python
# 1. 기존 내용을 변경
update_body = {"doc": {"price": 1099}}
response = es.update(index="products", id=1001, body=update_body)
```
```python
# 2. 새로운 필드 추가
update_body = {"doc": {"stock": 200}}
response = es.update(index="products", id=1001, body=update_body)
```

**Upsert** — 업데이트(Update)와 삽입(Insert)을 결합한 연산. 해당 ID의 문서가 존재 → 업데이트 수행 / 해당 ID의 문서 없음 → 새 문서 생성
```
POST /products/_update/1001
{
  "doc": {"price": 1099, "stock": 150},
  "doc_as_upsert": true
}
```

**Delete — DELETE를 통한 삭제**
```python
response = es.delete(index="products", id=1001)
```
* 실제로 삭제되는 것이 아니고 표시되기 때문에 flush를 해야 완전 삭제

```python
# 인덱스 강제 flush
response = es.indices.flush(index="products")
```

### Elasticsearch 문서 업데이트
* Elasticsearch의 문서는 불변(Immutable)하므로 직접 수정되지 않으며, 업데이트 시 새로운 문서로 저장 등으로 사용 가능
* **업데이트 과정:** ① 기존 문서 조회 → 업데이트 요청 시 현재 색인된 문서를 가져옴 ② 변경 사항 적용 → 기존 문서에 수정된 내용을 반영 ③ 새 문서 색인 → 변경된 문서를 새로운 버전으로 다시 저장 ④ 이전 문서 삭제 처리 → 기존 문서는 논리적 삭제(Logical Deletion)로 표시되어 검색에서 제외됨 ⑤ 세그먼트 병합 → Segment Merging을 수행하여 삭제된 문서를 물리적으로 제거하고 저장 공간 확보

### Elasticsearch에서 세그먼트와 Flush의 관계
1. `upsert` & `update`는 기존 문서를 수정하는 것이 아니라 새로운 세그먼트를 생성하는 방식으로 동작 — Elasticsearch는 Lucene을 사용하며, 세그먼트는 수정될 수 없는(Immutable) 구조이므로 문서가 업데이트될 때 기존 세그먼트를 수정하지 않고 새로운 세그먼트를 생성
2. Flush는 새로운 세그먼트를 디스크에 기록하는 과정 — 새로운 문서 추가(index), 업데이트(update), 삭제(delete)가 발생하면 메모리(Buffer)에 먼저 저장되고, Flush가 발생하면 메모리에 있던 문서들이 새로운 세그먼트로 생성되고 디스크에 저장됨
3. 세그먼트가 증가하면 자동으로 병합(Merge) 수행 — 새로운 세그먼트가 많아지면 검색 속도가 느려질 수 있어, Elasticsearch는 주기적으로 여러 개의 작은 세그먼트를 하나로 병합하여 성능을 최적화

---

## 핵심 요약
* **Elasticsearch**는 Apache **Lucene** 기반의 분산 검색·분석 엔진으로, 역색인(Inverted Index)과 TF-IDF/BM25 알고리즘으로 빠르고 연관성 높은 전문 검색을 제공하며 준실시간(NRT) 검색을 지원한다.
* 데이터는 **Index(≈Database) → Document(≈Row) → Field(≈Column)** 구조로 저장되며, Index는 여러 **Shard**로 나뉘어 분산 저장되고 각 Primary Shard는 **Replica Shard**로 복제되어 고가용성과 검색 부하 분산을 확보한다.
* 클러스터는 **Master(클러스터 관리) / Data(저장·검색·집계) / Ingest(전처리) / Coordinating(요청 라우팅)** 노드로 역할이 나뉘며, 하나의 노드가 여러 역할을 겸할 수도 있다.
* 설치는 Docker Compose로 진행하며, `node.name`/`cluster.name`/`discovery.seed_hosts`/`cluster.initial_master_nodes`/`node.roles` 등의 설정으로 단일·다중 노드 클러스터를 구성한다.
* Lucene의 세그먼트는 불변(Immutable) 구조이기 때문에 **update/upsert**도 실제로는 새 세그먼트 생성 방식으로 동작하며, 변경 사항은 메모리 버퍼에 쌓였다가 **Flush** 시 디스크의 새 세그먼트로 기록되고, 세그먼트가 늘어나면 주기적으로 병합(Merge)되어 성능을 최적화한다.
* REST API(`GET`/`POST`/`PUT`/`DELETE`)로 Document의 CRUD(생성/조회/업데이트/삭제)를 수행하며, Delete는 즉시 물리 삭제가 아니라 논리적 삭제 후 `flush`/세그먼트 병합을 거쳐 완전히 제거된다.
