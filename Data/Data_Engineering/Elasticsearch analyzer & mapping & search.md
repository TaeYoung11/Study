# Elasticsearch analyzer & mapping & search 정리

Elasticsearch의 **Mapping**(필드 타입 정의), **Index**(정방향/역방향 인덱스), **Analyzer**(형태소 분석·동의어·불용어 처리), 그리고 **Search**(Query DSL을 활용한 다양한 검색 방법)를 정리한 문서입니다.

---

## 1. Mapping

### Elasticsearch Mapping
* **Mapping:** 관계형 데이터베이스의 스키마(Schema)와 유사한 개념. Elasticsearch에서 문서(Document)의 필드 유형과 속성을 정의
* **동적 매핑(Dynamic Mapping):** 매핑을 명시적으로 설정하지 않아도 Elasticsearch가 자동으로 생성. 편리하지만, 잘못된 데이터 타입 할당 가능성이 있음
* **정적 매핑(Explicit Mapping):** 사전에 명확하게 매핑을 정의하여 정확한 데이터 타입을 설정. 검색 성능 최적화 및 불필요한 리소스 낭비 방지 가능

### Dynamic Mapping
* Elasticsearch에서 문서를 색인할 때 필드 이름과 데이터 유형을 자동으로 결정하는 기능
* 관계형 데이터베이스의 스키마(Schema)와 유사한 개념이지만, 사전 정의 없이도 데이터 유형이 결정
* **자동 매핑:** 필드를 사전에 정의하지 않아도 문서를 색인하면 자동으로 매핑
* **규칙 설정 가능:** 동적 필드 매핑 규칙을 사용하여 원하는 방식으로 동작하도록 설정 가능
* **데이터 유형 결정:** 새로운 필드가 감지되면 해당 필드의 데이터 유형을 자동으로 판단하여 매핑
* **오류 가능성:** 자동으로 할당된 데이터 유형이 잘못될 경우 검색 오류 발생 가능
* **파라미터 설정 가능:** 동적 매핑을 활성화하거나 비활성화하는 설정을 조정 가능

### Dynamic parameter
```python
es = Elasticsearch("http://localhost:9200")
es.indices.create(index="products", body={"mappings": {"dynamic": "runtime"}})
```
| 설정값 | 동작 |
| --- | --- |
| true | 새로운 필드가 자동 추가됨(기본값) |
| runtime | 새로운 필드는 색인되지 않고 쿼리 시 로드 |
| false | 새로운 필드는 무시됨 |
| strict | 새로운 필드 추가 시 오류 발생 |

### Static Mapping
* 정적 매핑은 문서에 저장될 데이터의 필드와 타입을 사전에 정의하는 방식
* 인덱스 생성 시점에 필드 타입을 미리 지정해야 하며, 한 번 설정된 매핑은 일부 변경이 제한

```python
es = Elasticsearch("http://localhost:9200")
es.indices.create(
    index="products",
    body={
        "mappings": {
            "properties": {
                "name": {"type": "text"},
                "brand": {"type": "keyword"},
                "price": {"type": "float"},
                "category": {"type": "keyword"},
                "rating": {"type": "float"},
            }
        }
    },
)
```

### Elasticsearch 필드 데이터 타입
* **지형 데이터 타입(Geo Data Types):** `geo_point`, `geo_shape`
* **계층 구조 데이터 타입(Hierarchical Data Types):** `Object`, `Nested`
* **일반 데이터 타입:** 문자열 데이터 타입(`keyword`, `text`), `date`, `long`, `double`, `integer`, `boolean` 등

### 문자열 데이터 타입
* **Text:** 전문(full-text) 검색을 위한 분석(토큰화)이 적용된 문자열
* **keyword:** 정렬 및 필터링에 최적화된 문자열
* 공간을 절약하고 쓰기 속도를 높이기 위해 색인 전 매핑을 설정하는 것이 유리

### Keyword 필드 타입
* 데이터 변형 없이 저장 → 분석기(analyzer)를 적용하지 않음
* 공백과 대소문자를 구분하지 않음 → 정확한 검색 수행
* 집계, 정렬, 필터링 등에 적합
* `keyword`, `constant_keyword`, `wildcard` 등의 타입으로 설정 가능
* **정확한 값으로 검색해야 하는 항목:** 태그(tags), 카테고리(category)
* **정렬이 필요한 데이터:** 브랜드명(brand), 사용자 ID(user_id)
* **집계(Aggregation)이 필요한 데이터:** 로그 수준(log_level), 국가 코드(country_code)

```python
es.indices.create(
    index="your_index_name",
    body={"mappings": {"properties": {"name": {"type": "keyword"}}}},
)
```
* **constant_keyword 필드 타입:** 색인 크기를 줄이고 검색 속도를 높임, 필터링(Querying) 성능 최적화에 적합, 수정 불가능

```python
es.indices.create(
    index="products",
    body={"mappings": {"properties": {"brand": {"type": "constant_keyword", "value": "Samsung"}}}},
)
```

### Text 필드 타입
* 입력된 텍스트를 분석기(Analyzer)를 사용해 토큰으로 분리
* 부분 검색 가능 → 입력된 단어의 일부만 일치해도 검색 결과에 포함됨
* 공백, 대소문자, 형태소 분석 등 다양한 처리가 가능
* 정확한 검색보다는 문서 검색과 연관성 기반 검색에 적합

```python
es.indices.create(index="products", body={"mappings": {"properties": {"name": {"type": "text"}}}})
```

* **match_only_text 필드 타입:** 로그 분석 시 주로 사용, keyword 필드와 text 필드의 중간 단계, 전체 텍스트 쿼리를 실행하며 점수를 매기지 않음, 정렬이나 집계가 필요 없을 경우 적합
* **search_as_you_type 필드 타입:** 자동 완성(Autocomplete) 검색을 지원하는 데이터 타입. n-gram 분석기를 사용하여 전방 일치(prefix match) 또는 중간 일치(substring match) 검색 가능. 검색 시 입력이 점진적으로 확장되는 형태의 검색을 지원. 매핑에 지정된 분석기 사용 / 별도 분석기가 없으면 기본(Standard) 분석기 사용

---

## 2. Index

### Forward Index vs Inverted Index
* **Forward Index(정방향 인덱스):** 문서 중심으로 인덱스를 구축. 각 문서가 포함하는 단어 목록을 저장. 인덱스 구축이 단순하지만, 검색 속도가 느림(모든 문서를 순회해야 함). 특정 문서의 내용을 확인할 때 유용

```
문서 1 → ["Elasticsearch", "검색", "엔진"]
문서 2 → ["Kibana", "시각화", "도구"]
```

* **Inverted Index(역방향 인덱스):** 단어 중심으로 인덱스를 구축. 각 단어가 포함된 문서 목록을 저장. Elasticsearch에서 기본적으로 사용하는 방식. 검색 시 특정 단어를 빠르게 찾을 수 있음

```
"검색"  → [문서 1]
"엔진"  → [문서 1]
"Kibana" → [문서 2]
```

* **Inverted Index에서 forward index 활용:**
  * **fielddata:** 특정 텍스트 필드에 대한 정렬, 집계(aggregation) 시 사용. 메모리 사용량이 증가하는 단점

```python
PUT my_index/_mapping
{
  "properties": {
    "my_field": {
      "type": "text",
      "fielddata": true
    }
  }
}
```
  * **doc_values:** 메모리 사용량을 줄이기 위해 디스크 기반 컬럼 저장 방식 사용. 기본적으로 비 텍스트 필드에서 활성화됨. `keyword` 필드 활용 가능

### Index 생성
```python
PUT my_index/_mapping
{
  "properties": {
    "user_name": {"type": "text"},
    "fields": {
      "keyword": {"type": "keyword", "ignore_above": 256}
    }
  }
}
```

### 인덱스 엘리아스
* 인덱스 명을 대신하는 가상의 이름을 부여할 수 있음
* 여러 개의 인덱스를 하나의 인덱스처럼 연결하여 사용 가능
* 신규 인덱스에 데이터를 색인하고, 엘리아스를 이용해 다운타임 없이 인덱스를 교체 가능
* `_alias API`를 사용하여 설정
* aliases로 선언한 `products_alias`를 통해 조회해도 `products` 인덱스의 데이터를 다룰 수 있음
* `products_alias`로 검색할 때 `products`와 `products_v2` 인덱스의 데이터를 모두 가져오게 됨

```python
# 인덱스 생성
es.indices.create(index="products", body={"aliases": {"products_alias": {}}})

# 인덱스 업데이트
es.indices.update_aliases(body={"actions": [{"add": {"index": "products_v2", "alias": "products_alias"}}]})
```

---

## 3. Analyzer

### Analyzer(분석기)란?
* Analyzer는 문서를 색인하고 검색할 때 텍스트를 처리하는 방식
* 문서의 내용을 토큰(token)으로 변환하여 색인 및 검색 — Analyzer를 거친 단어들만 검색 가능
* 어떤 Analyzer를 사용하고 실행 순서를 정하는 것이 중요 — Analyzer의 설정 방식에 따라 검색 결과가 달라질 수 있음
* 너무 많은 분석을 하면 색인 성능 저하 — Analyzer가 복잡할수록 색인 속도가 느려질 수 있음

### 색인과 검색에서의 Analyzer의 차이
* 색인할 때 분석기와 검색할 때 분석기가 다를 수도 있음
* 일반적으로 색인과 검색 시 같은 analyzer를 사용하는 것이 좋음
* **색인과 검색에 다른 분석기를 적용하는 경우:**
  * 검색에 필터링이 필요한 경우: 검색에서 의미 없는 단어(불용어)를 제거해야 하는 경우, 특정 단어를 제외하고 검색해야 하는 경우
  * 동의어나 맞춤법 교정을 적용하는 경우: "car"를 검색하면 "automobile"도 검색되도록 설정, "color"와 "colour"를 동일하게 처리

### Analyzer 구성요소
* 분석기(Analyzer)는 검색 성능을 향상시키기 위해 문서를 토큰화(tokenization)하고, 텍스트를 변환하는 기능을 수행
* **Character Filter → Tokenizer → Token Filter** 순서로 진행

```
Input String → [Character Filter] → [Tokenizer] → [Token Filter] → A list of unique terms
```

* **Character Filters(문자 필터):** 원본 텍스트를 전처리하는 단계. 특정 문자나 패턴을 변환하거나 제거함. `html_strip` → HTML 태그 제거, `mapping` → 특정 문자열을 다른 문자열로 매핑, `pattern_replace` → 정규식을 이용한 텍스트 변경
* **Tokenizer(토크나이저):** Character Filter를 거친 텍스트를 특정 규칙에 따라 토큰(단어 단위)으로 분리. 분석기 구성 시 한 개의 Tokenizer만 사용 가능. `whitespace` → 공백 기준으로 단어 분리, `standard` → 일반적인 텍스트 토큰화, `ngram` → 부분 문자열(서브스트링) 단위로 분리
* **Token Filters(토큰 필터):** Tokenizer를 통해 분리된 토큰을 추가, 수정, 삭제하는 필터. 여러 개를 배열로 사용 가능. `lowercase` → 모든 단어를 소문자로 변환, `stop` → 불용어(예: "the", "is") 제거, `synonym` → 동의어 처리(예: "car" ↔ "automobile")

### _analyze API
* `_analyze API`는 커스텀 분석기(Analyzer)를 테스트할 수 있도록 제공되는 API
* `"whitespace"`: 공백을 기준으로 텍스트를 분리
* `"token"`: 분리된 단어 / `"start_offset"`/`"end_offset"`: 원본 텍스트에서 시작/끝 위치 / `"position"`: 토큰의 순서

```python
POST _analyze
{
  "analyzer": "whitespace",
  "text": "삼성 청년 SW 아카데미"
}
```

### Analyzer 조합(Custom Analyzer)
* custom analyzer를 생성할 때 여러 가지 요소를 조합 가능
* tokenizer는 하나만 지정할 수 있으며, char_filter와 filter는 여러 개 적용 가능

```python
GET _analyze
{
  "char_filter": ["html_strip"],
  "tokenizer": "whitespace",
  "filter": ["stop", "lowercase"],
  "text": ["<b>삼성 갤럭시</b> S25 Ultra"]
}
```

### 한국어 Analyzer(Nori Analyzer)
* 한국어 처리를 위해 nori 분석기를 제공
* **nori 분석기 특징:** 형태소 기반 분석기로, 한국어의 복잡한 문장 구조를 효과적으로 분석. Elasticsearch 기본 패키지가 아니므로 설치 필요
* **nori 분석기 구성요소:** Tokenizer(토크나이저), `nori_tokenizer` → 형태소 분석을 수행하여 단어를 분리, Token Filters(토큰 필터), `nori_part_of_speech` → 품사 기반 필터링(예: 명사만 남기기), `nori_readingform` → 한자/외래어 등을 한글 발음으로 변환, `nori_number` → 숫자를 표준화(예: "일" → "1")

### 한국어 Tokenizer(Nori Tokenizer)
* 한국어 형태소 분석기
* 형태소 분석을 지원하지 않는 기본 분석기 사용 시 복합명사를 적절히 분해할 수 없음
* **사용자 정의의 사전(user_dictionary) 지원**
* **복합명사 처리 방식(decompound_mode) 선택 가능**
* 기본적으로 구두점(discard_punctuation) 제거

```python
PUT /products
{
  "settings": {
    "index": {
      "analysis": {
        "tokenizer": {
          "nori_tokenizer": {
            "type": "nori_tokenizer",
            "decompound_mode": "mixed",
            "discard_punctuation": "false",
          }
        },
        "analyzer": {
          "custom_nori_analyzer": {
            "type": "custom",
            "tokenizer": "nori_tokenizer",
            "filter": ["nori_pos_filter"],
          }
        },
      }
    }
  }
}
```

### 사용자 사전 처리
* 단어 형태를 강제로 지정해 복합명사 단일명사를 원하는 형태로 색인하기 위한 방식
* `decompound_mode: "mixed"`: 복합어 처리를 적절하게 수행
* `discard_punctuation: "false"`: 구두점 유지
* `user_dictionary: "dictionary/userdic_ko.txt"`: 사용자 사전 적용
* 향후 색인 및 검색 시 적용 가능

```
userdic_ko.txt
1  아이폰
2  삼성갤럭시 삼성 갤럭시
```

### 동의어 처리
* 색인 시 동의어 처리와 검색 시 동의어 처리 두 가지 방식 가능
* **색인 시 동의어 처리:** 색인(indexing) 시점에서 동의어를 확장하여 저장하는 방식
* **검색 시 동의어 처리:** 검색(query) 시점에서 동의어를 확장하여 질의하는 방식

### 색인 시 동의어 처리
* 색인(indexing) 시점에서 동의어를 확장하여 저장하므로, 검색 시 추가적인 처리가 필요하지 않음
* 색인된 데이터의 크기가 증가할 수 있음
* 새로운 동의어를 추가하려면 전체 데이터 재색인(reindexing)이 필요

```python
PUT /products
{
  "settings": {
    "analysis": {
      "filter": {
        "synonym_filter": {
          "type": "synonym",
          "synonyms": ["notebook, laptop", "smartphone, mobile"],
        }
      },
      "analyzer": {
        "synonym_analyzer": {
          "type": "custom",
          "tokenizer": "standard",
          "filter": ["lowercase", "synonym_filter"],
        }
      },
    }
  },
  "mappings": {
    "properties": {"description": {"type": "text", "analyzer": "synonym_analyzer"}}
  },
}
```

### 검색 시 동의어 처리
* 검색된 문서를 변경할 필요가 없으며, 동의어를 추가하면 즉시 반영
* 검색 시 추가적인 처리 비용이 발생하여 성능이 저하될 수 있음
* 동의어를 잘못 등록하면 쿼리가 예상치 못한 단어를 포함해 검색 품질이 저하될 수 있음
* 매핑의 `analyzer`(색인용)와 별도로 `search_analyzer`(검색용)를 지정하여 동일한 synonym_analyzer를 검색 시점에만 적용

### 동의어 사전 구성
* **단어 동등 관계(A, B):** 색인할 때 A와 B를 동일한 의미로 저장. 검색 시 A를 입력해도, B를 입력해도 같은 문서가 검색됨

```json
"synonyms": ["notebook, laptop"]
```
* **단어 치환 단계(A → B):** 색인할 때 A를 B로 변환하여 저장. 검색할 때 A를 검색해도 색인되지 않음. 검색 시 A → B로 변환 후 검색하면, 기존 A로 저장된 문서는 검색되지 않음

```json
"synonyms": ["notebook => laptop"]
```

### 동의어 사전 불러오기
* **동의어 사전 저장 위치:** Elasticsearch 노드의 config 폴더 하위에 동의어 사전을 생성. 일반적으로 `config/dictionary` 디렉토리에 `synonyms.txt` 같은 파일로 저장

```
synonyms.txt
1  갤럭시, galaxy
2  삼성, samsung
3  울트라, Ultra
```

* **동의어 사전 리로드 API:** 색인이 아닌 검색 시 동의어를 적용할 경우, 사전 업데이트가 즉시 반영되지 않음. 새로운 동의어를 반영하려면 리로드 API를 실행해야 함. 리로드 API 실행 후 캐시를 비워야 변경 내용이 반영

```
POST /products/_reload_search_analyzers
POST /products/_cache/clear?request=true
```

### 불용어(Stopword) 처리
* **불용어 사전:** Elasticsearch 노드의 `config/dictionary/` 폴더에 파일을 생성. 불용어로 지정된 단어들은 색인 및 검색에서 제외. 검색타임에도 사전을 리로드 가능. 내용을 한 줄에 입력해 txt파일로 저장

```
stopwords.txt
the / and / is / was / a / an / of
```

### 불용어(Stopword) 필터 적용
* 분석기(search_stop_analyzer) — `search_stop_filter` 적용 → `stopwords.txt`에 있는 단어 제거
* **불용어 필터(search_stop_filter):** Stopwords_path 옵션을 사용해 불용어를 파일에서 로드

```python
PUT /products
{
  "settings": {
    "index": {
      "analysis": {
        "analyzer": {
          "search_stop_analyzer": {
            "tokenizer": "whitespace",
            "filter": ["search_stop_filter"],
          }
        },
        "filter": {
          "search_stop_filter": {
            "type": "stop",
            "stopwords_path": "dictionary/stopwords.txt",
          }
        },
      }
    }
  }
}
```

---

## 4. Search

### URI 검색
* 간단한 검색을 수행할 때 URI 검색(URI Query String Search)을 사용
* `"key=value"` 형식으로 전달
* URL에 검색할 컬럼과 검색어를 지정 가능하며 검색 조건을 추가할 수 있음
* Request Body 검색 대비 단순하고 사용이 편리하지만 복잡한 쿼리를 수행할 수 없다는 한계

```
GET /products/_search?q=brand:Samsung&default_operator=AND
```

### Query DSL
* Elasticsearch에서 검색을 수행하기 위한 JSON 기반의 질의 언어
* **JSON 형식 사용:** HTTP 요청 시 본문의 JSON 문서를 활용하여 Elasticsearch에 검색 요청
* **Query Context:** 검색어와 문서 간의 유사도 점수(`_score`)를 기반으로 검색 스코어링을 통해 문서의 중요도를 평가
* **Filter Context:** 문서가 검색 조건에 해당하는지 여부만 판단, `_score` 값을 계산하지 않음. 캐싱이 가능하여 성능 최적화 가능

### Query DSL 쿼리 형식
| 필드명 | 필드명 설명 |
| --- | --- |
| size | 반환할 문서 개수(기본값: 10) |
| from | 검색 결과에서 몇 번째 문서부터 표시할지 설정(기본값: 0) |
| timeout | 검색 수행 시간 제한(기본값 없음, 필요 시 "30s" 등 설정 가능) |
| _source | 검색 결과에 포함할 필드 지정(기본값: 전체 포함) |
| query | 검색 조건이 들어가는 공간 |
| aggs | 통계 및 집계 데이터 설정 공간 |
| sort | 문서 정렬 기준 설정 |

```json
{
  "size": 10,
  "from": 0,
  "timeout": "30s",
  "_source": ["field1", "field2"],
  "query": { "..." },
  "aggs": { "..." },
  "sort": ["..."]
}
```

### Query DSL 쿼리 검색 예시
```json
GET /products/_search
{
  "size": 5,
  "query": {
    "match": {"description": "무선 마우스"}
  }
}
```
검색 결과에서 최대 5개 문서 반환, `description` 필드에서 "무선 마우스"와 유사한 문서 검색

```json
GET /products/_search
{
  "_source": ["name", "price"],
  "query": {"match": {"category": "전자기기"}},
  "sort": [{"price": "desc"}]
}
```
`name`과 `price` 필드만 출력, `category`가 "전자기기"인 문서 검색, `price` 기준으로 내림차순 정렬

### Query DSL 쿼리 결과
```json
{
  "took": "쿼리 실행에 소요된 시간(ms)",
  "_shards": {"total": "검색 대상이 된 전체 샤드 개수", "successful": "정상적으로 검색이 수행된 샤드 개수", "failed": "검색 중 오류가 발생한 샤드 개수"},
  "hits": {"total": "검색된 문서의 총 개수", "max_score": "가장 높은 검색 점수", "hits": "검색된 문서 목록"}
}
```

### 검색 결과 정렬
* **기본 정렬(Default Sorting):** 기본적으로 검색 쿼리가 실행되면 `_score` 값(유사도 점수)에 따라 검색 결과가 정렬. `_score` 값이 높은 문서일수록 쿼리와 더 높은 유사도를 가지므로 상위에 노출됨. `_score`는 TF-IDF(Term Frequency-Inverse Document Frequency) 또는 BM25 등의 알고리즘을 기반으로 계산(변경 가능)
* **특정 필드를 기준으로 정렬:** `price`(가격) 기준 정렬(최신 제품 또는 가격이 높은 순으로 정렬 가능), `name`(상품명) 기준 정렬(이름순 정렬 가능), `created_at`(등록일) 기준 정렬(최신순 또는 오래된 순 정렬 가능)

```json
GET products/_search
{
  "sort": [{"price": {"order": "desc"}}]
}
```

### 검색 결과 페이징
* **검색 결과 보여주기:** `from`(페이지를 가져올 때의 시작점), `size`(검색 결과를 가져올 양)

```json
GET products/_search
{
  "from": 0,
  "size": 5
}
```

### Filter Context — Term level query
* 텍스트 분석기를 사용하지 않고 정확한 값을 검색할 때 사용
* 필터 되는 속도가 빠름
* SQL에서 `=`과 같은 역할

```json
GET products/_search
{
  "query": {"term": {"brand": "Samsung"}}
}
```

### Filter Context — Terms level query
* 특정 필드에 대해 SQL의 IN 조건처럼 여러 값을 검색
* 다중 값을 조회할 수 있음
* 텍스트와 다른 타입의 정보를 동시에 이용할 수 있음

```json
GET products/_search
{
  "_source": ["name", "brand"],
  "query": {"terms": {"brand": ["Samsung", "Apple"]}}
}
```

### Range Query
* 숫자, 가격, 크기, 날짜 등을 범위로 필터링할 때 사용
* SQL의 `BETWEEN` 또는 `>=`, `<=`, `>`, `<` 조건과 유사한 기능

| 파라미터 | 설명 |
| --- | --- |
| gt | A보다 큼(> A) |
| gte | A보다 크거나 같음(>= A) |
| lt | A보다 작음(< A) |
| lte | A보다 작거나 같음(<= A) |

```json
GET products/_search
{
  "query": {
    "range": {"price": {"gte": 2000, "lte": 3000}}
  }
}
```

### Query Context — Match query
* Elasticsearch에서 가장 기본적인 전체 텍스트 검색(full-text search) 방식
* 분석기(Analyzer)를 적용하여 단어를 토큰화 및 변환한 후 검색
* SQL로 표현하면 `LIKE '%검색어%'`와 비슷한 기능이지만 훨씬 효율적

```json
GET products/_search
{
  "_source": ["product_id", "name", "description"],
  "query": {"match": {"description": "AI"}}
}
```

### Match query with operator
* `operator`는 검색어가 여러 개일 때 AND 또는 OR 조건을 적용할지 결정

```json
GET products/_search
{
  "_source": ["product_id", "name", "brand"],
  "query": {"match": {"name": "Samsung Ultra", "operator": "AND"}}
}
```
* "Samsung"과 "Ultra" 둘 다 포함된 문서만 반환

### Match Phrase Query
* 단어 순서를 유지한 채 검색을 수행

```json
GET products/_search
{
  "_source": ["product_id", "name", "brand"],
  "query": {"match_phrase": {"name": "Samsung Neo"}}
}
```

### Match Phrase Prefix Query
* 단어 순서를 유지한 채 검색을 수행하되 자동완성 형태로 검색 가능

```json
GET products/_search
{
  "_source": ["product_id", "name", "brand"],
  "query": {"match_phrase_prefix": {"name": "Samsung N"}}
}
```
* "Samsung Neo"가 기억이 안나는 경우 "N"까지만 입력해도 활용하기 위한 방식

### Multi Match Query
* 여러 개의 필드를 동시에 검색할 때 사용하는 쿼리
* `match` 쿼리와 다르게 여러 필드에서 일치하는 문서를 찾을 수 있음
* type을 설정하여 검색 방식 조정 가능(`best_fields`, `most_fields`, `cross_fields`)

### Multi Match Query(best_fields)
* 여러 필드에서 가장 높은 점수를 가진 필드만 반영
* "Samsung"이 `name` 또는 `description` 중 하나라도 포함된 문서 반환
* score가 가장 높은 필드를 기준으로 결정

```json
GET products/_search
{
  "_source": ["product_id", "name", "brand", "description"],
  "query": {"multi_match": {"query": "Samsung", "fields": ["name", "description"], "type": "best_fields", "operator": "or"}}
}
```

### Multi Match Query(most_fields)
* 여러 필드에서 모든 일치 항목의 점수를 합산
* "Samsung"이 `name`과 `description` 양쪽에서 검색되면 점수가 더 높아짐
* "Samsung"이 여러 필드에서 발견될수록 검색 결과의 점수가 증가

```json
GET products/_search
{
  "query": {"multi_match": {"query": "Samsung", "fields": ["name", "description"], "type": "most_fields", "operator": "or"}}
}
```

### Multi Match Query(cross_fields)
* 여러 필드의 텍스트를 조합한 검색
* "Samsung"과 "Ultra"가 서로 다른 필드에 있어도 검색 가능

```json
GET products/_search
{
  "query": {"multi_match": {"query": "Samsung Ultra", "fields": ["name", "description"], "type": "cross_fields", "operator": "and"}}
}
```

### Exist Query
* 필드가 존재하는 문서만 검색
* Elasticsearch는 필드가 존재하는 문서와 존재하지 않는 문서 공존 가능

```json
GET products/_search
{
  "query": {"exists": {"field": "description"}}
}
```

### Boolean Query
* 여러 개의 조건을 조합하여 검색을 수행하는 쿼리 방식
* `must`, `must_not`, `should`, `filter` 네 가지 방식으로 조합 가능
* SQL의 AND, OR, NOT 연산과 유사함

| Boolean Query 유형 | 설명 | 예제 |
| --- | --- | --- |
| must | 모든 조건을 만족하는 문서 검색 | `brand = Samsung AND description = AI` |
| must_not | 특정 조건을 포함하지 않는 문서 검색 | `brand != Google` |
| should | 하나라도 조건을 만족하면 검색 | `brand = Samsung OR brand = Apple` |
| filter | 빠른 검색(점수 미계산) | `brand = Samsung AND description = AI` |

**Brand가 samsung이면서 AI라는 설명이 들어가는 문서**
```json
GET products/_search
{
  "_source": ["name", "brand", "description"],
  "query": {
    "bool": {
      "must": [
        {"match": {"brand": "Samsung"}},
        {"match": {"description": "AI"}}
      ]
    }
  }
}
```

**Brand가 Google이 아닌 문서**
```json
GET products/_search
{
  "_source": ["name", "brand", "description"],
  "query": {
    "bool": {
      "must_not": [{"match": {"brand": "Google"}}]
    }
  }
}
```

---

## 핵심 요약
* **Mapping**은 RDBMS의 스키마에 대응하는 개념으로, 필드 타입을 자동으로 추론하는 **동적 매핑**과 미리 정의하는 **정적 매핑**으로 나뉘며, 정확한 값 검색·정렬·집계에는 `keyword`, 전문 검색에는 `text` 타입을 사용한다.
* **Index**는 단어 중심으로 문서를 찾는 **역방향 인덱스(Inverted Index)** 구조를 기본으로 사용하며, 정렬·집계를 위해 `fielddata`/`doc_values`로 정방향 인덱스를 보조적으로 활용하고, **인덱스 엘리아스**로 다운타임 없는 인덱스 교체가 가능하다.
* **Analyzer**는 `Character Filter → Tokenizer → Token Filter` 순서로 텍스트를 토큰화하며, 한국어는 형태소 분석이 가능한 **Nori Analyzer**를 사용한다. 동의어(synonym)와 불용어(stopword)는 색인 시점 또는 검색 시점 중 하나를 선택해 처리할 수 있고, 각각 트레이드오프(재색인 필요 vs 검색 성능 저하)가 있다.
* **Search**는 간단한 조회는 URI 검색으로, 복잡한 조건은 JSON 기반의 **Query DSL**로 수행하며, 점수를 계산하는 **Query Context**(match, multi_match 등)와 점수를 계산하지 않고 빠른 **Filter Context**(term, terms, range)로 구분된다.
* **Match 계열 쿼리**(`match`/`match_phrase`/`match_phrase_prefix`/`multi_match`)로 전문 검색을, **Boolean Query**(`must`/`must_not`/`should`/`filter`)로 여러 조건의 조합 검색을 수행하며, `sort`/`from`+`size`로 정렬과 페이징을 제어한다.
