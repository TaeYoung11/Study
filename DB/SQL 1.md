# DB SQL 1 정리

관계형 데이터베이스(Relational Database)의 개념을 살펴보고, 데이터를 조회·정렬·필터링·그룹화하는 기본 SQL 문법을 정리한 문서입니다.

---

## 1. Relational Database

* **관계형 데이터베이스(RDB):** 데이터를 행(Row)과 열(Column)로 구성된 **테이블(Table)** 형태로 저장하고, 테이블 간의 **관계(Relation)** 로 데이터를 연결하는 데이터베이스
* **SQL(Structured Query Language):** 관계형 데이터베이스에 데이터를 정의·조작·조회하기 위한 표준 질의 언어

### SQL 문법의 분류

| 분류 | 설명 | 대표 명령어 |
| --- | --- | --- |
| **DDL** (Data Definition Language) | 테이블 등 데이터 구조를 정의 | `CREATE`, `ALTER`, `DROP` |
| **DML** (Data Manipulation Language) | 데이터를 조작(조회/삽입/수정/삭제) | `SELECT`, `INSERT`, `UPDATE`, `DELETE` |
| **DCL** (Data Control Language) | 권한을 제어 | `GRANT`, `REVOKE` |

---

## 2. Querying data — 기본 조회

```sql
SELECT * FROM articles;                       -- 모든 컬럼, 모든 행 조회
SELECT title, created_at FROM articles;        -- 특정 컬럼만 조회
SELECT DISTINCT category FROM articles;         -- 중복 제거한 값만 조회
```

---

## 3. Sorting data — 정렬

```sql
SELECT * FROM articles ORDER BY created_at DESC;         -- 최신순(내림차순)
SELECT * FROM articles ORDER BY category ASC, title DESC; -- 다중 기준 정렬
```

* `ASC`(오름차순, 기본값), `DESC`(내림차순)

---

## 4. Filtering data — 조건 필터링

```sql
SELECT * FROM articles WHERE category = '공지';
SELECT * FROM articles WHERE views > 100 AND category = '공지';
SELECT * FROM articles WHERE title LIKE '%파이썬%';        -- 부분 문자열 포함
SELECT * FROM articles WHERE category IN ('공지', '이벤트'); -- 목록에 포함
SELECT * FROM articles WHERE created_at BETWEEN '2025-01-01' AND '2025-12-31';
SELECT * FROM articles WHERE deleted_at IS NULL;             -- NULL 여부 확인 (= 대신 IS 사용)
```

* `LIKE`의 `%`는 0개 이상의 임의 문자를 의미하는 와일드카드 (`_`는 정확히 한 글자)
* NULL 비교는 `=`이 아닌 반드시 `IS NULL` / `IS NOT NULL`을 사용해야 함 (`NULL = NULL`은 참이 아니라 알 수 없음(UNKNOWN)으로 처리됨)

---

## 5. Grouping data — 그룹화

```sql
SELECT category, COUNT(*) AS cnt
FROM articles
GROUP BY category;                      -- 카테고리별 게시글 수

SELECT category, AVG(views) AS avg_views
FROM articles
GROUP BY category
HAVING AVG(views) > 50;                  -- 그룹화 결과에 대한 조건 필터링
```

* **집계 함수:** `COUNT()`, `SUM()`, `AVG()`, `MAX()`, `MIN()`
* `WHERE`은 그룹화 이전의 개별 행에 대한 조건, `HAVING`은 그룹화 이후 집계 결과에 대한 조건이라는 차이가 있음 (그래서 `HAVING`에는 집계 함수를 조건으로 쓸 수 있음)

---

## 핵심 요약
* 관계형 데이터베이스는 데이터를 테이블로 저장하고 관계로 연결하며, SQL은 이를 조작하는 표준 언어로 DDL/DML/DCL로 분류된다.
* 데이터 조회는 `SELECT`, 정렬은 `ORDER BY`, 조건 필터링은 `WHERE`(`LIKE`, `IN`, `BETWEEN`, `IS NULL` 등)로 수행한다.
* 그룹화는 `GROUP BY` + 집계 함수로 수행하며, 그룹화 이전 조건은 `WHERE`, 그룹화 이후 집계 결과에 대한 조건은 `HAVING`으로 구분해서 사용한다.
