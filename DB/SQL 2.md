# DB SQL 2 정리

테이블을 만들고 구조를 변경하는 **Managing Tables**, 데이터를 삽입·수정·삭제하는 **Modifying Data**, 그리고 여러 테이블을 연결해 조회하는 **Multi table queries(JOIN)** 를 정리한 문서입니다.

---

## 1. Managing Tables

```sql
CREATE TABLE articles (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    title VARCHAR(100) NOT NULL,
    content TEXT,
    category_id INTEGER,
    FOREIGN KEY (category_id) REFERENCES categories(id)
);

ALTER TABLE articles ADD COLUMN views INTEGER DEFAULT 0;   -- 컬럼 추가
ALTER TABLE articles DROP COLUMN views;                       -- 컬럼 삭제

DROP TABLE articles;    -- 테이블 전체 삭제 (구조까지 삭제, 복구 불가에 주의)
```

* `PRIMARY KEY`: 각 행을 고유하게 식별하는 기본키
* `FOREIGN KEY`: 다른 테이블의 기본키를 참조해 두 테이블 간의 관계를 표현하는 외래키
* `NOT NULL`, `DEFAULT`: 컬럼에 대한 제약 조건 지정

---

## 2. Modifying Data

```sql
-- Create
INSERT INTO articles (title, content) VALUES ('제목', '내용');

-- Update
UPDATE articles SET views = views + 1 WHERE id = 1;

-- Delete
DELETE FROM articles WHERE id = 1;
```

* `UPDATE`/`DELETE`에서 `WHERE`을 빠뜨리면 **테이블의 모든 행**이 수정/삭제되므로 각별히 주의해야 함

---

## 3. Multi table queries — JOIN

여러 테이블에 나뉘어 저장된 데이터를 하나의 결과로 합쳐서 조회할 때 사용합니다.

```sql
-- INNER JOIN: 양쪽 테이블에 모두 데이터가 있는 경우만 결합
SELECT articles.title, categories.name
FROM articles
INNER JOIN categories ON articles.category_id = categories.id;

-- LEFT JOIN: 왼쪽(articles) 테이블은 모두 포함, 오른쪽에 없으면 NULL
SELECT articles.title, categories.name
FROM articles
LEFT JOIN categories ON articles.category_id = categories.id;
```

| JOIN 종류 | 설명 |
| --- | --- |
| `INNER JOIN` | 두 테이블에 공통으로 존재하는 데이터만 결합 |
| `LEFT JOIN` | 왼쪽 테이블 전체 + 오른쪽에서 매칭되는 데이터 (없으면 NULL) |
| `RIGHT JOIN` | 오른쪽 테이블 전체 + 왼쪽에서 매칭되는 데이터 (없으면 NULL) |

* Django ORM의 `article.category`, `article.comments.all()` 같은 관계 접근은 내부적으로 이러한 JOIN 쿼리로 변환되어 실행됨

---

## 참고

* SQL을 직접 다루는 이유는, ORM이 내부적으로 어떤 쿼리를 만들어내는지 이해하고 있어야 **복잡한 조회나 성능 튜닝**이 필요할 때 원인을 정확히 진단할 수 있기 때문
* `EXPLAIN` 명령으로 쿼리의 실행 계획을 확인하면, 인덱스가 잘 활용되고 있는지 등을 점검할 수 있음

---

## 핵심 요약
* 테이블 구조는 `CREATE TABLE`/`ALTER TABLE`/`DROP TABLE`로 관리하며, `PRIMARY KEY`/`FOREIGN KEY`로 데이터의 고유성과 테이블 간 관계를 정의한다.
* 데이터 조작은 `INSERT`(생성), `UPDATE`(수정), `DELETE`(삭제)로 수행하며, `WHERE` 절을 빠뜨리면 전체 행에 영향을 준다는 점에 항상 주의해야 한다.
* 여러 테이블에 나뉜 데이터는 `JOIN`(INNER/LEFT/RIGHT)으로 연결해 조회하며, 이는 Django ORM의 관계 필드 접근이 내부적으로 수행하는 동작과 동일하다.
