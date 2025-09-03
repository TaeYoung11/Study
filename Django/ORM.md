# Django ORM 정리

SQL을 직접 작성하지 않고 파이썬 코드로 데이터베이스를 조작할 수 있게 해주는 **ORM**과, 데이터 조회의 핵심인 **QuerySet API**, 그리고 이를 View와 연결하는 방법을 정리한 문서입니다.

---

## 1. ORM (Object-Relational Mapping)

* 객체(파이썬 클래스/인스턴스)와 관계형 데이터베이스의 테이블/행을 자동으로 매핑해주는 기술
* 개발자는 SQL 대신 **파이썬 메서드 체이닝**으로 데이터를 다루고, Django가 이를 내부적으로 SQL로 변환해 실행

```python
# ORM 사용 시
articles = Article.objects.filter(title__icontains="파이썬")

# 위 코드가 내부적으로 생성하는 SQL (개념적으로)
# SELECT * FROM articles_article WHERE title LIKE '%파이썬%';
```

* 장점: DB 종류(SQLite, PostgreSQL 등)가 바뀌어도 코드 수정이 거의 필요 없고, SQL Injection 같은 보안 이슈로부터 비교적 안전
* `Model.objects`는 **Manager**라고 부르며, 이 Manager를 통해 QuerySet API를 호출

---

## 2. QuerySet API

### 기본 CRUD

```python
# Create
Article.objects.create(title="제목", content="내용")

# Read
Article.objects.all()                          # 전체 조회
Article.objects.get(pk=1)                       # 단일 조회 (없거나 여러 개면 예외 발생)
Article.objects.filter(title__icontains="파이썬")  # 조건에 맞는 여러 개 조회

# Update
article = Article.objects.get(pk=1)
article.title = "수정된 제목"
article.save()

# Delete
article.delete()
```

### 필드 조회 연산자 (Lookup)

| Lookup | 의미 | 예시 |
| --- | --- | --- |
| `__exact` | 정확히 일치 (기본값) | `title__exact="제목"` |
| `__icontains` | 대소문자 무시 포함 | `title__icontains="장고"` |
| `__gt`, `__lt` | 초과, 미만 | `views__gt=100` |
| `__in` | 목록에 포함 | `pk__in=[1, 2, 3]` |
| `__startswith` | 특정 문자열로 시작 | `title__startswith="공지"` |

### QuerySet의 지연 평가 (Lazy Evaluation)

* `filter()`, `all()` 등은 호출 즉시 DB를 조회하지 않고, **실제로 값이 필요해지는 시점**(반복문, `list()` 변환 등)에 SQL이 실행됨
* 여러 조건을 체이닝해도 최종적으로 한 번의 SQL로 최적화되어 실행됨

```python
qs = Article.objects.filter(title__icontains="파이썬")   # 아직 DB 조회 안 함
qs = qs.exclude(is_deleted=True)                             # 조건 추가 (역시 아직 실행 안 함)
for article in qs:                                             # 여기서 실제 쿼리 실행
    print(article.title)
```

---

## 3. QuerySet API 실습

```python
# 정렬
Article.objects.order_by("-created_at")   # 최신순 (- 는 내림차순)

# 개수
Article.objects.filter(title__icontains="공지").count()

# 존재 여부
Article.objects.filter(pk=999).exists()

# 특정 컬럼만 조회
Article.objects.values("title", "created_at")
```

---

## 4. ORM with View

QuerySet API를 실제 View 로직과 결합해 요청을 처리합니다.

```python
from django.shortcuts import render, get_object_or_404

def index(request):
    articles = Article.objects.order_by("-pk")   # 최신 글이 위로 오도록 정렬
    return render(request, "articles/index.html", {"articles": articles})

def detail(request, pk):
    article = get_object_or_404(Article, pk=pk)   # 없으면 자동으로 404 응답
    return render(request, "articles/detail.html", {"article": article})
```

* `get_object_or_404()`: `Model.objects.get()`과 달리, 데이터가 없을 때 예외를 그대로 노출하지 않고 **404 Not Found 페이지**로 안전하게 처리해줌

---

## 핵심 요약
* **ORM**은 SQL 대신 파이썬 메서드로 DB를 다루게 해주는 계층으로, `Model.objects`(Manager)를 통해 QuerySet API를 호출한다.
* QuerySet은 `filter`, `exclude`, `order_by` 등을 체이닝해도 실제 값이 필요한 시점까지 SQL을 실행하지 않는 **지연 평가** 방식으로 동작한다.
* View에서는 QuerySet으로 조회한 데이터를 템플릿에 전달하며, 단일 객체 조회 시 `get_object_or_404()`를 사용하면 존재하지 않는 데이터에 대해 안전하게 404 응답을 처리할 수 있다.
