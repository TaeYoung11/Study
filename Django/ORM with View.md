# Django ORM with View 정리

게시글 데이터를 다루는 **Read, Create**부터 **Update, Delete**까지, ORM과 View를 결합한 CRUD 전체 흐름과 **HTTP 요청 메서드**, **응답 상태 코드**, **Redirect**를 정리한 문서입니다.

---

## 1. Read

```python
def index(request):
    articles = Article.objects.all()
    return render(request, "articles/index.html", {"articles": articles})

def detail(request, pk):
    article = get_object_or_404(Article, pk=pk)
    return render(request, "articles/detail.html", {"article": article})
```

---

## 2. Create

폼을 통해 사용자로부터 데이터를 입력받아 새로운 객체를 생성하는 흐름입니다.

```python
def create(request):
    if request.method == "POST":
        title = request.POST.get("title")
        content = request.POST.get("content")
        article = Article.objects.create(title=title, content=content)
        return redirect("articles:detail", article.pk)
    return render(request, "articles/create.html")
```

* 하나의 View 함수에서 `GET`(입력 폼 보여주기)과 `POST`(실제 데이터 저장) 두 가지 메서드를 함께 처리하는 패턴이 자주 사용됨

---

## 3. HTTP Request methods

| 메서드 | 용도 |
| --- | --- |
| **GET** | 데이터 조회 (URL에 파라미터 노출, 캐시 가능) |
| **POST** | 데이터 생성 (본문에 데이터 포함, 민감한 정보 전송에 적합) |
| **PUT / PATCH** | 데이터 전체/부분 수정 |
| **DELETE** | 데이터 삭제 |

* Django의 기본 폼 기반 View는 브라우저 폼이 `GET`/`POST`만 지원하기 때문에 주로 이 둘을 사용하며, PUT/PATCH/DELETE는 이후 DRF(Django REST Framework)에서 본격적으로 다룸

---

## 4. HTTP response status code

| 코드 | 의미 |
| --- | --- |
| `200 OK` | 요청 성공 |
| `201 Created` | 생성 성공 |
| `301/302` | 리다이렉트 (영구/임시) |
| `400 Bad Request` | 잘못된 요청 (유효성 검증 실패 등) |
| `403 Forbidden` | 권한 없음 |
| `404 Not Found` | 리소스를 찾을 수 없음 |
| `500 Internal Server Error` | 서버 내부 오류 |

---

## 5. Redirect

```python
from django.shortcuts import redirect

def create(request):
    # ... 저장 로직 ...
    return redirect("articles:detail", article.pk)   # name 기반 리다이렉트 (권장)
    # return redirect(f"/articles/{article.pk}/")     # 경로를 직접 문자열로 지정하는 방식 (지양)
```

* **PRG 패턴(Post-Redirect-Get):** `POST` 요청 처리 후에는 같은 URL을 다시 `render`하지 않고 `redirect`로 다른 GET 요청을 유도 → 새로고침 시 폼이 중복 제출되는 문제를 방지

---

## 6. Delete

```python
def delete(request, pk):
    article = get_object_or_404(Article, pk=pk)
    if request.method == "POST":       # 안전을 위해 POST 요청에서만 삭제 수행
        article.delete()
        return redirect("articles:index")
    return redirect("articles:detail", pk)
```

* 삭제처럼 상태를 변경하는 작업은 **GET 요청으로 처리하지 않는 것이 원칙** (링크 클릭만으로 삭제되는 것을 방지하기 위해, 폼의 `POST`로 감싸서 요청)

---

## 7. Update

```python
def update(request, pk):
    article = get_object_or_404(Article, pk=pk)
    if request.method == "POST":
        article.title = request.POST.get("title")
        article.content = request.POST.get("content")
        article.save()
        return redirect("articles:detail", article.pk)
    return render(request, "articles/update.html", {"article": article})
```

* Create와 마찬가지로 `GET`(수정 폼 보여주기)과 `POST`(실제 수정 반영)을 하나의 View에서 함께 처리
* 기존 데이터를 폼에 미리 채워 보여줘야 하므로, `GET` 처리 시 조회한 `article`을 그대로 템플릿에 전달

---

## 핵심 요약
* Django의 폼 기반 CRUD는 하나의 View 함수 안에서 `request.method`(GET/POST)를 분기해 "입력 폼 표시"와 "실제 처리"를 함께 담당하는 패턴이 일반적이다.
* 데이터를 변경하는 Create/Update/Delete는 `POST` 요청으로 처리하고, 처리 후에는 **PRG 패턴**에 따라 `redirect`로 응답해 새로고침 시 중복 제출을 방지한다.
* HTTP 상태 코드(`200`, `201`, `404` 등)는 요청의 처리 결과를 클라이언트에게 명확히 전달하는 역할을 한다.
