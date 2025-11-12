# Django DRF 1 정리

지금까지의 Django는 서버가 HTML을 직접 렌더링해 응답했지만, 프론트엔드(Vue 등)와 분리된 서버를 만들기 위한 **REST API**의 개념과, 이를 Django에서 손쉽게 구현하게 해주는 **DRF(Django REST Framework)** 를 정리한 문서입니다.

---

## 1. REST API

### REST란?

* **REST(Representational State Transfer):** 자원(Resource)을 URL로 표현하고, HTTP 메서드로 해당 자원에 대한 행위(CRUD)를 표현하는 API 설계 방식

| 자원 | GET | POST | PUT/PATCH | DELETE |
| --- | --- | --- | --- | --- |
| `/articles/` | 목록 조회 | 새 글 생성 | - | - |
| `/articles/1/` | 상세 조회 | - | 수정 | 삭제 |

* 기존 MTV 패턴의 View는 HTML(Template)을 응답했지만, REST API의 View는 **JSON**을 응답한다는 점이 가장 큰 차이

### 왜 REST API가 필요한가

* 프론트엔드(Vue, React 등)와 백엔드(Django)를 분리해서 개발할 수 있게 해줌 (SPA와의 결합에 필수)
* 하나의 API로 웹/모바일 앱 등 여러 클라이언트에서 동일한 데이터를 재사용 가능

---

## 2. DRF with Single Model

### Serializer

* DRF의 핵심 개념으로, **모델 인스턴스(Python 객체) ↔ JSON** 간의 변환을 담당 (Django Form과 비슷하게, 검증 기능도 함께 제공)

```python
# serializers.py
from rest_framework import serializers
from .models import Article

class ArticleSerializer(serializers.ModelSerializer):
    class Meta:
        model = Article
        fields = "__all__"
```

```python
# views.py
from rest_framework.decorators import api_view
from rest_framework.response import Response
from .models import Article
from .serializers import ArticleSerializer

@api_view(["GET"])
def article_list(request):
    articles = Article.objects.all()
    serializer = ArticleSerializer(articles, many=True)   # QuerySet -> JSON (여러 개는 many=True)
    return Response(serializer.data)

@api_view(["GET"])
def article_detail(request, pk):
    article = get_object_or_404(Article, pk=pk)
    serializer = ArticleSerializer(article)                 # 단일 객체 -> JSON
    return Response(serializer.data)
```

* `@api_view(["GET"])`: 이 View가 허용할 HTTP 메서드를 명시. DRF의 `Response`는 요청 헤더에 따라 JSON 등 적절한 형식으로 자동 변환해 응답

---

## 3. CRUD with ModelSerializer

```python
@api_view(["GET", "POST"])
def article_list(request):
    if request.method == "GET":
        articles = Article.objects.all()
        serializer = ArticleSerializer(articles, many=True)
        return Response(serializer.data)

    elif request.method == "POST":
        serializer = ArticleSerializer(data=request.data)
        if serializer.is_valid(raise_exception=True):    # 유효성 검증 (실패 시 자동 400 응답)
            serializer.save()                               # 검증된 데이터로 저장
            return Response(serializer.data, status=201)

@api_view(["GET", "PUT", "DELETE"])
def article_detail(request, pk):
    article = get_object_or_404(Article, pk=pk)

    if request.method == "GET":
        serializer = ArticleSerializer(article)
        return Response(serializer.data)

    elif request.method == "PUT":
        serializer = ArticleSerializer(article, data=request.data)   # instance + 새 데이터
        if serializer.is_valid(raise_exception=True):
            serializer.save()
            return Response(serializer.data)

    elif request.method == "DELETE":
        article.delete()
        return Response(status=204)
```

* `raise_exception=True`: 검증 실패 시 직접 에러를 처리하지 않아도 DRF가 자동으로 `400 Bad Request`와 에러 메시지를 응답으로 만들어줌
* `serializer.save()`: `instance` 없이 생성하면 `Model.objects.create()`처럼 새로 생성, `instance`가 있으면 해당 인스턴스를 수정

---

## 핵심 요약
* **REST API**는 자원을 URL로, 행위를 HTTP 메서드로 표현하는 설계 방식으로, 프론트엔드/백엔드 분리 개발의 기반이 된다.
* **Serializer**(특히 `ModelSerializer`)는 모델 인스턴스와 JSON을 상호 변환하며, Django Form처럼 유효성 검증 기능도 함께 제공한다.
* DRF의 `@api_view`와 `Response`를 사용하면, GET/POST/PUT/DELETE 요청에 따라 분기해 Serializer로 CRUD 전체를 JSON 기반으로 구현할 수 있다.
