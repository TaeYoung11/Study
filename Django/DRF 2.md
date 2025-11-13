# Django DRF 2 정리

여러 모델이 얽힌 **1:N 관계**를 DRF로 표현하는 방법, 자식 쪽에서 부모 데이터를 함께 보여주는 **역참조 데이터 구성**, 그리고 프론트엔드 협업을 위한 **API 문서화**를 정리한 문서입니다.

---

## 1. DRF with 1:N Relation

* 게시글(Article) - 댓글(Comment)처럼 1:N 관계가 있는 모델을 어떻게 직렬화(Serialize)할지가 핵심 주제

```python
class Comment(models.Model):
    article = models.ForeignKey(Article, on_delete=models.CASCADE, related_name="comments")
    content = models.CharField(max_length=200)
```

### 댓글에서 게시글 표현 — Nested Serializer

```python
class ArticleSerializer(serializers.ModelSerializer):
    class Meta:
        model = Article
        fields = ("id", "title", "content")

class CommentSerializer(serializers.ModelSerializer):
    article = ArticleSerializer(read_only=True)   # 댓글 안에 게시글 정보를 중첩해서 표현

    class Meta:
        model = Comment
        fields = "__all__"
```

* `read_only=True`: 이 필드는 응답(직렬화) 시에만 사용하고, 댓글 생성 요청(역직렬화) 시에는 사용하지 않겠다는 의미 (게시글 자체를 댓글 생성 요청으로 새로 만들지 않도록 방지)

---

## 2. 역참조 데이터 구성

* `ForeignKey`는 "자식(Comment) → 부모(Article)" 방향의 참조이며, 반대로 "부모 → 자식들" 방향으로 접근하는 것을 **역참조**라고 함
* `related_name`으로 지정한 이름이 바로 역참조 시 사용하는 속성명이 됨 (지정하지 않으면 기본값 `comment_set`)

```python
article = Article.objects.get(pk=1)
article.comments.all()   # related_name="comments" 로 지정했으므로 이렇게 접근 가능
```

### 게시글 안에 댓글 목록을 함께 응답하기

```python
class ArticleSerializer(serializers.ModelSerializer):
    comment_set = CommentSerializer(many=True, read_only=True)   # 역참조 필드도 Nested로 표현 가능

    class Meta:
        model = Article
        fields = "__all__"
```

```json
{
  "id": 1,
  "title": "제목",
  "comment_set": [
    {"id": 1, "content": "댓글1"},
    {"id": 2, "content": "댓글2"}
  ]
}
```

* 게시글 상세 조회 API 하나로 게시글 정보와 댓글 목록을 한 번에 응답할 수 있어, 프론트엔드에서 별도의 추가 요청 없이 화면을 구성할 수 있게 됨

---

## 3. API 문서화

### 문서화가 필요한 이유

* 프론트엔드와 백엔드를 분리해서 개발할 때, API의 요청/응답 형식이 명확히 문서화되어 있지 않으면 협업 과정에서 혼선이 생기기 쉬움

### drf-yasg / drf-spectacular를 활용한 자동 문서화

```python
# settings.py
INSTALLED_APPS += ["drf_yasg"]

# urls.py
from drf_yasg.views import get_schema_view
from drf_yasg import openapi

schema_view = get_schema_view(
    openapi.Info(title="Article API", default_version="v1"),
    public=True,
)

urlpatterns += [
    path("swagger/", schema_view.with_ui("swagger", cache_timeout=0)),
]
```

* Serializer/View 코드로부터 **Swagger UI** 형태의 API 문서를 자동 생성해, 별도로 문서를 손으로 작성하지 않아도 실시간으로 최신 API 스펙을 공유할 수 있음
* 각 엔드포인트별로 요청 파라미터, 응답 예시, 상태 코드를 브라우저에서 직접 확인하고 테스트도 가능

---

## 핵심 요약
* 1:N 관계는 `ForeignKey` + `related_name`으로 구현하며, DRF에서는 **Nested Serializer**로 관계된 모델의 데이터를 중첩된 JSON 구조로 함께 응답할 수 있다.
* `related_name`으로 지정한 이름은 **역참조**(부모→자식들 접근) 시 그대로 사용되며, 게시글 Serializer에 댓글 목록을 중첩시키면 한 번의 요청으로 관련 데이터를 모두 응답할 수 있다.
* **API 문서화** 도구(drf-yasg 등)를 사용하면 코드로부터 Swagger 문서를 자동 생성해, 프론트엔드와의 협업 시 API 스펙을 항상 최신 상태로 공유할 수 있다.
