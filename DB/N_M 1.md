# DB N:M 1 정리

두 테이블이 서로 여러 개씩 대응되는 **N:M(다대다) 관계**의 개념과, Django에서 이를 표현하는 `ManyToManyField`, 그리고 실전 예제인 **좋아요(Like) 기능 구현**을 정리한 문서입니다.

---

## 1. Many to many relationships (N:M 관계)

* **N:M 관계:** 한쪽 테이블의 여러 행이, 다른 쪽 테이블의 여러 행과 대응되는 관계
* 예: 한 명의 사용자가 여러 게시글에 "좋아요"를 누를 수 있고, 하나의 게시글도 여러 사용자로부터 "좋아요"를 받을 수 있음 (User ↔ Article, 양쪽 모두 N)

### N:M 관계는 왜 중개 테이블이 필요한가

* 1:N과 달리, N:M은 어느 한쪽 테이블에만 FK를 두는 방식으로 표현할 수 없음 (한쪽에 여러 값을 저장해야 하는데, 관계형 DB의 한 컬럼에는 원자값 하나만 저장 가능하기 때문)
* 따라서 두 테이블 사이에 **중개 테이블(Junction Table)** 을 별도로 두어, 이 중개 테이블이 양쪽 테이블을 각각 N:1로 참조하도록 구성

```sql
CREATE TABLE article_likes (
    article_id INTEGER REFERENCES articles(id),
    user_id INTEGER REFERENCES users(id),
    PRIMARY KEY (article_id, user_id)   -- 같은 조합의 좋아요 중복 방지
);
```

---

## 2. ManyToManyField

Django ORM은 이 중개 테이블을 자동으로 만들어주는 `ManyToManyField`를 제공합니다.

```python
class Article(models.Model):
    title = models.CharField(max_length=100)
    like_users = models.ManyToManyField(settings.AUTH_USER_MODEL, related_name="like_articles")
```

* `ManyToManyField`는 관계를 정의하는 쪽(여기서는 Article) 모델에만 선언하면 되며, Django가 중개 테이블을 알아서 생성
* `related_name`: 반대쪽(User)에서 역참조할 때 사용할 이름 (`user.like_articles.all()`)

```python
article.like_users.all()          # 이 게시글을 좋아요한 사용자 목록
article.like_users.add(user)        # 좋아요 추가
article.like_users.remove(user)      # 좋아요 취소
article.like_users.count()            # 좋아요 개수

user.like_articles.all()              # 이 사용자가 좋아요한 게시글 목록 (역참조)
```

---

## 3. 좋아요 기능 구현

```python
@login_required
def likes(request, article_pk):
    article = get_object_or_404(Article, pk=article_pk)
    user = request.user

    if article.like_users.filter(pk=user.pk).exists():   # 이미 좋아요를 눌렀는지 확인
        article.like_users.remove(user)                     # 이미 눌렀다면 취소 (토글)
    else:
        article.like_users.add(user)                          # 안 눌렀다면 추가

    return redirect("articles:detail", article_pk)
```

```html
<!-- 템플릿에서 좋아요 상태에 따라 버튼 표시 분기 -->
{% if request.user in article.like_users.all %}
  <button>좋아요 취소 ({{ article.like_users.count }})</button>
{% else %}
  <button>좋아요 ({{ article.like_users.count }})</button>
{% endif %}
```

* `filter(pk=user.pk).exists()`: 전체 목록을 가져오지 않고 존재 여부만 효율적으로 확인
* 하나의 View에서 "좋아요 추가/취소"를 상태에 따라 분기하는 **토글(Toggle) 패턴**이 자주 사용됨

---

## 핵심 요약
* **N:M 관계**는 양쪽 테이블이 서로 여러 개씩 대응되는 관계로, 한쪽에 FK를 두는 방식으로는 표현이 불가능해 **중개 테이블**이 반드시 필요하다.
* Django의 `ManyToManyField`는 이 중개 테이블 생성과 관리를 자동으로 처리해주며, `add()`/`remove()`/`filter().exists()` 등으로 관계를 다룬다.
* 좋아요 기능은 현재 좋아요 여부를 확인해 `add`/`remove`를 분기하는 **토글 패턴**으로 구현하는 것이 일반적이다.
