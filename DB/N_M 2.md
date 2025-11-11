# DB N:M 2 정리

같은 모델(User) 간의 N:M 관계로 구현하는 **팔로우(Follow) 기능**, 초기 테스트 데이터를 손쉽게 채워 넣는 **Fixtures**, 그리고 N:M 관계 조회 시 성능을 개선하는 **Improve query**를 정리한 문서입니다.

---

## 1. 팔로우 기능 구현 — Self-referencing ManyToMany

* 팔로우는 "User가 User를 팔로우한다"는, **같은 모델끼리 맺는 N:M 관계(Self-referencing)** 라는 점이 좋아요 기능과의 차이

```python
class User(AbstractUser):
    followings = models.ManyToManyField(
        "self",                 # 자기 자신(User) 모델을 참조
        symmetrical=False,       # 팔로우는 짝사랑(비대칭) 관계이므로 False 필수
        related_name="followers",
    )
```

* `symmetrical=False`: 기본값(`True`)은 "내가 A를 관계 맺으면 A도 자동으로 나와 관계를 맺는" 대칭 관계를 가정하지만, 팔로우는 **내가 A를 팔로우해도 A는 나를 팔로우하지 않을 수 있는 비대칭 관계**이므로 반드시 `False`로 지정해야 함
* `related_name="followers"`: `user.followings.all()`(내가 팔로우하는 사람들) ↔ `user.followers.all()`(나를 팔로우하는 사람들)로 양방향을 구분해서 접근

```python
@login_required
def follow(request, user_pk):
    target = get_object_or_404(get_user_model(), pk=user_pk)
    if target != request.user:                 # 자기 자신은 팔로우할 수 없도록 방지
        if target.followers.filter(pk=request.user.pk).exists():
            target.followers.remove(request.user)
        else:
            target.followers.add(request.user)
    return redirect("accounts:profile", user_pk)
```

---

## 2. Fixtures

* 개발/테스트 중 매번 Admin이나 폼으로 데이터를 하나씩 입력하는 것은 비효율적 → **Fixtures**(초기 데이터 파일)를 이용해 한 번에 대량의 테스트 데이터를 적재

```bash
# 현재 DB 데이터를 fixture 파일로 내보내기
python manage.py dumpdata articles.Article --indent 4 > articles/fixtures/articles.json

# fixture 파일을 DB로 불러오기
python manage.py loaddata articles.json
```

```json
[
  {
    "model": "articles.article",
    "pk": 1,
    "fields": { "title": "제목1", "content": "내용1", "user": 1 }
  }
]
```

* 팀원 모두가 동일한 샘플 데이터(게시글, 댓글, 팔로우 관계 등)로 개발/테스트하고 싶을 때, fixture 파일을 저장소에 공유해두면 `loaddata` 한 번으로 동일한 초기 상태를 만들 수 있음

---

## 3. Improve query — N+1 문제와 최적화

### N+1 문제란?

* 게시글 목록을 조회하면서, 각 게시글마다 반복문 안에서 작성자 정보를 다시 조회하면 **게시글 조회 1번 + 게시글 수(N)번의 추가 쿼리**가 발생하는 비효율적인 상황

```python
articles = Article.objects.all()          # 쿼리 1번
for article in articles:
    print(article.user.username)            # 매 반복마다 추가 쿼리 발생! (N번)
```

### `select_related` — N:1, 1:1 관계 최적화

* 정참조(FK) 관계를 **SQL JOIN**으로 미리 한 번에 가져와, 반복문에서 추가 쿼리가 발생하지 않도록 함

```python
articles = Article.objects.select_related("user").all()   # JOIN으로 한 번에 조회
for article in articles:
    print(article.user.username)    # 추가 쿼리 없음
```

### `prefetch_related` — N:M, 역참조(1:N) 관계 최적화

* JOIN으로 한 번에 합치기 어려운 N:M/역참조 관계는, **별도의 쿼리 한 번을 추가로 실행**해 결과를 메모리에서 미리 연결해둠

```python
articles = Article.objects.prefetch_related("like_users").all()   # 총 2번의 쿼리로 해결
for article in articles:
    print(article.like_users.count())    # 추가 쿼리 없음
```

| 최적화 함수 | 적합한 관계 | 방식 |
| --- | --- | --- |
| `select_related` | N:1, 1:1 (정참조) | SQL JOIN으로 한 번에 조회 |
| `prefetch_related` | N:M, 1:N(역참조) | 관련 쿼리를 별도로 한 번 더 실행 후 메모리에서 결합 |

---

## 핵심 요약
* 팔로우 기능은 User 모델 자신을 참조하는 `ManyToManyField("self", symmetrical=False)`로 구현하며, `symmetrical=False`가 비대칭 관계 표현의 핵심이다.
* **Fixtures**는 `dumpdata`/`loaddata`로 초기 테스트 데이터를 파일로 관리해, 팀 전체가 동일한 샘플 데이터를 빠르게 재현할 수 있게 해준다.
* 반복문 안에서 관계 필드에 접근할 때 발생하는 **N+1 문제**는 N:1 관계는 `select_related`(JOIN), N:M/역참조 관계는 `prefetch_related`(추가 쿼리 후 결합)로 최적화한다.
