# JS 비동기 JS with Django 정리

앞서 배운 AJAX/Promise를 실제 Django DRF 서버와 연결해, 페이지 새로고침 없이 **팔로우 기능**과 **좋아요 기능**을 구현하는 방법을 정리한 문서입니다.

---

## 1. Ajax with follow

### 기존 방식의 한계

* Django의 폼 기반 팔로우 기능(`<form method="POST">`)은 클릭할 때마다 **페이지 전체가 새로고침**되어 사용자 경험이 매끄럽지 않음
* AJAX로 요청을 보내면, 팔로우 버튼의 텍스트/카운트만 즉시 갱신하고 나머지 화면은 그대로 유지할 수 있음

```javascript
document.querySelector("#follow-btn").addEventListener("click", async (e) => {
    const btn = e.target;
    const userPk = btn.dataset.userPk;

    const response = await fetch(`/accounts/${userPk}/follow/`, {
        method: "POST",
        headers: {
            "X-CSRFToken": getCookie("csrftoken"),   // Django의 CSRF 보호 통과를 위해 필요
        },
    });
    const data = await response.json();

    btn.textContent = data.is_followed ? "언팔로우" : "팔로우";   // 서버 응답에 따라 버튼 상태 갱신
    document.querySelector("#follower-count").textContent = data.follower_count;
});
```

```python
# views.py (DRF)
@api_view(["POST"])
@permission_classes([IsAuthenticated])
def follow(request, user_pk):
    target = get_object_or_404(get_user_model(), pk=user_pk)
    if target != request.user:
        if target.followers.filter(pk=request.user.pk).exists():
            target.followers.remove(request.user)
            is_followed = False
        else:
            target.followers.add(request.user)
            is_followed = True
    return Response({
        "is_followed": is_followed,
        "follower_count": target.followers.count(),
    })
```

* Django는 `POST` 요청에 **CSRF 토큰**을 요구하므로, AJAX 요청 헤더에도 `X-CSRFToken`을 함께 담아야 함 (쿠키에서 토큰 값을 읽어오는 `getCookie` 유틸 함수가 자주 사용됨)

---

## 2. Ajax with likes

```javascript
async function toggleLike(articlePk) {
    const response = await fetch(`/api/articles/${articlePk}/likes/`, {
        method: "POST",
        headers: { "X-CSRFToken": getCookie("csrftoken") },
    });
    const data = await response.json();

    const likeBtn = document.querySelector(`#like-btn-${articlePk}`);
    likeBtn.textContent = `좋아요 ${data.like_count}`;
    likeBtn.classList.toggle("liked", data.is_liked);   // liked 여부에 따라 스타일 클래스 토글
}
```

```python
@api_view(["POST"])
@permission_classes([IsAuthenticated])
def likes(request, article_pk):
    article = get_object_or_404(Article, pk=article_pk)
    if article.like_users.filter(pk=request.user.pk).exists():
        article.like_users.remove(request.user)
        is_liked = False
    else:
        article.like_users.add(request.user)
        is_liked = True
    return Response({
        "is_liked": is_liked,
        "like_count": article.like_users.count(),
    })
```

### 프론트-백엔드 통신 흐름 정리

```
1. 사용자가 좋아요/팔로우 버튼 클릭
2. JS가 fetch()로 DRF API에 POST 요청 (CSRF 토큰 포함)
3. Django View가 토글 로직 수행 후, 최신 상태(JSON)를 응답
4. JS가 응답을 받아 버튼 텍스트/카운트/스타일만 부분적으로 갱신 (페이지 새로고침 없음)
```

---

## 핵심 요약
* AJAX로 팔로우/좋아요를 처리하면, 페이지 전체를 새로고침하지 않고 **버튼 상태와 카운트만 즉시 갱신**할 수 있어 사용자 경험이 크게 개선된다.
* Django에 `POST` 요청을 보낼 때는 반드시 **CSRF 토큰**을 요청 헤더에 포함해야 하며, 서버는 처리 결과(토글 여부, 최신 카운트)를 JSON으로 응답한다.
* 이 패턴은 "클릭 → fetch로 API 호출 → 응답 받은 최신 상태로 DOM 일부만 갱신"이라는 흐름으로, 이후 배우게 될 Vue 등 프론트엔드 프레임워크에서도 동일하게 이어지는 핵심 원리이다.
