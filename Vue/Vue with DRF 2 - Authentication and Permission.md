# Vue with DRF 2 - Authentication and Permission 정리

Vue-DRF 분리 구조에서 로그인 상태를 유지하는 **Authentication**과, 로그인한 사용자에 따라 접근 가능한 범위를 제한하는 **Permission**을 정리한 문서입니다.

---

## 1. Authentication with DRF

### 세션 인증의 한계

* 기존 Django 세션 인증은 서버가 브라우저의 쿠키를 기준으로 로그인 여부를 판단하는데, Vue가 별도 서버/도메인에서 동작하면 쿠키 공유 및 CORS 설정이 까다로워짐
* 이런 분리형 구조에서는 **토큰 기반 인증(Token Authentication, JWT 등)** 이 더 널리 사용됨

### DRF에 토큰 인증 적용

```bash
pip install djangorestframework-simplejwt
```

```python
# settings.py
REST_FRAMEWORK = {
    "DEFAULT_AUTHENTICATION_CLASSES": (
        "rest_framework_simplejwt.authentication.JWTAuthentication",
    ),
}

# urls.py
from rest_framework_simplejwt.views import TokenObtainPairView, TokenRefreshView

urlpatterns += [
    path("token/", TokenObtainPairView.as_view()),      # 로그인 -> Access/Refresh Token 발급
    path("token/refresh/", TokenRefreshView.as_view()),   # Refresh Token으로 Access Token 재발급
]
```

### Vue에서 로그인 및 토큰 저장

```javascript
async function login(username, password) {
    const { data } = await api.post("token/", { username, password });
    localStorage.setItem("access_token", data.access);
    localStorage.setItem("refresh_token", data.refresh);
}
```

```javascript
// 이후 모든 요청에 Access Token을 자동으로 담아 보내는 axios interceptor
api.interceptors.request.use((config) => {
    const token = localStorage.getItem("access_token");
    if (token) {
        config.headers.Authorization = `Bearer ${token}`;
    }
    return config;
});
```

---

## 2. Permission with DRF

### 권한(Permission) 클래스

* **인증(Authentication)** 이 "누구인지 확인"하는 것이라면, **권한(Permission)** 은 "그 사람이 이 요청을 수행해도 되는지"를 판단하는 단계

```python
from rest_framework.permissions import IsAuthenticated, IsAuthenticatedOrReadOnly

@api_view(["GET", "POST"])
@permission_classes([IsAuthenticatedOrReadOnly])   # 조회는 누구나, 생성은 로그인 사용자만
def article_list(request):
    ...
```

| Permission 클래스 | 설명 |
| --- | --- |
| `AllowAny` | 누구나 접근 가능 (기본값) |
| `IsAuthenticated` | 로그인한 사용자만 접근 가능 |
| `IsAuthenticatedOrReadOnly` | 조회(GET)는 누구나, 변경(POST/PUT/DELETE)은 로그인 사용자만 |
| 커스텀 Permission | `has_permission`, `has_object_permission`을 오버라이드해 직접 규칙 정의 |

### 작성자 본인만 수정/삭제 가능하게 만들기 (커스텀 Permission)

```python
from rest_framework.permissions import BasePermission

class IsOwnerOrReadOnly(BasePermission):
    def has_object_permission(self, request, view, obj):
        if request.method in ("GET", "HEAD", "OPTIONS"):
            return True                          # 조회는 항상 허용
        return obj.user == request.user            # 수정/삭제는 작성자 본인만 허용
```

* Vue 쪽에서는 이 권한 실패 시 DRF가 응답하는 `403 Forbidden` 상태 코드를 확인해, 사용자에게 "권한이 없다"는 메시지를 적절히 표시

---

## 핵심 요약
* Vue-DRF 분리 구조에서는 세션 대신 **JWT 같은 토큰 기반 인증**이 일반적이며, Vue는 로그인 시 발급받은 토큰을 저장해 이후 요청 헤더(`Authorization: Bearer`)에 자동으로 포함시킨다.
* DRF의 **Permission 클래스**는 인증된 사용자인지 확인하는 것을 넘어, "이 요청을 수행할 자격이 있는지"까지 세밀하게 제어한다.
* 작성자 본인만 수정/삭제를 허용하는 등의 세부 규칙은 `BasePermission`을 상속한 **커스텀 Permission**의 `has_object_permission`으로 구현한다.
