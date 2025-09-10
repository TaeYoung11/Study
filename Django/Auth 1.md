# Django Auth 1 정리

Django의 정적/미디어 파일 처리를 복습하고, 로그인 상태를 유지하는 **쿠키와 세션**, Django가 기본 제공하는 **인증 시스템**, 그리고 **커스텀 유저 모델**과 **로그인** 구현을 정리한 문서입니다.

---

## 1. Django Static Review

* Static/Media 설정(`STATIC_URL`, `MEDIA_URL` 등)을 복습하며, 이번 단계부터는 로그인한 사용자별로 다른 화면(자신의 글만 수정 가능 등)을 만들기 위한 **인증(Authentication)** 을 본격적으로 다룸

---

## 2. Cookie & Session

### 쿠키(Cookie)

* HTTP는 기본적으로 **무상태(Stateless)** 프로토콜이라, 매 요청이 독립적이며 이전 요청을 기억하지 못함
* **쿠키:** 서버가 응답 시 브라우저에 저장하도록 지시하는 작은 데이터 조각으로, 이후 요청마다 브라우저가 자동으로 함께 전송

### 세션(Session)

* 쿠키에 민감한 정보(로그인 여부 등)를 직접 담으면 위험하므로, 서버는 **세션 저장소**에 실제 데이터를 저장하고 브라우저에는 **세션 ID만** 쿠키로 전달
* 이후 요청마다 브라우저가 세션 ID 쿠키를 함께 보내면, 서버가 이 ID로 세션 저장소를 조회해 로그인 여부를 판별

```
[로그인 성공] 서버: 세션 생성(세션ID: abc123) → 브라우저: Cookie: sessionid=abc123 저장
[이후 요청]    브라우저: Cookie: sessionid=abc123 전송 → 서버: 세션 저장소에서 abc123 조회 → 로그인 상태 확인
```

---

## 3. Django Authentication System

* Django는 `django.contrib.auth` 라는 인증 앱을 기본 내장하고 있어, User 모델과 로그인/로그아웃/권한 확인 기능을 바로 사용할 수 있음

```python
from django.contrib.auth import authenticate, login, logout

user = authenticate(request, username="test", password="1234")  # 자격 증명 검증
if user is not None:
    login(request, user)    # 세션 생성 (로그인 처리)
```

* `request.user`: 현재 요청을 보낸 사용자 객체. 로그인하지 않았다면 `AnonymousUser` 인스턴스
* `request.user.is_authenticated`: 로그인 여부를 True/False로 확인

---

## 4. Custom User Model

* Django 기본 `User` 모델은 이메일 로그인, 추가 프로필 필드 등 커스터마이징이 필요할 때 확장이 번거로움 → **프로젝트 시작 시점에** 커스텀 유저 모델을 미리 설정해두는 것이 정석

```python
# accounts/models.py
from django.contrib.auth.models import AbstractUser

class User(AbstractUser):
    nickname = models.CharField(max_length=50, blank=True)   # 필요한 필드 추가
```

```python
# settings.py
AUTH_USER_MODEL = "accounts.User"   # 프로젝트 전체가 이 커스텀 모델을 기본 User로 사용하도록 지정
```

> `AUTH_USER_MODEL`은 **마이그레이션을 한 번도 실행하지 않은 프로젝트 초기에만** 안전하게 변경할 수 있으므로, 프로젝트를 시작하자마자 커스텀 유저 모델부터 설정하는 것이 일반적인 관례입니다.

---

## 5. Login

```python
# forms.py
from django.contrib.auth.forms import AuthenticationForm

# views.py
from django.contrib.auth import login as auth_login

def login(request):
    if request.method == "POST":
        form = AuthenticationForm(request, data=request.POST)
        if form.is_valid():
            auth_login(request, form.get_user())
            return redirect("articles:index")
    else:
        form = AuthenticationForm()
    return render(request, "accounts/login.html", {"form": form})
```

* Django가 기본 제공하는 `AuthenticationForm`을 사용하면, 아이디/비밀번호 검증 로직을 직접 작성하지 않아도 됨

---

## 6. Template with Authentication data

```html
{% if request.user.is_authenticated %}
  <p>{{ request.user.username }}님 환영합니다!</p>
  <a href="{% url 'accounts:logout' %}">로그아웃</a>
{% else %}
  <a href="{% url 'accounts:login' %}">로그인</a>
{% endif %}
```

* `request.user`는 템플릿에서도 그대로 접근 가능해, 로그인 여부에 따라 다른 화면을 손쉽게 렌더링할 수 있음

---

## 핵심 요약
* HTTP는 무상태 프로토콜이므로, **쿠키(세션 ID) + 서버의 세션 저장소** 조합으로 로그인 상태를 유지한다.
* Django의 `django.contrib.auth`는 `authenticate`/`login`/`logout` 함수와 `request.user`로 인증 처리를 표준화된 방식으로 제공한다.
* 프로필 필드 확장 등을 위해서는 프로젝트 초기에 **커스텀 유저 모델(`AbstractUser` 상속)** 을 `AUTH_USER_MODEL`에 지정해두는 것이 이후 마이그레이션 문제를 피하는 정석적인 방법이다.
