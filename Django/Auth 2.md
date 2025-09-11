# Django Auth 2 정리

로그인 기능을 복습하고, **로그아웃**, `AbstractUser`를 활용한 **회원가입/회원 탈퇴**, 그리고 **로그인 사용자에 대한 접근 제한**을 정리한 문서입니다.

---

## 1. Login Review

* `AuthenticationForm`으로 자격 증명을 검증하고, `login()`으로 세션을 생성해 로그인 처리하는 흐름을 복습

---

## 2. Logout

```python
from django.contrib.auth import logout as auth_logout

def logout(request):
    auth_logout(request)     # 현재 요청의 세션 데이터를 삭제
    return redirect("articles:index")
```

* `logout()`은 현재 사용자의 세션을 완전히 비워, 이후 요청에서 `request.user`가 다시 `AnonymousUser`가 되도록 만듦
* 세션 관련 상태를 변경하는 작업이므로, 보안 상 GET 링크보다는 **POST 요청**으로 처리하는 것이 권장됨

---

## 3. AbstractUser class

* Django Auth 1에서 다룬 `AbstractUser`는 `username`, `password`, `email` 등 기본 필드와 `is_staff`, `is_superuser` 같은 권한 관련 필드까지 이미 포함하고 있는 **추상 모델**
* 여기에 필요한 필드만 추가하면 되므로, 처음부터 User 모델을 새로 설계하는 것보다 훨씬 안전하고 빠름

```python
class User(AbstractUser):
    profile_image = models.ImageField(blank=True, upload_to="profile/")
```

---

## 4. 회원 가입

* Django의 `UserCreationForm`을 커스텀 유저 모델에 맞게 확장해 사용

```python
# accounts/forms.py
from django.contrib.auth.forms import UserCreationForm
from django.contrib.auth import get_user_model

class CustomUserCreationForm(UserCreationForm):
    class Meta(UserCreationForm.Meta):
        model = get_user_model()    # AUTH_USER_MODEL로 설정된 커스텀 유저 모델 사용
        fields = UserCreationForm.Meta.fields    # 필요 시 + ("email",) 등 추가 가능
```

```python
def signup(request):
    if request.method == "POST":
        form = CustomUserCreationForm(request.POST)
        if form.is_valid():
            user = form.save()
            auth_login(request, user)      # 회원가입 후 자동 로그인 처리
            return redirect("articles:index")
    else:
        form = CustomUserCreationForm()
    return render(request, "accounts/signup.html", {"form": form})
```

* `get_user_model()`: 프로젝트에서 실제로 사용 중인 User 모델(커스텀 모델 포함)을 안전하게 참조하는 함수. 모델을 직접 import하는 대신 이 함수를 사용하는 것이 관례

---

## 5. 회원 탈퇴

```python
def delete(request):
    request.user.delete()      # 현재 로그인한 사용자 객체를 DB에서 삭제
    auth_logout(request)         # 삭제 후 세션도 함께 정리
    return redirect("articles:index")
```

* 회원 탈퇴는 실제 데이터 삭제이므로 반드시 **로그인 여부 확인 + POST 요청**으로만 처리해야 함
* `on_delete=models.CASCADE`로 연결된 관련 데이터(게시글, 댓글 등)는 회원 탈퇴 시 함께 삭제되므로, 실무에서는 완전 삭제 대신 **비활성화(`is_active=False`)** 처리를 택하기도 함

---

## 6. 로그인 사용자에 대한 접근 제한

### 데코레이터를 활용한 접근 제한

```python
from django.contrib.auth.decorators import login_required

@login_required
def create(request):
    # 로그인하지 않은 사용자는 자동으로 로그인 페이지로 리다이렉트됨
    ...
```

```python
# settings.py
LOGIN_URL = "accounts:login"   # login_required가 리다이렉트할 로그인 페이지 지정
```

### 조건문을 통한 직접 제한

```python
def update(request, pk):
    article = get_object_or_404(Article, pk=pk)
    if request.user != article.user:      # 작성자 본인이 아니면
        return redirect("articles:index")   # 수정 페이지 접근 차단
    ...
```

---

## 핵심 요약
* **로그아웃**은 현재 세션을 비워 `request.user`를 다시 익명 사용자로 되돌리는 처리이다.
* **회원가입/탈퇴**는 `UserCreationForm`을 커스텀 유저 모델에 맞게 확장해 구현하며, `get_user_model()`로 실제 사용 중인 User 모델을 안전하게 참조한다.
* 로그인 여부에 따른 접근 제한은 함수 전체를 막는 `@login_required` 데코레이터와, 작성자 본인 여부처럼 세부 조건을 확인하는 직접적인 조건문을 조합해 구현한다.
