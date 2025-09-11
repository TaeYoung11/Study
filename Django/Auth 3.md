# Django Auth 3 정리

로그인한 사용자가 자신의 **회원정보를 수정**하고 **비밀번호를 변경**하는 방법, 그리고 Django가 비밀번호를 안전하게 저장하는 **비밀번호 암호화** 원리를 정리한 문서입니다.

---

## 1. 회원정보 수정

* 비밀번호를 제외한 나머지 정보(닉네임, 이메일 등)는 일반 `ModelForm`으로 다룰 수 있음

```python
# accounts/forms.py
from django.contrib.auth.forms import UserChangeForm
from django.contrib.auth import get_user_model

class CustomUserChangeForm(UserChangeForm):
    class Meta(UserChangeForm.Meta):
        model = get_user_model()
        fields = ("email", "nickname")   # 비밀번호 필드는 별도 폼에서 처리
```

```python
@login_required
def update(request):
    if request.method == "POST":
        form = CustomUserChangeForm(request.POST, instance=request.user)
        if form.is_valid():
            form.save()
            return redirect("articles:index")
    else:
        form = CustomUserChangeForm(instance=request.user)
    return render(request, "accounts/update.html", {"form": form})
```

* `instance=request.user`: 현재 로그인한 사용자 자신의 정보만 폼에 채워 수정하도록 강제 → 다른 사용자의 정보를 실수로 수정하는 상황을 원천적으로 방지

---

## 2. 비밀번호 변경

* 비밀번호는 단순 `ModelForm`으로 다루면 평문 그대로 저장되어 위험하므로, Django가 제공하는 전용 Form을 사용

```python
from django.contrib.auth.forms import PasswordChangeForm
from django.contrib.auth import update_session_auth_hash

@login_required
def change_password(request):
    if request.method == "POST":
        form = PasswordChangeForm(request.user, request.POST)
        if form.is_valid():
            user = form.save()
            update_session_auth_hash(request, user)   # 비밀번호 변경 후 로그인 세션 유지
            return redirect("articles:index")
    else:
        form = PasswordChangeForm(request.user)
    return render(request, "accounts/change_password.html", {"form": form})
```

* `update_session_auth_hash()`를 호출하지 않으면, 비밀번호 변경 시 세션이 무효화되어 **사용자가 강제로 로그아웃**되어버리는 문제가 발생함

---

## 3. 비밀번호 암호화

### 왜 암호화(해싱)가 필요한가

* 비밀번호를 평문으로 저장하면, DB가 유출될 경우 모든 사용자의 비밀번호가 그대로 노출됨
* Django는 비밀번호를 저장할 때 되돌릴 수 없는 **해시 함수**로 변환해 저장하며, 로그인 시에는 입력값을 같은 방식으로 해싱한 뒤 저장된 해시와 비교

```
저장 시:   "1234" --(해시 함수)--> "pbkdf2_sha256$600000$salt$hash값"
로그인 시: 입력된 "1234"를 같은 방식으로 해싱 → 저장된 해시값과 일치하는지 비교 (원문 복호화는 하지 않음)
```

### Salt를 사용하는 이유

* 같은 비밀번호("1234")를 쓰는 사용자가 여럿이어도, 사용자마다 다른 무작위 값인 **Salt**를 비밀번호에 더해 해싱하기 때문에 최종 해시값이 서로 달라짐
* 이를 통해 미리 계산된 해시값 표(Rainbow Table)를 이용한 공격을 방어할 수 있음

```python
# Django 내부적으로는 이런 유틸 함수로 처리됨
from django.contrib.auth.hashers import make_password, check_password

hashed = make_password("1234")            # 저장용 해시 생성
check_password("1234", hashed)             # 로그인 시 입력값 검증 (True/False)
```

* Django의 `AbstractUser.set_password()`, `create_user()` 등은 내부적으로 이 해싱 로직을 자동으로 적용하므로, 직접 비밀번호 필드에 문자열을 대입(`user.password = "1234"`)하는 것은 절대 피해야 함

---

## 핵심 요약
* 회원정보 수정은 `instance=request.user`로 항상 **본인의 데이터만** 대상으로 하도록 제한해야 안전하다.
* 비밀번호 변경은 `PasswordChangeForm`을 사용하고, 변경 후 `update_session_auth_hash()`를 호출해 세션이 끊기지 않도록 해야 한다.
* Django는 비밀번호를 평문이 아닌 **Salt + 해시 함수**로 저장하며, 로그인 시에도 복호화가 아니라 동일한 방식으로 재해싱한 값을 비교하는 방식으로 검증한다.
