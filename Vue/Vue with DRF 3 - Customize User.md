# Vue with DRF 3 - Customize User 정리

Vue 화면에서 인증 상태를 다루는 방법(**인증 With Vue**)과, 프로젝트 요구사항에 맞게 사용자 모델을 확장하는 **User Customize**를 정리한 문서입니다.

---

## 1. 인증 With Vue

### 인증 상태를 Pinia로 전역 관리하기

* 로그인 여부, 현재 사용자 정보는 여러 컴포넌트(네비게이션 바, 마이페이지, 라우터 가드)에서 공통으로 필요하므로 **Pinia store**로 관리하는 것이 일반적

```javascript
// stores/auth.js
export const useAuthStore = defineStore("auth", {
    state: () => ({
        accessToken: localStorage.getItem("access_token") || null,
        user: null,
    }),
    getters: {
        isLoggedIn: (state) => !!state.accessToken,
    },
    actions: {
        async login(credentials) {
            const { data } = await api.post("token/", credentials);
            this.accessToken = data.access;
            localStorage.setItem("access_token", data.access);
            await this.fetchProfile();
        },
        async fetchProfile() {
            const { data } = await api.get("accounts/profile/");   // 현재 로그인한 사용자 정보 조회
            this.user = data;
        },
        logout() {
            this.accessToken = null;
            this.user = null;
            localStorage.removeItem("access_token");
        },
    },
});
```

```vue
<!-- NavBar.vue -->
<script setup>
import { useAuthStore } from "@/stores/auth";
const authStore = useAuthStore();
</script>

<template>
  <span v-if="authStore.isLoggedIn">{{ authStore.user?.nickname }}님 환영합니다</span>
  <RouterLink v-else :to="{ name: 'login' }">로그인</RouterLink>
</template>
```

---

## 2. User Customize

### 프로젝트 요구사항에 맞춘 User 필드 확장

* Django의 `AbstractUser`를 상속해 닉네임, 프로필 이미지 등 서비스에 필요한 필드를 추가 (Django Auth 파트에서 다룬 것과 동일한 원리)

```python
# accounts/models.py
class User(AbstractUser):
    nickname = models.CharField(max_length=50, unique=True)
    profile_image = models.ImageField(upload_to="profile/", blank=True)
```

### 확장된 User 정보를 DRF Serializer로 노출

```python
class UserSerializer(serializers.ModelSerializer):
    class Meta:
        model = get_user_model()
        fields = ("id", "username", "nickname", "profile_image")

@api_view(["GET"])
@permission_classes([IsAuthenticated])
def profile(request):
    serializer = UserSerializer(request.user)   # 토큰으로 인증된 현재 사용자 정보 반환
    return Response(serializer.data)
```

* `request.user`는 JWT 인증이 통과된 시점에 Django가 자동으로 채워주는 현재 로그인 사용자 객체이므로, 이를 그대로 Serializer에 전달하면 됨

---

## 3. 참고 — 회원가입 시 커스텀 필드 함께 저장

```python
class SignupSerializer(serializers.ModelSerializer):
    class Meta:
        model = get_user_model()
        fields = ("username", "password", "nickname")
        extra_kwargs = {"password": {"write_only": True}}   # 응답에는 절대 포함되지 않도록 설정

    def create(self, validated_data):
        user = get_user_model().objects.create_user(**validated_data)   # 비밀번호 자동 해싱 저장
        return user
```

* `write_only=True`: 비밀번호는 요청(생성)에만 사용되고, 응답(조회) JSON에는 절대 노출되지 않도록 하는 필수 설정
* `create_user()`를 사용해야 비밀번호가 평문이 아닌 해시로 저장됨 (`create()`에 직접 `password` 필드를 넣으면 평문 저장되므로 지양)

---

## 핵심 요약
* Vue에서는 로그인 상태와 사용자 정보를 **Pinia store**로 전역 관리해, 네비게이션 바 등 여러 화면에서 일관되게 참조한다.
* Django의 커스텀 User 모델에 추가한 필드(닉네임, 프로필 이미지 등)는 DRF Serializer로 노출해 Vue에서 그대로 활용할 수 있다.
* 회원가입 Serializer에서는 비밀번호에 `write_only=True`를 지정하고, 반드시 `create_user()`로 생성해 비밀번호가 안전하게 해싱되어 저장되도록 해야 한다.
