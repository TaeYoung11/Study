# Vue with DRF 1 - CORS Policy 정리

Vue(프론트엔드)와 Django REST Framework(백엔드)를 별도의 서버로 분리해 연동하는 방법과, 이 과정에서 반드시 마주치게 되는 **CORS 정책**, 그리고 실전 예제인 **게시글 생성/조회 구현**을 정리한 문서입니다.

---

## 1. DRF with Vue — 분리된 프론트/백엔드 연동

* 지금까지는 Django가 Template을 직접 렌더링했지만, 이제부터는 **Vue(별도 서버, 예: localhost:5173)** 와 **DRF(별도 서버, 예: localhost:8000)** 가 각각 독립적으로 실행되고, Vue가 DRF의 API를 호출하는 구조로 전환

```javascript
// Vue에서 axios로 DRF API 호출
import axios from "axios";

const api = axios.create({
    baseURL: "http://localhost:8000/api/",
});

export default api;
```

---

## 2. CORS Policy

### CORS(Cross-Origin Resource Sharing)란?

* 브라우저의 **Same-Origin Policy(동일 출처 정책)** 때문에, 기본적으로 스크립트는 자신이 로드된 출처(Origin)와 **다른 출처의 서버에 요청을 보낼 수 없음**
* **출처(Origin):** `프로토콜 + 도메인 + 포트`의 조합 — Vue(`localhost:5173`)와 Django(`localhost:8000`)는 포트가 다르므로 서로 다른 출처로 취급됨
* **CORS:** 서버가 "이 출처(Origin)의 요청은 허용한다"고 명시적으로 응답 헤더에 알려주어, 다른 출처 간의 요청을 예외적으로 허용하는 정책

### Django에서 CORS 허용하기

```bash
pip install django-cors-headers
```

```python
# settings.py
INSTALLED_APPS += ["corsheaders"]
MIDDLEWARE = ["corsheaders.middleware.CorsMiddleware"] + MIDDLEWARE   # 가능한 최상단에 위치

CORS_ALLOWED_ORIGINS = [
    "http://localhost:5173",   # Vue 개발 서버의 출처만 명시적으로 허용
]
```

* 운영 환경에서는 `CORS_ALLOW_ALL_ORIGINS = True`처럼 모든 출처를 허용하지 않고, 실제로 신뢰하는 프론트엔드 도메인만 `CORS_ALLOWED_ORIGINS`에 명시하는 것이 보안상 안전함

---

## 3. 게시글 생성_조회 구현

```vue
<script setup>
import { ref, onMounted } from "vue";
import api from "@/api";

const articles = ref([]);
const newArticle = ref({ title: "", content: "" });

onMounted(async () => {
    const response = await api.get("articles/");   // GET /api/articles/
    articles.value = response.data;
});

async function createArticle() {
    const response = await api.post("articles/", newArticle.value);   // POST /api/articles/
    articles.value.push(response.data);
    newArticle.value = { title: "", content: "" };
}
</script>

<template>
  <form @submit.prevent="createArticle">
    <input v-model="newArticle.title" placeholder="제목">
    <textarea v-model="newArticle.content" placeholder="내용"></textarea>
    <button type="submit">작성</button>
  </form>

  <ul>
    <li v-for="article in articles" :key="article.id">{{ article.title }}</li>
  </ul>
</template>
```

* 컴포넌트가 화면에 마운트되는 시점(`onMounted`)에 목록을 조회하고, 폼 제출 시 `POST` 요청 후 응답으로 받은 새 게시글을 기존 배열에 바로 추가해 **다시 목록 전체를 조회하지 않고도** 화면을 갱신

---

## 핵심 요약
* Vue와 DRF를 별도 서버로 분리하면, 서로 다른 출처(Origin) 간의 요청이 브라우저의 **Same-Origin Policy**에 의해 기본적으로 차단된다.
* **CORS**는 서버가 특정 출처의 요청을 명시적으로 허용하는 정책으로, Django에서는 `django-cors-headers`와 `CORS_ALLOWED_ORIGINS` 설정으로 처리한다.
* 실전에서는 `onMounted`에서 목록을 조회하고, 생성 후에는 응답 데이터를 기존 배열에 바로 반영해 불필요한 재조회 없이 화면을 갱신하는 패턴을 사용한다.
