# Vue Router 정리

SPA에서 여러 페이지를 다루기 위한 **Vue Router**의 기본 사용법과, 특정 조건에서 페이지 이동을 제어하는 **Navigation Guard**를 정리한 문서입니다.

---

## 1. Vue Router

### SPA에서 라우팅이 필요한 이유

* SPA는 하나의 HTML 페이지 위에서 동작하므로, URL이 바뀔 때마다 **서버에 새 페이지를 요청하는 대신 Vue Router가 알맞은 컴포넌트로 교체**해주어야 함

### 기본 설정

```javascript
// router/index.js
import { createRouter, createWebHistory } from "vue-router";
import HomeView from "../views/HomeView.vue";
import ArticleDetailView from "../views/ArticleDetailView.vue";

const router = createRouter({
    history: createWebHistory(),
    routes: [
        { path: "/", name: "home", component: HomeView },
        { path: "/articles/:id", name: "article-detail", component: ArticleDetailView },   // 동적 세그먼트
    ],
});

export default router;
```

```javascript
// main.js
import router from "./router";
createApp(App).use(router).mount("#app");   // 앱 전체에 라우터 플러그인 적용
```

```vue
<!-- App.vue -->
<template>
  <RouterLink to="/">홈</RouterLink>          <!-- 클릭 시 페이지 새로고침 없이 SPA 방식으로 이동 -->
  <RouterLink :to="{ name: 'article-detail', params: { id: 1 } }">게시글 1</RouterLink>

  <RouterView />                                 <!-- 현재 경로에 해당하는 컴포넌트가 렌더링되는 위치 -->
</template>
```

### 라우트 파라미터 접근

```vue
<script setup>
import { useRoute } from "vue-router";
const route = useRoute();
console.log(route.params.id);   // URL의 :id 부분 값
</script>
```

* `<a href="">` 대신 `<RouterLink>`를 사용해야 브라우저의 기본 페이지 이동(새로고침)이 아닌, Vue Router가 관리하는 **SPA 방식의 이동**이 일어남

---

## 2. Navigation Guard

* 특정 라우트로 이동하기 **직전/직후**에 로직을 실행해, 이동을 허용하거나 다른 경로로 돌려보낼 수 있는 기능

```javascript
router.beforeEach((to, from, next) => {
    const isLoggedIn = !!localStorage.getItem("access_token");

    if (to.meta.requiresAuth && !isLoggedIn) {
        next({ name: "login" });   // 로그인이 필요한 페이지인데 미로그인 상태면 로그인 페이지로 이동
    } else {
        next();                     // 정상적으로 원래 목적지로 이동 허용
    }
});
```

```javascript
// 라우트 정의 시 meta로 인증 필요 여부 표시
{ path: "/mypage", name: "mypage", component: MyPageView, meta: { requiresAuth: true } }
```

| 종류 | 실행 시점 |
| --- | --- |
| `router.beforeEach` | 모든 라우트 이동 전 (전역 가드) |
| `beforeEnter` | 특정 라우트 하나에 진입하기 전 |
| `beforeRouteEnter` (컴포넌트 내) | 해당 컴포넌트로 진입하기 전 |

* `next()`를 호출하지 않으면 라우팅이 그 자리에서 멈추므로, 조건 분기의 모든 경로에서 반드시 `next()` 또는 `next(다른경로)`를 호출해야 함

---

## 핵심 요약
* Vue Router는 `routes` 설정으로 경로와 컴포넌트를 매핑하며, `<RouterLink>`/`<RouterView>`로 SPA 방식의 페이지 전환을 구현한다.
* 동적 세그먼트(`:id`)는 `useRoute().params`로 접근하며, `RouterLink`는 일반 `<a>` 태그와 달리 새로고침 없는 이동을 보장한다.
* **Navigation Guard**(`beforeEach` 등)는 라우트 이동 전에 로그인 여부 등을 검사해, 인증이 필요한 페이지에 대한 접근을 제어하는 데 사용된다.
