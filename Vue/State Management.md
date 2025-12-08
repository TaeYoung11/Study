# Vue State Management 정리

여러 컴포넌트가 함께 사용하는 데이터를 효율적으로 관리하기 위한 Vue의 공식 상태 관리 라이브러리 **Pinia**의 구조와 실습 방법을 정리한 문서입니다.

---

## 1. Pinia 구조 및 구성

### 왜 별도의 상태 관리가 필요한가

* Props/Emit은 부모-자식처럼 **가까운 컴포넌트 간**의 데이터 전달에는 적합하지만, 서로 관련 없는 여러 컴포넌트(로그인 정보, 장바구니 등)가 같은 데이터를 공유해야 할 때는 매번 여러 단계를 거쳐 props를 전달(Props Drilling)해야 해서 비효율적
* **Pinia**는 컴포넌트 트리와 무관하게 어디서든 접근 가능한 **전역 저장소(Store)** 를 제공해 이 문제를 해결

### Store의 구성 요소

| 요소 | 역할 | Vue 컴포넌트에 비유 |
| --- | --- | --- |
| **State** | 저장되는 데이터 자체 | `data` (반응형 상태) |
| **Getters** | State를 가공한 계산된 값 | `computed` |
| **Actions** | State를 변경하는 로직 (비동기 처리 포함 가능) | `methods` |

```javascript
// stores/counter.js
import { defineStore } from "pinia";

export const useCounterStore = defineStore("counter", {
    state: () => ({
        count: 0,
    }),
    getters: {
        doubleCount: (state) => state.count * 2,
    },
    actions: {
        increment() {
            this.count++;   // actions 내부에서는 this로 state에 바로 접근
        },
    },
});
```

```javascript
// main.js
import { createPinia } from "pinia";
createApp(App).use(createPinia()).mount("#app");
```

---

## 2. Pinia 실습

### Store 사용하기

```vue
<script setup>
import { useCounterStore } from "@/stores/counter";

const counterStore = useCounterStore();   // 어느 컴포넌트에서든 동일한 store 인스턴스를 가져옴
</script>

<template>
  <p>{{ counterStore.count }}</p>
  <p>2배: {{ counterStore.doubleCount }}</p>
  <button @click="counterStore.increment">증가</button>
</template>
```

### 로그인 정보를 전역으로 관리하는 예시

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
            const { data } = await axios.post("/api/login/", credentials);
            this.accessToken = data.access;
            localStorage.setItem("access_token", data.access);   // 새로고침 후에도 유지되도록 저장
        },
        logout() {
            this.accessToken = null;
            localStorage.removeItem("access_token");
        },
    },
});
```

* 로그인 상태처럼 여러 화면(네비게이션 바, 마이페이지, 라우터 가드 등)에서 공통으로 필요한 데이터는 Pinia store 하나로 관리하면, 어느 컴포넌트에서 접근하든 **항상 동일한 최신 상태**를 참조하게 됨

---

## 핵심 요약
* Pinia는 컴포넌트 트리 구조와 무관하게 데이터를 공유할 수 있는 **전역 Store**를 제공해, Props Drilling 문제를 해결한다.
* Store는 `state`(데이터), `getters`(계산된 값), `actions`(상태 변경 로직) 세 가지로 구성되며 각각 컴포넌트의 data/computed/methods에 대응한다.
* `useXxxStore()`를 호출하면 어느 컴포넌트에서든 동일한 Store 인스턴스에 접근할 수 있어, 로그인 정보 등 전역적으로 필요한 상태를 일관되게 관리할 수 있다.
