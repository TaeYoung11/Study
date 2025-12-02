# Vue SFC 정리

Vue 컴포넌트를 하나의 파일로 관리하는 **SFC(Single File Component)** 구조와, 이를 기반으로 실제 **Vue 프로젝트**를 구성하는 방법을 정리한 문서입니다.

---

## 1. SFC (Single File Component)

### `.vue` 파일의 구조

* Vue의 SFC는 하나의 `.vue` 파일 안에 **템플릿(HTML), 스크립트(JS 로직), 스타일(CSS)** 을 함께 작성하는 구조

```vue
<template>
  <!-- 화면 구조 (HTML) -->
  <div class="card">
    <h2>{{ title }}</h2>
  </div>
</template>

<script setup>
// 로직 (JavaScript) — script setup 문법을 사용하면 별도의 return 없이 바로 템플릿에서 사용 가능
import { ref } from "vue";
const title = ref("컴포넌트 제목");
</script>

<style scoped>
/* 스타일 (CSS) */
.card {
    padding: 16px;
    border: 1px solid #ddd;
}
</style>
```

* **`<style scoped>`:** 이 컴포넌트 안에서 작성한 스타일이 **다른 컴포넌트에 영향을 주지 않도록** 자동으로 격리(캡슐화)해줌

### 컴포넌트 등록 및 사용

```vue
<!-- App.vue -->
<script setup>
import ArticleCard from "./components/ArticleCard.vue";   // 파일을 import하는 것만으로 등록 완료 (script setup)
</script>

<template>
  <ArticleCard />
  <ArticleCard title="다른 제목" />   <!-- Props로 값을 다르게 전달해 재사용 -->
</template>
```

* 컴포넌트를 재사용 가능한 단위로 나누면, 같은 UI 패턴(카드, 버튼 등)을 여러 곳에서 일관되게 사용할 수 있고 유지보수도 한 곳(컴포넌트 파일)만 수정하면 됨

---

## 2. Vue Project — 프로젝트 구조

```
src/
├── main.js          # 앱의 진입점, 최상위 App을 마운트
├── App.vue           # 최상위 루트 컴포넌트
├── components/        # 재사용 가능한 컴포넌트들
│   ├── ArticleCard.vue
│   └── NavBar.vue
├── views/              # 라우터에 연결되는 페이지 단위 컴포넌트
│   ├── HomeView.vue
│   └── ArticleDetailView.vue
├── router/              # Vue Router 설정
├── stores/               # Pinia 상태 관리
└── assets/                # 이미지, 폰트 등 정적 자원
```

```javascript
// main.js
import { createApp } from "vue";
import App from "./App.vue";

createApp(App).mount("#app");    // App 컴포넌트를 index.html의 #app 요소에 렌더링
```

* **`components/`** 는 여러 화면에서 공통으로 재사용되는 작은 단위(버튼, 카드 등), **`views/`** 는 하나의 라우트(페이지)에 대응하는 단위로 구분하는 것이 일반적인 관례

---

## 핵심 요약
* **SFC**는 하나의 `.vue` 파일 안에 template(구조), script(로직), style(디자인)을 함께 작성하는 Vue의 컴포넌트 단위이며, `scoped` 스타일로 컴포넌트 간 CSS 충돌을 방지한다.
* 컴포넌트는 import해서 태그처럼 사용하며, Props로 다른 값을 전달해 같은 컴포넌트를 여러 곳에서 재사용할 수 있다.
* 실제 프로젝트는 재사용 컴포넌트(`components/`)와 라우트 단위 페이지(`views/`)를 구분해 구성하는 것이 유지보수에 유리하다.
