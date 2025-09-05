# Vue PJT Vue를 활용한 SPA 구성 정리

관통 PJT에서 다룬 **Web Font**, **Programming with AI**, **Figma를 활용한 웹 페이지 구현**을, **Vue 기반 SPA(Single Page Application)** 프로젝트에 적용하는 관점에서 정리한 문서입니다.

---

## 1. SPA(Single Page Application)란?

* 페이지 전체를 새로 요청하는 대신, **하나의 HTML 페이지** 위에서 JavaScript(Vue)가 필요한 부분만 동적으로 갱신하는 방식의 웹 애플리케이션
* 화면 전환 시 서버로부터 전체 HTML을 다시 받지 않고, **Vue Router**가 URL에 따라 컴포넌트만 교체하기 때문에 화면 전환이 매끄럽고 빠름

```
[기존 MPA]                       [SPA]
페이지 이동 -> 서버에 새 HTML 요청   페이지 이동 -> JS가 필요한 컴포넌트만 교체
(매번 전체 새로고침)                (하나의 index.html 유지, 부분 렌더링)
```

---

## 2. Web Font in Vue

* SPA 프로젝트에서도 웹 폰트 적용 원칙은 동일하게, `@font-face`(또는 CDN)로 폰트를 선언하고 전역 스타일에 반영
* Vue 프로젝트에서는 보통 `src/assets/fonts`에 폰트 파일을 두고, **전역 스타일 파일**(`main.css`/`App.vue`의 `<style>`)에서 한 번만 선언해 모든 컴포넌트에서 공통으로 사용

```css
/* src/assets/fonts.css */
@font-face {
    font-family: "Pretendard";
    src: url("./fonts/Pretendard-Regular.woff2") format("woff2");
}

/* 전역 기본 폰트로 지정 */
body {
    font-family: "Pretendard", sans-serif;
}
```

---

## 3. Programming with AI로 컴포넌트 설계하기

* Vue 컴포넌트 구조(부모-자식, Props/Emit 흐름)를 설계할 때도 생성형 AI를 활용해 초안을 빠르게 잡을 수 있음
* 효과적인 활용 예
  * "이 화면을 Vue 컴포넌트로 어떻게 나누면 좋을지" 구조를 먼저 질문 → 컴포넌트 트리 초안 획득
  * 반복되는 UI 패턴(카드 목록, 폼 등)의 템플릿 코드 초안 생성 요청
  * 생성된 코드는 실제 프로젝트의 상태 관리 방식(Pinia 등)에 맞게 반드시 직접 수정·검증

---

## 4. Figma 디자인을 Vue 컴포넌트로 옮기기

### 디자인 → 컴포넌트 매핑

1. Figma에서 설계한 화면을 **재사용 가능한 단위**로 먼저 구획 (버튼, 카드, 헤더 등)
2. 각 구획을 Vue의 **SFC(Single File Component)** 하나로 대응
3. Figma의 Variants(버튼의 상태별 디자인 등)는 Vue의 **Props**로 표현

```vue
<!-- Figma의 버튼 컴포넌트(Variants: primary/secondary)를 Vue Props로 매핑 -->
<template>
  <button :class="['btn', `btn-${type}`]">
    <slot></slot>
  </button>
</template>

<script setup>
defineProps({
  type: { type: String, default: "primary" },   // Figma의 Variant에 대응
});
</script>
```

### 반응형 디자인 반영

* Figma에서 설정한 브레이크포인트를 Vue 컴포넌트의 스타일(`@media` 쿼리 또는 CSS 유틸리티)에 그대로 반영
* 컴포넌트 단위로 스타일을 캡슐화(`<style scoped>`)해두면, 반응형 스타일이 다른 컴포넌트에 영향을 주지 않음

---

## 핵심 요약
* **SPA**는 하나의 HTML 위에서 Vue Router가 컴포넌트만 교체하는 방식으로 동작해, 페이지 전체를 다시 받는 기존 방식보다 빠른 화면 전환을 제공한다.
* Web Font는 SPA에서도 전역 스타일 파일에 한 번만 선언해 모든 컴포넌트가 공유하도록 구성한다.
* Figma에서 설계한 디자인은 재사용 단위(버튼, 카드 등)로 나누어 Vue의 **SFC + Props**로 매핑하면, 디자인의 Variants가 컴포넌트의 Props로 자연스럽게 이어진다.
