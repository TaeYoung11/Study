# Vue Intoduction of Vue 정리

프론트엔드 프레임워크가 필요한 이유를 살펴보고, 대표적인 프론트엔드 프레임워크 중 하나인 **Vue**의 기본 개념과 시작 방법을 정리한 문서입니다.

---

## 1. Frontend

### 프론트엔드 프레임워크가 필요한 이유

* 지금까지처럼 순수 JavaScript로 DOM을 직접 조작(`querySelector`, `innerHTML` 등)하면, 화면이 복잡해질수록 **"현재 상태"와 "화면에 보이는 모습"을 계속 수동으로 일치시켜야** 해서 코드가 급격히 복잡해짐
* **선언적(Declarative) UI:** "어떻게 DOM을 바꿀지" 명령형으로 작성하는 대신, "현재 상태가 이렇다면 화면은 이래야 한다"를 선언하면 프레임워크가 알아서 화면을 상태에 맞게 갱신해주는 방식

```javascript
// 명령형 (기존 JS): 상태가 바뀔 때마다 DOM을 직접 찾아 수동으로 갱신
count++;
document.querySelector("#count").textContent = count;

// 선언적 (Vue): 상태(count)만 변경하면, 화면은 자동으로 최신 상태를 반영
count.value++;   // 템플릿의 {{ count }}가 자동으로 갱신됨
```

### 대표적인 프론트엔드 프레임워크

| 프레임워크 | 특징 |
| --- | --- |
| **Vue** | 배우기 쉬운 문법, 점진적으로 도입 가능(Progressive) |
| **React** | JSX 기반, 방대한 생태계 |
| **Angular** | 풀스택 프레임워크, 대규모 엔터프라이즈 애플리케이션에 강점 |

---

## 2. Vue tutorial

### Vue 시작하기

```bash
npm create vue@latest    # Vite 기반 Vue 프로젝트 생성
npm install
npm run dev                 # 개발 서버 실행
```

### 반응형 데이터와 템플릿 문법 맛보기

```vue
<script setup>
import { ref } from "vue";

const count = ref(0);   // ref: 값의 변화를 Vue가 감지할 수 있는 반응형 데이터로 만듦
</script>

<template>
  <p>현재 카운트: {{ count }}</p>
  <button @click="count++">증가</button>
</template>
```

* `ref()`로 감싼 데이터는 값이 바뀔 때마다 이를 사용하는 화면 부분이 **자동으로 다시 렌더링**됨
* `{{ }}`(콧수염 문법, Mustache)로 데이터를 화면에 출력하고, `@click`처럼 `@` 접두사로 이벤트를 연결

---

## 3. 참고 — Vue의 핵심 철학

* **점진적 프레임워크(Progressive Framework):** 기존 HTML 페이지에 `<script>` 하나만 추가해 일부분에만 적용할 수도 있고, 대규모 SPA 전체를 Vue로 구성할 수도 있음 (React/Angular보다 진입 장벽이 낮은 편)
* **반응형 시스템(Reactivity):** 데이터(상태)가 바뀌면 이를 사용하는 화면이 자동으로 갱신되는 것이 Vue를 비롯한 현대 프론트엔드 프레임워크의 공통된 핵심 원리

---

## 핵심 요약
* 프론트엔드 프레임워크는 "상태 변경 → 화면 자동 갱신"을 지원하는 **선언적 UI**를 통해, 순수 JS로 DOM을 직접 조작할 때 발생하는 복잡도를 줄여준다.
* Vue는 `ref()`로 반응형 데이터를 만들고, `{{ }}`/`@이벤트` 문법으로 데이터와 화면을 연결하는 방식으로 동작한다.
* Vue는 필요한 부분에만 점진적으로 도입할 수 있는 **Progressive Framework**라는 점이 다른 프레임워크와 구분되는 특징이다.
