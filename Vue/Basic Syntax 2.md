# Vue Basic Syntax 2 정리

Vue의 조건부 렌더링, 리스트 렌더링을 포함한 **Vue 기본 문법**과, 컴포넌트가 생성되고 사라지기까지의 각 시점에 개입할 수 있는 **LifeCycle Hook**을 정리한 문서입니다.

---

## 1. Vue 기본 문법

### 조건부 렌더링

```vue
<template>
  <p v-if="score >= 90">A등급</p>
  <p v-else-if="score >= 80">B등급</p>
  <p v-else>C등급</p>

  <p v-show="isVisible">항상 DOM에 있지만 display로 보임/숨김만 전환</p>
</template>
```

* `v-if`: 조건이 거짓이면 **DOM 자체를 생성하지 않음** (전환이 잦지 않은 경우에 적합)
* `v-show`: 조건과 무관하게 DOM은 유지하고 `display: none`으로만 감춤 (자주 토글되는 경우 성능에 유리)

### 리스트 렌더링

```vue
<script setup>
const fruits = ref(["사과", "바나나", "포도"]);
</script>

<template>
  <ul>
    <li v-for="(fruit, index) in fruits" :key="fruit">
      {{ index }} - {{ fruit }}
    </li>
  </ul>
</template>
```

* `v-for`로 배열을 순회하며 요소를 반복 렌더링하며, 반드시 **`:key`에 고유한 값**을 지정해야 Vue가 각 항목을 효율적으로 추적하고 갱신할 수 있음 (배열의 index를 key로 쓰는 것은 배열 순서가 바뀌는 경우 버그의 원인이 될 수 있어 지양)

### computed — 계산된 속성

```vue
<script setup>
import { ref, computed } from "vue";
const price = ref(1000);
const quantity = ref(3);

const total = computed(() => price.value * quantity.value);   // 의존하는 값이 바뀔 때만 재계산
</script>
```

* `computed`는 관련된 데이터가 바뀔 때만 다시 계산되고, 그렇지 않으면 이전 계산 결과를 **캐싱**해서 재사용 — 매번 다시 계산하는 일반 메서드보다 효율적

---

## 2. LifeCycle Hook

컴포넌트는 생성 → 화면에 붙음(mount) → 데이터 갱신 → 화면에서 제거(unmount)까지 일정한 생명주기를 거치며, 각 단계마다 원하는 코드를 실행할 수 있는 훅(Hook)을 제공합니다.

```vue
<script setup>
import { onMounted, onUpdated, onUnmounted } from "vue";

onMounted(() => {
    console.log("컴포넌트가 화면에 렌더링 완료됨");
    // 예: API 요청으로 초기 데이터를 불러오는 시점
    fetchArticles();
});

onUpdated(() => {
    console.log("데이터 변경으로 화면이 다시 렌더링됨");
});

onUnmounted(() => {
    console.log("컴포넌트가 화면에서 제거됨");
    // 예: 등록해둔 타이머/이벤트 리스너 정리
    clearInterval(timer);
});
</script>
```

| 훅 | 호출 시점 | 대표적인 사용 예 |
| --- | --- | --- |
| `onMounted` | 컴포넌트가 DOM에 처음 렌더링된 직후 | 초기 데이터 fetch, DOM 요소 접근 |
| `onUpdated` | 반응형 데이터 변경으로 재렌더링된 직후 | 렌더링 결과에 따른 후처리 |
| `onUnmounted` | 컴포넌트가 제거되기 직전 | 타이머 해제, 이벤트 리스너 제거 (메모리 누수 방지) |

* API 호출처럼 **컴포넌트가 화면에 나타난 뒤에 실행되어야 하는 로직**은 반드시 `onMounted` 안에서 호출해야 함

---

## 핵심 요약
* `v-if`/`v-show`는 각각 DOM 존재 여부와 CSS 표시 여부로 조건부 렌더링을 다르게 처리하고, `v-for`는 반드시 고유한 `:key`와 함께 사용해야 한다.
* `computed`는 의존하는 데이터가 바뀔 때만 재계산되는 캐싱된 계산값으로, 반복 계산 비용을 줄여준다.
* **LifeCycle Hook**(`onMounted`, `onUpdated`, `onUnmounted`)을 사용하면 컴포넌트의 생성부터 소멸까지 각 시점에 필요한 로직(데이터 fetch, 리소스 정리 등)을 정확히 실행할 수 있다.
