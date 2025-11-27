# Vue Basic Syntax 1 정리

Vue의 핵심 기능인 **데이터 바인딩(Data binding)**, 사용자 동작에 반응하는 **이벤트 핸들링**, 그리고 폼 입력과 데이터를 자동으로 동기화하는 **Form Input Binding**을 정리한 문서입니다.

---

## 1. Data binding

### 텍스트 바인딩

```vue
<script setup>
import { ref } from "vue";
const message = ref("안녕하세요");
</script>

<template>
  <p>{{ message }}</p>                          <!-- 텍스트 콘텐츠 바인딩 -->
  <p>{{ message.toUpperCase() }}</p>              <!-- 표현식(간단한 연산) 사용 가능 -->
</template>
```

### 속성(Attribute) 바인딩 — `v-bind`

```vue
<script setup>
const imageUrl = ref("logo.png");
const isDisabled = ref(true);
</script>

<template>
  <img v-bind:src="imageUrl">     <!-- v-bind로 속성값을 데이터와 연결 -->
  <img :src="imageUrl">             <!-- v-bind: 는 : 로 축약 가능 (가장 흔히 쓰는 표기) -->
  <button :disabled="isDisabled">클릭</button>
</template>
```

* `{{ }}`는 **텍스트 콘텐츠**에만 사용하고, HTML 속성 값에는 반드시 `v-bind`(`:`)를 사용해야 함

---

## 2. Event Handling

```vue
<script setup>
const count = ref(0);
function increase() {
    count.value++;
}
</script>

<template>
  <button v-on:click="increase">증가</button>   <!-- v-on:이벤트="메서드" -->
  <button @click="increase">증가</button>          <!-- v-on: 은 @ 로 축약 가능 -->
  <button @click="count++">인라인 표현식도 가능</button>

  <!-- 이벤트 객체 접근 및 수식어(modifier) -->
  <form @submit.prevent="handleSubmit">   <!-- .prevent: preventDefault()를 자동 호출 -->
    <input @keyup.enter="handleEnter">      <!-- .enter: 특정 키 입력에만 반응 -->
  </form>
</template>
```

* `v-on:` → `@`, `v-bind:` → `:` 축약 표기가 실무에서 훨씬 널리 사용됨
* 수식어(`.prevent`, `.stop`, `.enter` 등)를 사용하면 `event.preventDefault()`, `event.stopPropagation()` 같은 반복 코드를 템플릿 문법만으로 처리 가능

---

## 3. Form Input Binding — `v-model`

* 입력 요소의 값과 데이터를 **양방향으로 자동 동기화**해주는 문법 (데이터가 바뀌면 화면이, 사용자가 입력하면 데이터가 함께 갱신됨)

```vue
<script setup>
const name = ref("");
const isAgreed = ref(false);
const selectedFruit = ref("apple");
</script>

<template>
  <input v-model="name">                              <!-- 텍스트 입력 -->
  <input type="checkbox" v-model="isAgreed">            <!-- 체크박스: boolean과 연결 -->
  <select v-model="selectedFruit">
    <option value="apple">사과</option>
    <option value="banana">바나나</option>
  </select>

  <p>{{ name }}님, 동의 여부: {{ isAgreed }}, 선택: {{ selectedFruit }}</p>
</template>
```

* `v-model`이 없다면, 입력값 변화(`@input`)를 감지해 데이터에 수동으로 대입하고, 데이터 변경 시 `:value`로 다시 화면에 반영하는 두 가지 작업을 직접 다 처리해야 함 — `v-model`은 이 둘을 하나의 문법으로 자동화한 것

---

## 핵심 요약
* 텍스트 콘텐츠는 `{{ }}`, HTML 속성은 `v-bind`(`:`)로 데이터를 화면에 바인딩한다.
* 이벤트는 `v-on`(`@`)으로 연결하며, `.prevent`/`.enter` 같은 수식어로 자주 쓰이는 이벤트 처리 로직을 간결하게 표현할 수 있다.
* `v-model`은 입력 요소와 데이터 사이의 **양방향 바인딩**을 자동으로 처리해, 폼 값 동기화 코드를 크게 줄여준다.
