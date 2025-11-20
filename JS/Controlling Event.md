# JS Controlling Event 정리

사용자의 동작에 반응하는 **이벤트**의 개념과 등록 방법, 이벤트가 전파되는 **버블링**, 그리고 실전에서 자주 쓰이는 **이벤트 핸들러 활용법**을 정리한 문서입니다.

---

## 1. 이벤트(Event)

* 클릭, 입력, 스크롤, 페이지 로드 등 **브라우저에서 발생하는 모든 사건**
* 이벤트가 발생했을 때 실행할 함수를 **이벤트 핸들러(리스너)** 라고 함

```javascript
const button = document.querySelector("#submit-btn");

button.addEventListener("click", (event) => {
    console.log("버튼이 클릭됨!");
    console.log(event.target);   // 이벤트가 실제로 발생한 요소
});
```

* `addEventListener`의 콜백 함수는 자동으로 **이벤트 객체(event)** 를 인자로 전달받으며, 이 객체를 통해 이벤트에 대한 다양한 정보(대상 요소, 좌표, 키 값 등)에 접근할 수 있음

---

## 2. 버블링(Bubbling)

### 이벤트 전파(Event Propagation)

* 어떤 요소에서 이벤트가 발생하면, 이벤트는 **자식 → 부모 → 조상**의 순서로 상위 요소까지 전파됨 (버블링)

```html
<div id="parent">
  <button id="child">클릭</button>
</div>
```

```javascript
document.querySelector("#parent").addEventListener("click", () => {
    console.log("부모까지 이벤트가 전파됨");
});

document.querySelector("#child").addEventListener("click", (e) => {
    console.log("버튼 클릭");
    e.stopPropagation();   // 이 지점에서 버블링을 멈춤 (부모의 핸들러 실행 방지)
});
```

### 이벤트 위임(Event Delegation)

* 버블링을 활용해, 자식 요소 하나하나에 이벤트를 등록하는 대신 **부모 요소 하나에만 이벤트를 등록**하고 `event.target`으로 실제 클릭된 자식을 판별하는 패턴

```javascript
document.querySelector("#list").addEventListener("click", (e) => {
    if (e.target.tagName === "LI") {
        console.log(`${e.target.textContent} 클릭됨`);
    }
});
```

* 이벤트 위임을 사용하면, 이후 동적으로 추가되는 자식 요소(`<li>`)에도 별도의 이벤트 등록 없이 자동으로 이벤트가 적용된다는 큰 장점이 있음

---

## 3. event handler 활용

### 기본 동작 막기 — `preventDefault()`

```javascript
document.querySelector("form").addEventListener("submit", (e) => {
    e.preventDefault();   // 폼의 기본 제출(페이지 새로고침) 동작을 막음
    // 이후 JavaScript로 직접 데이터를 처리 (AJAX 요청 등)
});

document.querySelector("a").addEventListener("click", (e) => {
    e.preventDefault();   // 링크의 기본 이동 동작을 막음
});
```

### 다양한 이벤트 종류

| 이벤트 | 발생 시점 |
| --- | --- |
| `click` | 클릭했을 때 |
| `input` | 입력 값이 바뀔 때마다 |
| `keydown` / `keyup` | 키를 누르거나 뗄 때 |
| `submit` | 폼이 제출될 때 |
| `load` | 페이지/리소스 로드가 완료됐을 때 |
| `DOMContentLoaded` | HTML 문서 파싱이 완료됐을 때 (이미지 등 리소스는 기다리지 않음) |

```javascript
document.addEventListener("DOMContentLoaded", () => {
    // DOM이 준비된 후 실행되어야 하는 초기화 코드
    initApp();
});
```

---

## 핵심 요약
* 이벤트는 `addEventListener`로 등록하며, 콜백에 전달되는 **이벤트 객체**를 통해 발생 대상과 관련 정보를 확인할 수 있다.
* 이벤트는 자식에서 부모로 전파되는 **버블링** 특성이 있으며, 이를 활용한 **이벤트 위임**을 사용하면 동적으로 추가되는 요소까지 하나의 리스너로 효율적으로 처리할 수 있다.
* `preventDefault()`로 폼 제출/링크 이동 같은 브라우저의 기본 동작을 막고, `DOMContentLoaded` 시점에 초기화 코드를 실행하는 패턴이 실전에서 자주 사용된다.
