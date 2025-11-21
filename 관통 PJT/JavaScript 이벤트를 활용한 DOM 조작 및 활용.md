# 관통 PJT JavaScript 이벤트를 활용한 DOM 조작 및 활용 정리

사용자 입력과 마우스 동작을 다루는 **Drag 이벤트**, 화면 스크롤에 반응하는 **Scroll 이벤트**를 활용해 인터랙티브한 UI를 구현하는 방법을 정리한 문서입니다.

---

## 1. Input / Drag Event

### 드래그 앤 드롭(Drag & Drop) 이벤트 종류

| 이벤트 | 발생 대상 | 설명 |
| --- | --- | --- |
| `dragstart` | 드래그되는 요소 | 드래그가 시작될 때 |
| `drag` | 드래그되는 요소 | 드래그 중 계속 발생 |
| `dragover` | 드롭 대상 요소 | 드래그 중인 요소가 위를 지나갈 때 (계속 발생) |
| `dragenter` | 드롭 대상 요소 | 드래그 중인 요소가 영역에 처음 들어올 때 |
| `dragleave` | 드롭 대상 요소 | 드래그 중인 요소가 영역을 벗어날 때 |
| `drop` | 드롭 대상 요소 | 실제로 놓았을 때 |
| `dragend` | 드래그되는 요소 | 드래그가 끝났을 때 (성공/취소 모두) |

```javascript
const dragItem = document.querySelector(".drag-item");
const dropZone = document.querySelector(".drop-zone");

dragItem.addEventListener("dragstart", (e) => {
  e.dataTransfer.setData("text/plain", e.target.id);   // 드래그할 데이터 저장
});

dropZone.addEventListener("dragover", (e) => {
  e.preventDefault();   // 기본 동작(드롭 불가)을 막아야 드롭이 허용됨
});

dropZone.addEventListener("drop", (e) => {
  e.preventDefault();
  const id = e.dataTransfer.getData("text/plain");
  dropZone.appendChild(document.getElementById(id));   // 실제 DOM 이동
});
```

* `draggable="true"` 속성을 요소에 지정해야 드래그가 가능해짐
* `dragover`에서 `preventDefault()`를 호출하지 않으면 `drop` 이벤트가 아예 발생하지 않는 점에 유의

### Input 이벤트

* `input` 이벤트: 입력 필드의 값이 바뀔 때마다(타이핑 즉시) 발생 → 실시간 검색어 자동완성, 글자 수 표시 등에 활용
* `change` 이벤트와의 차이: `change`는 값이 확정(포커스 아웃 등)된 시점에만 발생

```javascript
searchInput.addEventListener("input", (e) => {
  renderSuggestions(e.target.value);   // 입력할 때마다 즉시 반응
});
```

---

## 2. Scroll Event

### 스크롤 위치 감지

```javascript
window.addEventListener("scroll", () => {
  const scrollY = window.scrollY;              // 현재 세로 스크롤 위치
  const scrollHeight = document.body.scrollHeight;
  const viewportHeight = window.innerHeight;

  if (scrollY + viewportHeight >= scrollHeight - 10) {
    loadMoreItems();   // 무한 스크롤: 바닥 근처에 도달하면 추가 데이터 로드
  }
});
```

### 성능 최적화 — Debounce / Throttle

* `scroll` 이벤트는 스크롤 도중 매우 짧은 간격으로 계속 발생하기 때문에, 콜백에서 무거운 연산(DOM 조작, API 호출)을 그대로 수행하면 성능 저하가 발생
* **Throttle:** 일정 시간 간격으로 한 번만 콜백을 실행하도록 제한
* **Debounce:** 이벤트가 연속으로 발생하는 동안은 실행을 미루다가, 마지막 이벤트 이후 일정 시간이 지나야 실행

```javascript
function throttle(fn, delay) {
  let last = 0;
  return (...args) => {
    const now = Date.now();
    if (now - last >= delay) {
      last = now;
      fn(...args);
    }
  };
}

window.addEventListener("scroll", throttle(handleScroll, 200));
```

### Intersection Observer — 스크롤 이벤트의 대안

* 특정 요소가 화면(뷰포트)에 보이는지 여부를 스크롤 이벤트 없이 효율적으로 감지하는 브라우저 API
* 무한 스크롤, 이미지 지연 로딩(Lazy Loading), 스크롤에 따른 애니메이션 트리거 등에 `scroll` 이벤트보다 성능상 유리

```javascript
const observer = new IntersectionObserver((entries) => {
  entries.forEach((entry) => {
    if (entry.isIntersecting) {
      loadMoreItems();    // 관찰 대상이 화면에 들어오면 실행
    }
  });
});
observer.observe(document.querySelector(".sentinel"));
```

---

## 핵심 요약
* **Drag 이벤트**는 `dragstart → dragover(대상) → drop → dragend` 순으로 발생하며, `dragover`에서 `preventDefault()`를 호출해야 실제 `drop`이 허용된다.
* **Input 이벤트**는 값이 바뀌는 즉시 발생해 실시간 반응(자동완성 등)에 적합하고, `change`는 값이 확정된 시점에만 발생한다는 차이가 있다.
* **Scroll 이벤트**는 매우 빈번하게 발생하므로 Throttle/Debounce로 성능을 관리해야 하며, 화면 노출 여부만 필요하다면 `IntersectionObserver`가 더 효율적인 대안이다.
