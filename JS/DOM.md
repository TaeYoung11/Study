# JS DOM 정리

JavaScript의 등장 배경을 간단히 살펴보고, HTML 문서를 프로그래밍적으로 다룰 수 있게 해주는 **DOM(Document Object Model)** 의 개념과 기본 조작 방법을 정리한 문서입니다.

---

## 1. History of JavaScript

* 1995년 넷스케이프의 브렌던 아이크가 열흘 만에 만든 언어로 시작, 이후 웹 표준화 기구(ECMA)를 통해 **ECMAScript**라는 표준 명세로 발전
* 초기에는 브라우저에서 간단한 상호작용을 위한 언어였지만, 현재는 Node.js를 통해 **서버 사이드**에서도 사용되는 범용 언어로 확장
* 브라우저에서 JavaScript는 크게 세 가지 역할을 함
  1. **DOM 조작:** HTML 요소를 선택하고 변경
  2. **이벤트 처리:** 사용자의 동작(클릭, 입력 등)에 반응
  3. **비동기 통신:** 서버와 데이터를 주고받음 (AJAX 등)

---

## 2. DOM (Document Object Model)

### DOM이란?

* 브라우저가 HTML 문서를 읽어 들여 만드는, **트리(Tree) 구조의 객체 모델**
* HTML의 각 태그가 하나의 **노드(Node)** 가 되며, JavaScript는 이 DOM 트리에 접근해 요소를 선택하고 변경할 수 있음

```html
<html>
  <body>
    <h1>제목</h1>
    <p>문단</p>
  </body>
</html>
```

```
document
 └── html
      └── body
           ├── h1
           └── p
```

### DOM 요소 선택

```javascript
document.getElementById("title");            // id로 하나의 요소 선택
document.getElementsByClassName("item");        // class로 여러 요소 선택 (HTMLCollection)
document.querySelector(".item");                  // CSS 선택자로 첫 번째 요소 선택
document.querySelectorAll(".item");                // CSS 선택자로 모든 요소 선택 (NodeList)
```

### DOM 조작

```javascript
const title = document.querySelector("#title");

title.textContent = "새 제목";            // 텍스트 내용 변경
title.innerHTML = "<b>강조된 제목</b>";     // HTML 태그를 포함해 내용 변경
title.style.color = "blue";                // 인라인 스타일 변경
title.classList.add("highlight");            // 클래스 추가
title.classList.remove("highlight");          // 클래스 제거
title.classList.toggle("active");             // 있으면 제거, 없으면 추가

// 요소 생성 및 추가
const newLi = document.createElement("li");
newLi.textContent = "새 항목";
document.querySelector("ul").appendChild(newLi);
```

* `innerHTML`은 문자열을 HTML로 해석해 삽입하므로, **사용자 입력값을 그대로 넣으면 XSS(스크립트 삽입) 공격에 취약**할 수 있어 신뢰할 수 없는 데이터에는 `textContent`를 사용하는 것이 안전함

---

## 핵심 요약
* JavaScript는 브라우저 상호작용을 위해 시작되어, ECMAScript 표준화를 거쳐 지금은 DOM 조작·이벤트 처리·비동기 통신을 아우르는 범용 언어로 자리잡았다.
* **DOM**은 HTML 문서를 트리 구조의 객체로 표현한 것으로, `querySelector` 계열 함수로 요소를 선택하고 `textContent`/`classList`/`style` 등으로 내용과 스타일을 변경한다.
* 신뢰할 수 없는 데이터를 화면에 표시할 때는 `innerHTML` 대신 `textContent`를 사용해 XSS 위험을 피해야 한다.
