# Web Bootstrap 정리

CSS를 처음부터 직접 작성하지 않고도 빠르게 스타일링할 수 있게 해주는 CSS 프레임워크 **Bootstrap**과, 브라우저마다 다른 기본 스타일을 통일하는 **Reset CSS**, 그리고 문서 구조화의 핵심인 **Semantic Web**을 정리한 문서입니다.

---

## 1. Bootstrap

### Bootstrap이란?

* 트위터(현 X)에서 만든 오픈소스 **CSS 프레임워크**로, 미리 만들어진 CSS 클래스와 컴포넌트를 조합해 빠르게 UI를 구성할 수 있음
* 반응형 그리드 시스템, 버튼/카드/모달 등의 컴포넌트, 유틸리티 클래스를 기본 제공

### 적용 방법

```html
<!-- CDN으로 간단히 불러오기 -->
<link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/css/bootstrap.min.css" rel="stylesheet">
<script src="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/js/bootstrap.bundle.min.js"></script>
```

```html
<button class="btn btn-primary">저장</button>
<div class="card">
  <div class="card-body">
    <h5 class="card-title">카드 제목</h5>
    <p class="card-text">카드 내용입니다.</p>
  </div>
</div>
```

* `class` 속성에 미리 정의된 이름을 조합하는 것만으로 버튼, 카드, 알림창 등의 UI를 별도 CSS 작성 없이 완성할 수 있음

---

## 2. Reset CSS

### 필요한 이유

* 브라우저(Chrome, Firefox, Safari 등)마다 `h1`, `ul`, `button` 같은 태그에 적용하는 **기본 스타일(User Agent Stylesheet)** 이 조금씩 다름
* 이 차이 때문에 같은 코드라도 브라우저마다 다르게 보일 수 있어, 이를 **초기화(Reset)** 한 뒤 일관된 스타일을 새로 쌓아 올리는 것이 안전함

### 대표적인 방식

```css
/* 대표적인 리셋 예시 */
* {
    margin: 0;
    padding: 0;
    box-sizing: border-box;
}
ul, ol {
    list-style: none;   /* 리스트의 기본 불릿/번호 제거 */
}
a {
    text-decoration: none;   /* 링크의 기본 밑줄 제거 */
    color: inherit;
}
```

* Bootstrap 등 CSS 프레임워크에도 내부적으로 **Reboot**(Bootstrap의 리셋 스타일시트)이 포함되어 있어, 프레임워크를 사용하면 별도로 리셋 CSS를 작성하지 않아도 되는 경우가 많음

---

## 3. Bootstrap 활용

### 그리드/레이아웃 유틸리티

```html
<div class="d-flex justify-content-between align-items-center p-3">
  <h1 class="m-0">로고</h1>
  <button class="btn btn-outline-secondary">로그인</button>
</div>
```

* `d-flex`, `justify-content-*`, `p-3`(padding), `m-0`(margin) 등 **유틸리티 클래스**만으로 앞서 배운 Flexbox/Box Model 스타일을 클래스 조합으로 빠르게 적용 가능

### 커스터마이징

* 필요한 부분만 Bootstrap 기본값을 덮어쓰는 방식으로 커스텀 스타일을 추가 (직접 만든 CSS를 Bootstrap CSS **다음에** 로드해야 우선 적용됨)

---

## 4. Semantic Web

* **시맨틱 웹:** 문서의 내용에 의미(semantic)를 부여해, 사람뿐 아니라 **기계(검색 엔진, 스크린 리더)** 도 내용을 이해할 수 있도록 만드는 웹의 방향성
* HTML의 시맨틱 태그(`header`, `nav`, `main`, `article` 등)와 `alt` 속성, ARIA 속성 등이 시맨틱 웹을 실현하는 구체적인 도구

```html
<img src="chart.png" alt="2025년 분기별 매출 추이 그래프">
<nav aria-label="주 메뉴">...</nav>
```

* 시맨틱하게 작성된 문서는 **검색엔진 최적화(SEO)** 와 **웹 접근성(Accessibility)** 두 가지 측면에서 모두 유리함

---

## 핵심 요약
* **Bootstrap**은 미리 정의된 CSS 클래스로 버튼/카드/그리드 등의 UI를 빠르게 구성할 수 있는 CSS 프레임워크이다.
* **Reset CSS**는 브라우저마다 다른 기본 스타일 차이를 없애 일관된 화면을 보장하는 역할을 하며, Bootstrap에는 이미 Reboot이라는 리셋 스타일이 내장되어 있다.
* **Semantic Web**은 시맨틱 태그와 `alt`/ARIA 속성 등으로 문서에 의미를 부여해, 검색엔진과 스크린 리더 모두가 콘텐츠를 올바르게 해석할 수 있도록 하는 것을 목표로 한다.
