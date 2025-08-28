# Web Responsive Web 정리

화면 크기에 따라 유동적으로 배치가 바뀌는 **반응형 웹(Responsive Web)** 을 위한 **Bootstrap Grid System**과, 지금까지 배운 레이아웃 지식을 종합 정리하고 **UX & UI**의 기본 원칙을 정리한 문서입니다.

---

## 1. Bootstrap Grid System

### 12 컬럼 그리드

* Bootstrap의 그리드 시스템은 한 행(row)을 **12개의 컬럼(column)** 으로 나누어, 원하는 비율로 요소를 배치할 수 있게 함
* `container > row > col` 3단 구조로 사용

```html
<div class="container">
  <div class="row">
    <div class="col-4">1/3 너비</div>
    <div class="col-8">2/3 너비</div>
  </div>
</div>
```

* `col-4` + `col-8` = 12 → 한 행을 정확히 채움
* `container`: 좌우 여백과 최대 너비를 관리, `row`: 컬럼들을 가로로 배치(내부적으로 Flexbox 사용), `col-*`: 실제 콘텐츠가 담기는 영역

---

## 2. Grid system for responsive web (반응형 그리드)

### 반응형 브레이크포인트

Bootstrap은 화면 너비에 따라 다른 컬럼 비율을 지정할 수 있는 **반응형 클래스 접두사**를 제공합니다.

| 접두사 | 대상 화면 너비 |
| --- | --- |
| `col-` | 모든 화면 (기본) |
| `col-sm-` | ≥576px (모바일 가로 등) |
| `col-md-` | ≥768px (태블릿) |
| `col-lg-` | ≥992px (데스크톱) |
| `col-xl-` | ≥1200px (큰 데스크톱) |

```html
<div class="row">
  <!-- 모바일에서는 한 줄 전체(12), 태블릿부터는 절반(6), 데스크톱부터는 1/3(4) -->
  <div class="col-12 col-md-6 col-lg-4">카드 1</div>
  <div class="col-12 col-md-6 col-lg-4">카드 2</div>
  <div class="col-12 col-md-6 col-lg-4">카드 3</div>
</div>
```

* 여러 접두사를 한 요소에 함께 지정하면, **화면이 커질수록 더 좁은 컬럼(더 많은 개수를 한 줄에)** 배치하는 전형적인 반응형 카드 레이아웃을 구현할 수 있음

### CSS 미디어 쿼리 (직접 구현 시)

```css
.card { width: 100%; }

@media (min-width: 768px) {
    .card { width: 50%; }
}
@media (min-width: 992px) {
    .card { width: 33.33%; }
}
```

* Bootstrap의 `col-md-*` 등의 반응형 클래스는 내부적으로 이러한 `@media` 쿼리를 미리 정의해둔 것

---

## 3. CSS Layout 종합 정리

지금까지 배운 레이아웃 기술을 상황에 맞게 조합하는 것이 실전 웹 개발의 핵심입니다.

| 상황 | 적합한 기술 |
| --- | --- |
| 요소 하나를 특정 위치에 고정/겹치기 | `position` (absolute, fixed) |
| 한 줄/한 열로 나열, 정렬 | `Flexbox` |
| 전체 페이지의 grid 구조(카드 목록 등) | `Bootstrap Grid` 또는 `CSS Grid` |
| 화면 크기별 다른 레이아웃 | `반응형 클래스` 또는 `@media` 쿼리 |

* Box Model(여백) → Position(배치) → Flexbox/Grid(정렬·분포) → 반응형(화면 대응) 순으로 레이어를 쌓아 올린다고 생각하면 전체 CSS 레이아웃 체계를 이해하기 쉬움

---

## 4. UX & UI

### UX(User Experience)와 UI(User Interface)

| 구분 | 의미 |
| --- | --- |
| **UI** | 사용자가 서비스와 상호작용하는 시각적 화면/요소 (버튼, 폼, 색상 등) |
| **UX** | 서비스를 사용하는 과정에서 사용자가 느끼는 전체적인 경험 (편의성, 만족도) |

* 좋은 UI는 좋은 UX를 위한 수단 중 하나이며, UX는 UI뿐 아니라 속도, 정보 구조, 접근성 등을 모두 포함하는 더 넓은 개념

### 반응형 웹에서의 UX 원칙

* **일관성:** 화면 크기가 바뀌어도 핵심 기능의 위치/동작 방식은 일관되게 유지
* **터치 영역 고려:** 모바일에서는 버튼 등 터치 요소의 최소 크기(약 44px 권장)를 확보
* **콘텐츠 우선순위:** 작은 화면에서는 핵심 정보를 먼저 보여주고, 부가 정보는 아래로 배치하거나 숨김

---

## 핵심 요약
* **Bootstrap Grid System**은 한 행을 12개의 컬럼으로 나누는 `container > row > col` 구조이며, `col-md-*` 등 반응형 접두사로 화면 크기별 다른 배치를 손쉽게 구현할 수 있다.
* 실전 레이아웃은 **Box Model(여백) → Position(배치) → Flexbox/Grid(정렬) → 반응형(화면 대응)** 을 상황에 맞게 조합해서 완성한다.
* **UX/UI**는 화면(UI)과 경험(UX)을 구분하는 개념으로, 반응형 웹에서는 일관성·터치 영역·콘텐츠 우선순위를 고려해 다양한 화면 크기에서도 좋은 사용자 경험을 제공해야 한다.
