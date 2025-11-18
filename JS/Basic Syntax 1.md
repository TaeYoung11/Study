# JS Basic Syntax 1 정리

JavaScript의 기본 **데이터 타입**과, 값을 처리하는 단위인 **함수**의 기초 문법을 정리한 문서입니다.

---

## 1. 데이터 타입

### 원시 타입 (Primitive Type)

| 타입 | 설명 |
| --- | --- |
| `Number` | 정수/실수 구분 없이 하나의 숫자 타입 |
| `String` | 문자열 (`"큰따옴표"`, `'작은따옴표'`, `` `백틱(템플릿 리터럴)` ``) |
| `Boolean` | `true` / `false` |
| `undefined` | 값이 할당되지 않은 상태 |
| `null` | 값이 의도적으로 "없음"을 나타내는 상태 |

```javascript
const age = 25;
const name = "홍길동";
const greeting = `안녕하세요, ${name}님! 나이는 ${age}살입니다.`;   // 템플릿 리터럴로 변수 삽입
```

### 변수 선언 — `let`, `const`, (`var`)

| 키워드 | 재할당 | 재선언 | 스코프 |
| --- | --- | --- | --- |
| `var` | 가능 | 가능 | 함수 스코프 (레거시, 지양) |
| `let` | 가능 | 불가능 | 블록 스코프 |
| `const` | 불가능 | 불가능 | 블록 스코프 |

* 재할당이 필요 없다면 `const`를 기본으로 사용하고, 값이 바뀌어야 할 때만 `let`을 사용하는 것이 최신 JS 스타일 관례

### 타입 변환

```javascript
Number("10")     // 10 (문자열 -> 숫자)
String(10)        // "10" (숫자 -> 문자열)
Boolean(0)         // false (0, "", null, undefined, NaN은 falsy)

10 + "20"           // "1020" (문자열과 결합 시 문자열로 자동 변환)
10 == "10"           // true  (값만 비교, 타입 변환 허용 — 지양)
10 === "10"           // false (값과 타입까지 엄격 비교 — 권장)
```

* 비교 연산자는 항상 **`===`(일치 연산자)** 를 사용해 예상치 못한 타입 변환으로 인한 버그를 방지하는 것이 권장됨

---

## 2. 함수

### 함수 선언 방식

```javascript
// 1) 함수 선언식 (Function Declaration) — 호이스팅되어 선언 전에도 호출 가능
function add(a, b) {
    return a + b;
}

// 2) 함수 표현식 (Function Expression) — 변수 할당 이후에만 호출 가능
const subtract = function (a, b) {
    return a - b;
};

// 3) 화살표 함수 (Arrow Function) — 더 간결한 문법, this 바인딩 방식이 다름
const multiply = (a, b) => a * b;
```

### 매개변수 관련 문법

```javascript
function greet(name = "손님") {     // 기본값 매개변수
    console.log(`안녕하세요, ${name}님`);
}

function sum(...numbers) {           // 나머지 매개변수 (Rest Parameter): 여러 인자를 배열로 받음
    return numbers.reduce((acc, cur) => acc + cur, 0);
}
```

* 화살표 함수는 자신만의 `this`를 가지지 않고 **바깥 스코프의 `this`를 그대로 사용**하는 특징이 있어, DOM 이벤트 핸들러 등에서 일반 함수와 동작 차이가 발생할 수 있음

---

## 핵심 요약
* JavaScript의 원시 타입은 `Number`, `String`, `Boolean`, `undefined`, `null`로 구성되며, 변수 선언은 재할당이 필요 없다면 `const`를 우선 사용한다.
* 비교 연산에는 타입까지 엄격히 비교하는 `===`을 사용해 암묵적 타입 변환에 의한 버그를 방지해야 한다.
* 함수는 선언식/표현식/화살표 함수로 정의할 수 있으며, 화살표 함수는 자신만의 `this`를 갖지 않는다는 점이 일반 함수와의 핵심적인 차이다.
