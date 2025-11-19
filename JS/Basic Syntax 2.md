# JS Basic Syntax 2 정리

여러 속성을 묶어 표현하는 **객체**, 순서가 있는 데이터의 모음인 **배열**, 그리고 배열을 다루는 다양한 **Array Helper Method**를 정리한 문서입니다.

---

## 1. 객체 (Object)

```javascript
const user = {
    name: "홍길동",
    age: 25,
    greet() {                       // 메서드(객체 안의 함수)
        console.log(`안녕하세요, ${this.name}입니다.`);
    },
};

user.name;          // 점 표기법으로 접근
user["age"];          // 대괄호 표기법으로 접근 (변수로 키를 지정할 때 유용)
user.email = "a@a.com";   // 새 속성 동적 추가

// 구조 분해 할당 (Destructuring)
const { name, age } = user;

// 전개 구문 (Spread)으로 객체 복사/병합
const updatedUser = { ...user, age: 26 };   // 기존 값 복사 후 age만 덮어쓰기
```

---

## 2. 배열 (Array)

```javascript
const numbers = [1, 2, 3, 4, 5];

numbers.push(6);        // 맨 뒤에 추가
numbers.pop();            // 맨 뒤 제거
numbers.unshift(0);        // 맨 앞에 추가
numbers.shift();             // 맨 앞 제거

numbers.length;                // 배열의 길이
numbers[0];                     // 인덱스로 접근

const [first, second] = numbers;   // 배열 구조 분해 할당
const combined = [...numbers, 7, 8];   // 전개 구문으로 배열 복사/병합
```

---

## 3. Array helper method

배열을 순회하며 데이터를 가공하는 대표적인 고차 함수(Higher-order Function)들입니다.

```javascript
const nums = [1, 2, 3, 4, 5];

nums.forEach((n) => console.log(n));            // 각 요소에 대해 실행만 함 (반환값 없음)

const doubled = nums.map((n) => n * 2);            // 각 요소를 변환한 새 배열 반환
const evens = nums.filter((n) => n % 2 === 0);      // 조건을 만족하는 요소만 모은 새 배열 반환
const sum = nums.reduce((acc, cur) => acc + cur, 0);  // 누적 계산으로 값 하나를 도출

const found = nums.find((n) => n > 3);               // 조건을 만족하는 첫 번째 요소 반환
const hasEven = nums.some((n) => n % 2 === 0);         // 하나라도 조건을 만족하면 true
const allPositive = nums.every((n) => n > 0);           // 모두 조건을 만족해야 true

const sorted = [...nums].sort((a, b) => b - a);          // 내림차순 정렬 (원본 변경 주의, 복사 후 정렬 권장)
```

| 메서드 | 반환값 | 용도 |
| --- | --- | --- |
| `forEach` | `undefined` | 단순 반복 실행 |
| `map` | 새 배열 (변환된 값) | 각 요소를 다른 값으로 변환 |
| `filter` | 새 배열 (조건 통과 요소) | 조건에 맞는 요소만 추출 |
| `reduce` | 누적된 값 하나 | 합계, 평균 등 하나의 결과로 압축 |
| `find` | 요소 하나 (또는 undefined) | 조건에 맞는 첫 요소 찾기 |

* `sort()`는 원본 배열을 직접 변경(mutate)하는 메서드이므로, 원본을 보존하고 싶다면 `[...nums].sort()`처럼 복사본을 만든 뒤 정렬하는 것이 안전함

---

## 핵심 요약
* **객체**는 구조 분해 할당(`{ name, age }`)과 전개 구문(`{ ...obj }`)으로 값을 꺼내거나 복사/병합할 수 있다.
* **배열**도 동일하게 구조 분해와 전개 구문을 지원하며, `push`/`pop`/`shift`/`unshift`로 앞뒤에 요소를 추가/제거한다.
* `map`(변환), `filter`(조건 추출), `reduce`(값 하나로 누적) 세 가지 **Array Helper Method**는 반복문 없이 데이터를 선언적으로 가공하는 JavaScript의 핵심 도구이다.
