# JS AJAX 정리

페이지 전체를 새로고침하지 않고 서버와 데이터를 주고받는 **비동기(Asynchronous)** 처리의 개념, 이를 구현하는 **AJAX**, 그리고 비동기 코드를 다루는 **Callback과 Promise**를 정리한 문서입니다.

---

## 1. 비동기(Asynchronous)

### 동기 vs 비동기

* **동기(Synchronous):** 코드가 작성된 순서대로, 이전 작업이 끝나야 다음 작업이 실행됨 (하나의 작업이 오래 걸리면 이후 코드 전체가 멈춤)
* **비동기(Asynchronous):** 오래 걸리는 작업(네트워크 요청 등)을 기다리지 않고, 먼저 다음 코드를 실행한 뒤 작업이 끝나면 결과를 나중에 처리

```javascript
console.log("1");
setTimeout(() => console.log("2"), 1000);   // 1초 뒤에 실행되도록 예약만 하고, 기다리지 않음
console.log("3");

// 실행 순서: 1 -> 3 -> (1초 후) -> 2
```

* JavaScript는 **싱글 스레드**로 동작하지만, 브라우저가 제공하는 **이벤트 루프(Event Loop)** 덕분에 네트워크 요청 같은 오래 걸리는 작업을 기다리지 않고 다른 코드를 계속 실행할 수 있음

---

## 2. Ajax (Asynchronous JavaScript and XML)

* 페이지 전체를 새로고침하지 않고, **자바스크립트로 서버에 요청을 보내고 필요한 부분만 업데이트**하는 기술
* 이름에 XML이 들어가지만, 오늘날은 대부분 **JSON**으로 데이터를 주고받음

```javascript
// Fetch API를 활용한 AJAX 요청
fetch("https://api.example.com/articles")
  .then((response) => response.json())     // 응답 body를 JSON으로 파싱
  .then((data) => {
      console.log(data);
      renderArticles(data);                    // 받아온 데이터로 DOM 갱신
  })
  .catch((error) => console.error("요청 실패:", error));
```

```javascript
// Axios 라이브러리를 활용한 AJAX 요청 (더 간결한 문법, 자동 JSON 파싱)
axios.get("https://api.example.com/articles")
  .then((response) => renderArticles(response.data))
  .catch((error) => console.error(error));
```

---

## 3. Callback 과 Promise

### 콜백(Callback)과 콜백 지옥(Callback Hell)

* 비동기 작업이 끝난 뒤 실행할 함수를 인자로 넘기는 방식이 콜백이며, 비동기 작업이 여러 번 중첩되면 코드가 계단식으로 깊어지는 **콜백 지옥**이 발생하기 쉬움

```javascript
getUser(id, (user) => {
    getArticles(user, (articles) => {
        getComments(articles[0], (comments) => {
            console.log(comments);   // 중첩이 깊어질수록 가독성이 크게 떨어짐
        });
    });
});
```

### Promise

* 비동기 작업의 **최종 결과(성공/실패)** 를 나타내는 객체로, `.then()`으로 체이닝해 콜백 지옥을 평탄하게 풀어낼 수 있음

```javascript
function getUser(id) {
    return new Promise((resolve, reject) => {
        // 비동기 작업 수행
        if (성공) resolve(userData);   // 성공 시 resolve
        else reject(new Error("실패"));  // 실패 시 reject
    });
}

getUser(1)
  .then((user) => getArticles(user))    // 이전 Promise의 결과를 이어받아 체이닝
  .then((articles) => getComments(articles[0]))
  .then((comments) => console.log(comments))
  .catch((error) => console.error(error));   // 체인 중 어디서든 실패하면 여기서 한 번에 처리
```

### async / await

* Promise 체인을 **동기 코드처럼 보이도록** 작성할 수 있게 해주는 문법 (내부 동작은 여전히 비동기)

```javascript
async function loadComments() {
    try {
        const user = await getUser(1);
        const articles = await getArticles(user);
        const comments = await getComments(articles[0]);
        console.log(comments);
    } catch (error) {
        console.error(error);
    }
}
```

* `await`는 Promise가 처리(resolve/reject)될 때까지 기다렸다가 결과값을 반환 — 반드시 `async` 함수 내부에서만 사용 가능

---

## 핵심 요약
* 비동기 처리는 오래 걸리는 작업을 기다리지 않고 다음 코드를 먼저 실행한 뒤, 작업이 끝나면 결과를 나중에 처리하는 방식이다.
* **AJAX**는 페이지 전체 새로고침 없이 서버와 데이터를 주고받는 기술로, `fetch`나 `axios`로 구현하며 오늘날은 대부분 JSON을 사용한다.
* **콜백**은 중첩될수록 콜백 지옥에 빠지기 쉬우며, **Promise**의 `.then()` 체이닝과 이를 동기 코드처럼 작성하게 해주는 **async/await**로 가독성 좋은 비동기 코드를 작성할 수 있다.
