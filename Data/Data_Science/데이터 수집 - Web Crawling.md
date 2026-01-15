# 데이터 수집 - Web Crawling 정리

웹 페이지 구조(HTML/CSS)의 이해를 바탕으로, 정적 페이지를 다루는 **BeautifulSoup**과 브라우저를 직접 조작하는 **Selenium**으로 데이터를 수집하는 방법을 정리한 문서입니다.

---

## 1. 웹 크롤링이란?

* **웹(Web) + 크롤링(Crawling, 기어다니다):** 인터넷 상의 웹 페이지를 자동으로 탐색하고 데이터를 수집하는 기술
* **스크래핑(Scraping):** 특정 웹페이지에서 원하는 데이터(텍스트, 이미지, 표 등)를 추출하는 과정
* **웹 크롤링의 특징:** 자동화, 대량 데이터 수집, 구조화된 데이터 추출

### 웹 크롤링의 필요성
매번 사이트에 접속해 상품명, 가격, 재고 상태 등을 일일이 확인하는 대신, 코드로 자동화하여 필요한 정보만 정리해서 가져올 수 있습니다.

### 웹 데이터 수집 방법
* 웹 페이지를 가져와서 필요한 정보만 추출하기 (스크래핑)
* WebDriver를 이용해 웹 브라우저 자동화하기 (스크래핑 or 크롤링)
* 제공된 OpenAPI를 이용해 실시간으로 데이터 가져오기

### 웹 페이지에서 데이터 추출
* 웹 페이지는 HTML(HyperText Markup Language)을 중심으로 이루어져 있음
* 원하는 주소의 웹 페이지로 들어가 HTML 내용을 가져오고, 그 안에서 원하는 데이터가 어디 있는지 가져옴(parsing)
* Python에는 정적 페이지에서는 **BeautifulSoup** 라이브러리를 주로 사용

---

## 2. 웹 페이지의 구조와 이해

### HTML vs CSS
* **HTML(HyperText Markup Language):** 웹 페이지를 구성하는 필수 언어로 제목, 단락, 목록 같은 본문을 위한 구조적 의미를 나타내는 것 뿐만 아니라 링크, 인용과 그 밖의 항목으로 구조적 문서를 만들 수 있는 방법을 제공
* **CSS(Cascading Style Sheets):** HTML의 요소의 외관을 디자인 함
* 웹 페이지는 HTML, CSS, JavaScript의 조합으로 구성됨
* 웹 크롤링을 통해 얻고자 하는 데이터는 HTML의 어딘가에 들어 있음 → HTML의 구조를 알아야 원하는 정보가 HTML의 어디에 위치하는지 파악할 수 있고, 데이터를 뽑아내 원하는 형태로 가공하는 작업을 파싱(parsing)이라 함

### HTML 기본 구성 요소
```html
<p align="center">Hello, HTML!</p>
```
* `<p>` : 시작 태그(Opening Tag), `</p>` : 종료 태그(Closing Tag)
* `align="center"` : **속성(Attribute)** — 태그의 추가적인 정보. 여러 개 부여할 수 있고, 없어도 됨 (예: 글자색, 크기, 배경색, 배경이미지, 여백 등)
* 시작 태그와 종료 태그 사이의 내용(Content) 전체를 **요소(Element)** 라고 함

### HTML 구조
```html
<html>
  <head>
    <title>Test홈페이지</title>
  </head>
  <body>
    환영합니다
  </body>
</html>
```
* `<html>`: 해당 프로그램 언어가 HTML이라는 것을 알려줌
* `<head>`: 문서 서문의 시작과 끝을 알려줌 (`<title>`은 문서의 제목을 나타내주는 태그)
* `<body>`: 문서 본문의 시작과 끝임을 알려줌

### CSS 기본 문법
```css
h1 { color: red; }
/* Selector(선택자) { Property(스타일속성): Property value(스타일속성값); } */
```

### CSS 선택자 종류
| 선택자 | 문법 예시 | 설명 |
| --- | --- | --- |
| 타입 선택자 | `p { color: red; }` | 태그 이름으로 선택 |
| 아이디 선택자 | `#item { color: yellow; }` | 해당 id 속성값을 가진 요소 선택 |
| 클래스 선택자 | `.item { color: yellow; }` | 해당 class를 가진 모든 요소 선택 |
| 자식 선택자 | `body > p { color: blue; }` | 바로 아래 자식 요소만 선택 |
| 가상 클래스 선택자 | `ul > li:nth-child(3) { color: yellow; }` | 몇 번째 자식인지 등 조건으로 선택 |

### Robots.txt란?
* 웹사이트 소유자가 검색 엔진 크롤러에게 사이트의 특정 경로를 크롤링해도 되는지에 대한 권고 규칙을 전달하기 위해 사용하는 표준 텍스트 파일
* 검색 엔진 크롤러를 대상으로 한 권고 규칙이며, 법적 강제력은 없이 기술적 차단을 의미하는 것도 아님. 검색 노출(인덱싱) 범위 제한 목적. 스크래핑의 합법·불법 기준은 아님
* `robots.txt` 위반만으로 불법이 되지는 않지만, 약관 위반·과도한 수집 등과 결합될 경우 고의적인 자동 수집으로 판단되는 근거로 사용될 수 있음

---

## 3. BeautifulSoup의 활용

* **BeautifulSoup이란?** HTML 및 XML 문서를 구문 분석하기 위한 Python 패키지로, 데이터를 쉽게 추출할 수 있도록 도와줌. 웹 브라우저가 하는 일과 비슷하게, HTML 소스를 트리 형태로 해석한 뒤 접근할 수 있음

### 기본 사용법
```python
import requests
from bs4 import BeautifulSoup

url = "https://snuco.snu.ac.kr/foodmenu"
html = requests.get(url).text
bs = BeautifulSoup(html, "html.parser")
print(bs.title)
# <title>식단 - 서울대학교 생활협동조합</title>
```

### 태그 검색과 텍스트 추출
```python
bs.select("td.lunch")           # td 태그이면서 class="lunch"인 모든 요소 찾기
bs.select("td.lunch")[1]        # 리스트의 두 번째 요소
bs.select("td.lunch")[1].text   # 텍스트 노드만 추출
bs.select_one("td.lunch")       # 첫 번째 요소만 반환

# BeautifulSoup 객체에서 태그 찾기(응용형)
(BeautifulSoup Object).select("태그명")
(BeautifulSoup Object).select("태그명.class")
(BeautifulSoup Object).select("태그명.class.subclass")
(BeautifulSoup Object).select("상위태그 > 하위태그")
(BeautifulSoup Object).select("상위태그.class > 하위태그.class")
(BeautifulSoup Object).select(".class")
(BeautifulSoup Object).select("#id")
(BeautifulSoup Object).select("태그명[속성=값]")
```
* `<td class="lunch">` 태그 내부의 텍스트로 구성되어 있음
* 메뉴명, 가격, 운영시간이 별도 태그로 분리되어 있지 않고, `<td class="lunch">` 태그 내부에 텍스트 노드 `<br>` 태그만 존재
* 이 경우 `<td>` 가운데 해당 클래스를 만족하는 것을 검색하면 점심 메뉴의 리스트를 얻을 수 있음
* 태그 객체 안에 들어있는 텍스트만 원한다면 `.text` 속성을 읽으면 됨. 이후 후처리는 파이썬 문법을 통해 진행할 수 있음(예: `.strip()`, `.split("\n")` 등)
* DOM을 잘 확인하는 것이 가장 중요. 개발자도구로 코드를 보면, 메뉴 전체는 `<table class="menu-table">`, 요일/식당별 메뉴는 `<tbody>` 안의 여러 `<tr>`로 구성되어 있고, 각 `<tr>`이 식당 1곳을 의미
* 태그의 식별을 위한 속성으로 주로 `id`와 `class`가 사용됨. `id`는 문서 내에서 유일한 존재이며, `class`는 중복될 수 있음. 따라서 이 경우는 `<td>` 가운데 해당 클래스를 만족하는 것을 검색해서 점심 메뉴의 리스트를 얻을 수 있음

---

## 4. Selenium의 활용

* **Selenium은 웹을 파싱하는 도구가 아니라 브라우저를 조종하는 도구**
* 정적 페이지를 크롤링하는 경우는 HTML을 받아서 파싱하는 방식
* **실제 서비스들은 대부분 자바스크립트로 화면을 그리는 형태**이며, 크롤링 관점에서 HTML을 가져오는 것으로는 한계가 있어 브라우저를 직접 조작해서 데이터를 가져와야 함
* 웹 데이터를 수집하는 것 뿐만 아니라 반복작업 자동화 및 웹 어플리케이션 테스트를 용이하게 함 (주소창에 URL 입력, 뒤로 가기/새로고침, 버튼 클릭, 입력창에 텍스트 입력, 화면 스크롤, 화면 캡처)

### Selenium이 필요한 이유
* BeautifulSoup은 결국 HTML을 가져와서 파싱하는 역할일 뿐
* Selenium의 핵심은 결국 웹 브라우저를 컨트롤할 수 있다는 것
* 브라우저를 코드로 제어. 화면에서 보이는 내용이라면 모두 컨트롤할 수 있음

| 개념 | 설명 |
| --- | --- |
| WebDriver | 브라우저 자체(크롬, 엣지 등) |
| Element | 화면에 있는 버튼, 입력창, 글자 등 |
| find_element | 화면에서 특정 요소를 찾는 역할 |
| action | 클릭, 입력, 스크롤, 대기 등 |

### 기본 사용법
```python
from selenium import webdriver
import chromedriver_autoinstaller

chromedriver_autoinstaller.install()
driver = webdriver.Chrome()

driver.get("https://www.naver.com")   # 페이지 이동
driver.refresh()                        # 새로고침 (F5와 동일한 동작)
print(driver.current_url)               # 현재 주소 확인

driver.back()                           # 페이지 뒤로가기 (브라우저의 뒤로 가기 버튼과 동일)
print(driver.current_url)
```
* `driver`가 브라우저를 조종하는 컨트롤러 역할
* `driver.get("주소")`: 주소창에 URL을 입력하고 접속하는 것과 동일. 웹 페이지 로딩이 완료되면 다음 코드 실행

### Action과 Element 대상 Action
| Action | 의미 |
| --- | --- |
| click() | 버튼, 링크 등 요소를 클릭 |
| send_keys() | 입력창에 텍스트 입력 |
| clear() | 입력창 내용 삭제 |
| submit() | form 전송 |
| get_attribute() | 요소의 속성값 조회 |
| text | 요소에 표시된 텍스트 추출 |

| Action (Navigation) | 의미 |
| --- | --- |
| get(url) | 지정한 URL로 이동 |
| back() | 이전 페이지로 이동 |
| forward() | 다음 페이지로 이동 |
| refresh() | 현재 페이지 새로고침 |
| close() | 현재 페이지 창 닫기 |
| quit() | 전체 브라우저 종료 |

* **Action:** 찾은 요소에 실제로 하는 행동
* **Element 대상 Action:** 요소(Element)를 정확히 찾고 적절한 타이밍에 행동(Action)을 주는 것
* **Browser 대상 Action(=Navigation):** 브라우저 상태 자체를 바꾸는 Action

### WebDriverWait과 대기
```python
from selenium.webdriver.support.ui import WebDriverWait

# 명시적 대기: 요소가 나타날 때까지 최대 10초 대기
wait = WebDriverWait(driver, 10)
```
* `driver`: 기다릴 브라우저
* `10`: 최대 대기 시간(초)
* 10초 안에 조건이 만족되면 즉시 다음 코드 실행, 10초가 지나도 안 되면 `TimeoutException` 발생

### Locator (요소 찾는 기준)
| Locator | 설명 |
| --- | --- |
| By.TAG_NAME | HTML 태그 이름으로 요소를 찾음 (예: p, div, a 같은 태그 전체를 가져올 때 사용) |
| By.ID | 태그의 id 속성값으로 찾음. 페이지 안에서 유일한 요소이기 때문에 가장 정확함 |
| By.CLASS_NAME | 태그의 class 속성으로 찾음. 같은 class를 가진 요소가 여러 개 있을 수 있음 |
| By.CSS_SELECTOR | CSS 문법으로 요소를 찾음. id, class, 태그를 자유롭게 조합 가능 (가장 많이 쓰임) |
| By.XPATH | HTML 구조(위치)를 기준으로 요소를 찾음. 문서 구조를 따라 위에서 아래로 내려가는 방식 |
| By.NAME | 태그의 name 속성값으로 찾음. 주로 form, input 태그에서 사용 |
| By.LINK_TEXT | `<a>` 태그 안의 전체 텍스트로 찾음. 메뉴나 링크 클릭할 때 사용 |
| By.PARTIAL_LINK_TEXT | `<a>` 태그 텍스트의 일부분만으로 찾음. 링크 글자가 길 때 유용 |

### ExpectedConditions (EC)
* 언제까지 기다릴 것인가를 정의하는 조건. `WebDriverWait`은 거의 `ExpectedConditions`와 거의 함께 사용
```python
search_box = WebDriverWait(driver, 10).until(
    EC.presence_of_element_located((By.ID, "query"))   # 어떤 요소를(Locator)
)
search_box.send_keys("파이썬")
```

| 조건 | 의미 |
| --- | --- |
| presence_of_element_located | 요소가 DOM에 존재 |
| visibility_of_element_located | 화면에 보이는 상태 |
| element_to_be_clickable | 클릭 가능 상태 |
| presence_of_all_elements_located | 여러 요소 존재 |
| url_contains | URL에 특정 문자열 포함 |

### 실전 예시: 뉴스 링크 목록 수집
```python
# 모든 <a> 태그를 가져온 뒤 조건으로 필터링
anchors = wait.until(
    EC.presence_of_all_elements_located((By.TAG_NAME, "a"))
)

articles = []
for a in anchors:
    title = (a.text or "").strip()
    link = a.get_attribute("href")
    if not title or not link:   # 텍스트나 링크가 없는 경우 제외
        continue
    if "n.news.naver.com" in link and "/article/" in link:
        articles.append((title, link))

# 화면에 보이는 텍스트가 "뉴스" 인 a 태그 클릭
news_tab = wait.until(
    EC.element_to_be_clickable((By.LINK_TEXT, "뉴스"))
)
news_tab.click()
```

---

## 핵심 요약
* 웹 크롤링은 HTML/CSS로 이루어진 웹 페이지 구조를 이해하고, 원하는 데이터가 위치한 태그를 찾아 파싱하는 작업입니다.
* **BeautifulSoup**은 정적 HTML을 받아 트리 구조로 파싱하는 라이브러리로, `select`/`select_one`으로 태그·클래스·id 기반 검색을 하고 `.text`로 내용을 추출합니다.
* **Selenium**은 파싱 도구가 아니라 **브라우저 자체를 조작하는 도구**로, 자바스크립트로 렌더링되는 동적 페이지나 로그인이 필요한 페이지에서 필수적입니다. `WebDriverWait` + `ExpectedConditions`로 요소가 준비될 때까지 기다린 뒤 `find_element`/`click`/`send_keys` 등으로 상호작용합니다.
* `robots.txt`는 법적 강제력이 있는 규칙이 아니라 크롤러에 대한 권고이지만, 약관 위반이나 과도한 수집과 결합되면 문제가 될 수 있으므로 항상 유의해야 합니다.
