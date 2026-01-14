# 데이터 수집 - OpenAPI 정리

웹에서 데이터를 수집하는 방법 중 하나인 **OpenAPI**의 개념과, 공공데이터 포털·네이버 등 실제 OpenAPI를 활용해 데이터를 수집·전처리하는 방법을 정리한 문서입니다.

---

## 1. OpenAPI란?

* **OpenAPI(Open Application Programming Interface):** 누구나 사용할 수 있도록 공개된 API
* 웹 사이트가 가진 기능 모두를 이용할 수 있도록 공개한 프로그래밍 인터페이스
* 네이버 지도, 구글 맵, 공공데이터 포털 등이 대표적인 예
* 많은 서비스 업체 및 공공 기관에서 제공하는 서비스를 외부에서 사용할 수 있게 API를 제공하는 형태

```
서버 --(데이터)--> User --> 서비스 / 애플리케이션 / 플랫폼
```

* 대부분 OpenAPI 서비스는 HTTP 프로토콜의 GET, POST 등의 메서드를 사용해 자원이나 서비스를 요청
* 일반적인 웹 페이지에 GET 요청을 보내면 HTML 문서를 응답해주지만, **OpenAPI에 GET 요청을 보내면 데이터를 정해진 형식의 텍스트로 응답**해줌

### OpenAPI의 데이터 유형
| 유형 | 설명 |
| --- | --- |
| XML | 트리 형태의 구조. 각 자료마다 태그를 붙여서 자료의 형태를 표현 |
| CSV | 콤마로 자료 내용을 구분 |
| JSON | 키 : 값 페어로 데이터 표현 |

```xml
<?xml version="1.0" encoding="UTF-8"?>
<EmployeeData>
  <employee id="34594">
    <firstName>Heather</firstName>
    <lastName>Banks</lastName>
    <hireDate>1/19/1998</hireDate>
    <deptCode>BB001</deptCode>
    <salary>72000</salary>
  </employee>
</EmployeeData>
```
```json
{
  "FirstName": "Sam",
  "LastName": "Jackson",
  "employeeID": 5698523,
  "Designation": "Manager"
}
```

### OpenAPI가 제공하는 데이터 제공 유형
| 유형 | 설명 | 확장자 예시 |
| --- | --- | --- |
| 텍스트 | 워드나 URL로 요청을 보낸 뒤, 응답 문자열을 html로 받아봄 | txt, ini 등 |
| 문서 | 이미지, 동영상 등 멀티미디어 포함이 가능하고 XML 포맷으로 작성된 문서 (윈도우, 리눅스 등에서 제공하는 기본 텍스트 편집 파일 형식) | hwpx, docx 등 |
| 이미지 | 이미지, 동영상 등 멀티미디어 포함이 가능하고 XML 포맷으로 작성된 문서, 레스터/벡터 형식의 파일로 배열 형태의 이미지 | jpg, png, svg 등 |
| 음성(음향) | 소리, 음성 등을 재생하기 위한 디지털 오디오 데이터 | mp3, acc 등 |
| 영상(동영상) | 프레임을 활용하여 만들어진 움직이는 이미지와 음성 및 음향의 조합 | mp4, mpg 등 |
| 공간 정보 | 지리정보, 위치정보 등 공간 데이터를 포함하는 비정형 데이터 유형 | Shapefile(SHP, SHX, DBF) 등 |

---

## 2. HTTP 통신과 Requests 라이브러리

* **HTTP 통신:** 웹 브라우저와 웹 서버 사이에 데이터를 주고 받는데 사용되는 통신. "요청(Request)"을 보내고 "응답(Response)"을 받는 구조
* **Requests 라이브러리:** 접근할 웹 페이지의 데이터를 요청/응답 받기 위한 파이썬 라이브러리

```python
import requests as rq

res = rq.get("http://www.naver.com")
# <Response [200]> → 데이터를 잘 가지고있다는 뜻. 통신에 성공했다는 뜻
res    # Response 변수에서 내용만 보고싶다면 res.text
```

### GET 메소드와 URL
* GET 메소드는 웹 서버에게 파라미터를 포함해 요청을 보내는 가장 쉬운 방법으로, URL에 파라미터 정보를 담아서 보내는 것
* 사용자가 서버에게 웹 페이지를 보여 달라고 하는 것을 요청이라 하고, 서버가 요청에 대한 대답을 담아 HTML 문서로 주는 것을 응답이라고 함
```
http://www.example.com/login?id=bigdata&password=123456
```
* 요청을 보낼 페이지의 URL과 파라미터 사이는 반드시 `?`로 구분
* 파라미터는 `변수명=값` 형태로 나열되며, 변수 사이는 `&`로 구분

### Python으로 GET 사용하기
```python
import requests

# 원하는 url로 요청을 보낸 뒤, 응답 문자열을 html로 받아보기
url = "https://smoyo.wo.ac.kr/foadmory/"
html = requests.get(url).text
title_begin = html.index("<title>")
title_end = html.index("</title>")
title = html[title_begin:title_end]
print(title)
```

---

## 3. 공공데이터 포털의 OpenAPI 활용

### 사용 절차
```
사용자 --(API key)--> 공공데이터 포털 --(오픈 API 호출)--> 제공기관
사용자 <--(API 결과 응답)-- 공공데이터 포털 <--(API 회신)-- 제공기관
```

1. 공공데이터포털(`https://www.data.go.kr/index.do`)에 접속한다
2. 검색창에 관심 주제를 검색한다. 혹은 인기검색어로 검색한다
3. 분류에서 파일데이터는 다운로드 가능한 과거 데이터, 오픈API는 실시간 상황이 반영되는 API를 의미. 오픈API를 원하는 주제를 선택한다
4. 적절히 활용 목적을 쓰고, 동의 후 활용신청 버튼을 누른다 (검토단계 없음)
5. 이후 마이페이지 → 오픈API → 인증키발급현황에서 인증키를 확인한다
   * 오픈API는 서버의 과도한 부담을 막기 위해 인증키를 사용하는 경우가 많음. 동일한 인증키로 과도한 요청이 들어오면, 해당 키의 권한이 일시정지된다
6. 다시 해당 OpenAPI의 상세페이지로 돌아가면, 요청변수에 무엇을 입력해야 하는지 설명되어 있다. 샘플데이터의 입력방식을 참고하여 GET 요청을 보낸다. `ServiceKey`만 반드시 입력해야 하고, 옵션 항목은 GET에 포함시키지 않아도 무방하다

### 응답 데이터 구조 이해 및 추출
```python
import requests
from bs4 import BeautifulSoup

url = "http://openapi.data.go.kr/openapi/service/rest/Covid19/getCovid19InfStateJson"
res = rq.get(url).text
bs = BeautifulSoup(res, "xml")
items = bs.select("item")
for item in items:
    confCase = item.select_one("confCase").text
    gubun = item.select_one("gubun").text
    print(f"구분: {gubun}, 확진자수: {confCase}")
```
* 구분자 `?` 와 `&`의 위치를 유의하여 URL 변수를 작성한다
* Optional 파라미터는 공란으로 두더라도, 답안변수명은 써주어야 한다
* python에서 행이 너무 길어지면 줄 끝에 `\`를 삽입해서 줄 바꿈 표시 가능
* 응답 텍스트가 길게 나타나면 성공

### 응답 데이터 전처리
```python
import pandas as pd

df = pd.DataFrame(data)
df.tail()
df.isna().sum()
print("중복 개수:", df.duplicated().sum())

# 연담다 데이터에서 남기기 (숫자-숫자 or '이상' 표기)
age_df = df[df["구분"].str.contains(r"\d")].copy()

def extract_age(group):
    if "이상" in group:
        return int(group.replace(" 이상", ""))
    else:
        return int(group.split("-")[0])

age_df["연령대"] = age_df["구분"].apply(extract_age)

pivot_df = df.pivot_table(values="확진자수", index="날짜", columns="구분", aggfunc="sum")
```

---

## 4. 기업 OpenAPI 활용

* 공공데이터는 정형·정적 데이터 위주이나, **기업 API는 실시간 / 서비스 중심 데이터** 등을 다양하게 제공
* 다양한 기업에서 OpenAPI를 제공 (Third-Party Developer Community를 통해 Backend Systems, Open API, Mobile & Web Applications를 연결 → Extend Customer Reach, Increase Revenue, Stimulate Innovation)

### 로그인 기반 API vs 비로그인 API
| 구분 | 로그인 방식 API (OAuth 기반) | 비로그인 방식 API (API Key 기반) |
| --- | --- | --- |
| 인증 방식 | OAuth (토큰) | API Key |
| 사용자 개입 | 필요 | 불필요 |
| 목적 | 사용자 기능 제공 | 데이터 조회 중심 |
| 활용 예 | 로그인, 글쓰기 | 검색, 분석 |

* **로그인 방식 OpenAPI:** 네이버 로그인 인증 필요(OAuth), 사용자 권한을 위임받아 기능을 수행하는 것이 주 목적
* **비로그인 방식 OpenAPI:** Client ID / Client Secret으로 인증, 사용자 로그인 불필요, 데이터 수집 목적의 비로그인 방식 위주로 확인
  * 실용용이라면 아무 의미 없는 값을 넣어도 된다. 네이버 OpenAPI → 비로그인(Open API) 서비스 설정 화면에서, 웹 서비스 URL의 의미는 네이버 로그인(OAuth)을 사용하는 서비스 주소를 뜻하며, 그냥 API 호출만 하는 경우는 의미가 없음(로그인 API 사용, OAuth 인증 흐름 사용, 로그인 성공 후 redirect 되는 URL이 필요할 때만 의미 있음)

### Naver OpenAPI를 활용한 뉴스 검색
```python
import requests as rq
from dotenv import load_dotenv
import os

load_dotenv()
CLIENT_ID = os.getenv("NAVER_CLIENT_ID")
CLIENT_SECRET = os.getenv("NAVER_CLIENT_SECRET")

url = "https://openapi.naver.com/v1/search/news.json"
headers = {
    "X-Naver-Client-Id": CLIENT_ID,
    "X-Naver-Client-Secret": CLIENT_SECRET,
}
params = {"query": "빅데이터", "display": 5, "start": 1}
response = requests.get(url, headers=headers, params=params)
data = response.json()
```

```python
import pandas as pd
from html import unescape

def clean_html(text: str) -> str:
    if text is None:
        return text
    text = unescape(text)
    text = re.sub("<.*?>", "", text)
    return text

rows = []
for item in items:
    rows.append({
        "title": clean_html(item.get("title")),
        "description": clean_html(item.get("description")),
        "link": item.get("link"),
        "pubDate": item.get("pubDate"),
        "originallink": item.get("originallink"),
    })
df = pd.DataFrame(rows)
```
* `.env` 파일 등을 이용해 Client ID/Secret 같은 민감정보는 코드에 직접 노출하지 않고 환경변수로 관리하는 것이 안전합니다.

### 기업 OpenAPI 활용의 특징
* Naver OpenAPI뿐만 아니라 다양한 기업(카카오맵, 신한은행 등)의 API를 통해 데이터를 불러올 수 있음
* 무료, 유료 API 모두 존재
* 단순히 데이터 수집뿐 아니라 다양한 기능(지도, 결제, 로그인 등)을 제공

---

## 핵심 요약
* **OpenAPI**는 서버가 제공하는 자원/기능을 외부에서 호출할 수 있게 공개한 인터페이스로, 요청에 대해 HTML이 아닌 **XML/CSV/JSON** 등 정형화된 데이터를 응답합니다.
* HTTP `GET` 요청은 URL에 `?파라미터=값&파라미터2=값2` 형태로 정보를 담아 보내며, 파이썬의 `requests` 라이브러리로 손쉽게 호출할 수 있습니다.
* **공공데이터 포털**은 인증키(ServiceKey) 기반으로 정형/정적 데이터를 제공하고, **기업 OpenAPI**(네이버 등)는 OAuth 로그인 기반과 API Key 기반(비로그인)으로 나뉘며 실시간 서비스 데이터를 제공합니다.
* 응답으로 받은 XML/JSON은 `BeautifulSoup`, `pandas` 등으로 파싱·정제(중복 제거, 결측치 확인, 파생 컬럼 생성)해 분석 가능한 형태로 전처리합니다.
