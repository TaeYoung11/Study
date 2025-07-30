# Python Function 2 & Module 정리

**재귀 함수**, 유용한 **내장 함수**(map, zip), **함수 스타일 가이드**(이름 규칙, 단일 책임 원칙), **Packing & Unpacking**, 그리고 **모듈/패키지/파이썬 표준 라이브러리**를 정리한 문서입니다.

---

## 1. 재귀 함수

### 재귀 함수란?
* 함수 내부에서 자기 자신을 호출하는 함수

### 재귀 함수의 예 — 팩토리얼
* 어떤 양의 정수 n에 대해 1부터 n까지의 모든 양의 정수를 곱한 값

```
n! = n * (n-1)!
   = n * (n-1) * (n-2)!
   = ...
```

* `factorial` 함수는 자기 자신을 재귀적으로 호출하여 입력된 숫자 n의 팩토리얼을 계산
* 재귀 호출은 n이 0이 될 때까지 반복되며, 종료 조건을 설정하여 재귀 호출이 멈추도록 함
* 재귀 호출의 결과를 이용하여 문제를 작은 단위의 문제로 분할하고, 분할된 문제들의 결과를 조합하여 최종 결과를 도출

```python
def factorial(n):
    # 종료 조건: n이 0이면 1을 반환
    if n == 0:
        return 1
    else:
        # 재귀 호출: n과 n-1의 팩토리얼을 곱한 결과를 반환
        return n * factorial(n - 1)

# 팩토리얼 계산 예시
print(factorial(5))  # 120
```
* `5! = 5*4*3*2*1 = 120`
* 같은 문제를 다른 input을 통해서 해결(base case로 수렴): `f(4)=4*f(3)`, `f(3)=3*f(2)`, `f(2)=2*f(1)`, `f(1)=1`

### 재귀 함수 특징
* 특정 알고리즘 식을 표현할 때 변수의 사용이 줄어들며, 코드의 가독성이 높아짐
* 1개 이상의 base case(종료되는 상황)가 존재하고, 수렴하도록 작성

### 재귀 함수를 사용하는 이유
* **문제의 자연스러운 표현:** 복잡한 문제를 간결하고 직관적으로 표현 가능
* **코드 간결성:** 상황에 따라 반복문보다 알고리즘 코드가 더 간결하고 명확해질 수 있음
* **수학적 문제 해결:** 수학적 정의가 재귀적으로 표현되는 경우, 직접적인 구현이 가능

### 재귀 함수 활용 시 기억해야 할 것
1. 종료 조건을 명확히
2. 반복되는 호출이 종료 조건을 향하도록 하기

---

## 2. 내장 함수(Built-in function)

### 내장 함수란?
* 파이썬이 기본적으로 제공하는 함수(별도의 import 없이 바로 사용 가능)
* 자주 사용되는 내장 함수 예시

```python
numbers = [1, 2, 3, 4, 5]

print(len(numbers))                 # 5
print(max(numbers))                 # 5
print(min(numbers))                 # 1
print(sum(numbers))                 # 15
print(sorted(numbers, reverse=True))# [5, 4, 3, 2, 1]
```

### 유용한 내장 함수 — map & zip

### map(function, iterable)
* 순회 가능한 데이터구조(iterable)의 모든 요소에 함수를 적용하고, 그 결과를 map object로 반환

```python
numbers = [1, 2, 3]
result = map(str, numbers)

print(result)         # <map object at 0x00000239C915D760>
print(list(result))   # ['1', '2', '3']
```

* SWEA 문제의 input처럼 문자열 '1 2 3'이 입력 되었을 때 활용 예시

```python
numbers1 = input().split()
print(numbers1)  # ['1', '2', '3']

numbers2 = list(map(int, input().split()))
print(numbers2)  # [1, 2, 3]
```

### zip(*iterables)
* 임의의 iterable을 모아 튜플을 원소로 하는 zip object를 반환

```python
a_students = ['jane', 'ashley']
b_students = ['peter', 'jay']
pair = zip(a_students, b_students)

print(pair)         # <zip object at 0x000001C760E58700>
print(list(pair))   # [('jane', 'peter'), ('ashley', 'jay')]
```

* 여러 개의 리스트를 동시에 조회할 때

```python
kr_scores = [10, 20, 30, 50]
math_scores = [20, 40, 50, 70]
en_scores = [40, 20, 30, 50]

for student_scores in zip(kr_scores, math_scores, en_scores):
    print(student_scores)
# (10, 20, 40)
# (20, 40, 20)
# (30, 50, 30)
# (50, 70, 50)
```

* 2차원 리스트의 같은 컬럼(열) 요소를 동시에 조회할 때

```python
scores = [
    [10, 20, 30],
    [40, 50, 39],
    [20, 40, 50],
]

for score in zip(*scores):
    print(score)
# (10, 40, 20)
# (20, 50, 40)
# (30, 39, 50)
```

---

## 3. 함수 스타일 가이드

### 기본 규칙
* 소문자와 언더스코어(`_`) 사용
* 동사로 시작하여 함수의 동작 설명
* 약어 사용 지양

```python
# Good
def calculate_total_price(price, tax):
    return price + (price * tax)

# Bad
def calc_price(p, t):
    return p + (p * t)
```

### 함수 이름 구성 요소
* **동사 + 명사:** `save_user()`
* **동사 + 형용사 + 명사:** `calculate_total_price()`
* **get/set 접두사:** `get_username()`, `set_username()`

### 단일 책임 원칙(Single Responsibility Principle)
* 모든 객체는 하나의 명확한 목적과 책임만을 가져야 함

**잘못된 설계 예시 — 여러 책임이 섞인 함수**
```python
def process_user_data(user_data):
    # 책임 1: 데이터 유효성 검사
    if len(user_data['password']) < 8:
        raise ValueError('비밀번호는 8자 이상이어야 합니다')

    # 책임 2: 비밀번호 암호화 및 저장
    user_data['password'] = hash_password(user_data['password'])
    db.users.insert(user_data)

    # 책임 3: 이메일 발송
    send_email(user_data['email'], '가입을 환영합니다!')
```

**올바른 설계 예시 — 책임을 분리한 함수들**
```python
def validate_password(password):
    """비밀번호 유효성 검사"""
    if len(password) < 8:
        raise ValueError('비밀번호는 8자 이상이어야 합니다')

def save_user(user_data):
    """비밀번호 암호화 및 저장"""
    user_data['password'] = hash_password(user_data['password'])
    db.users.insert(user_data)

def send_welcome_email(email):
    """환영 이메일 발송"""
    send_email(email, '가입을 환영합니다!')

# 메인 함수에서 순차적으로 실행
def process_user_data(user_data):
    validate_password(user_data['password'])
    save_user(user_data)
    send_welcome_email(user_data['email'])
```

### 함수 설계 원칙
1. **명확한 목적:** 함수는 한 가지 작업만 수행. 함수 이름으로 목적을 명확히 표현
2. **책임 분리:** 데이터 검증, 처리, 저장 등을 별도 함수로 분리. 각 함수는 독립적으로 동작 가능하도록 설계
3. **유지보수성:** 작은 단위의 함수로 나누어 관리. 코드 수정 시 영향 범위를 최소화

---

## 4. Packing & Unpacking

### 패킹(Packing)
* 여러 개의 값을 하나의 변수에 묶어서 담는 것
* 변수에 담긴 값들은 튜플(tuple) 형태로 묶임

```python
packed_values = 1, 2, 3, 4, 5
print(packed_values)  # (1, 2, 3, 4, 5)
```

### `*`을 활용한 패킹
* `*b`는 남은 요소들을 리스트로 패킹하여 할당

```python
numbers = [1, 2, 3, 4, 5]
a, *b, c = numbers

print(a)  # 1
print(b)  # [2, 3, 4]
print(c)  # 5
```

* print 함수에서 임의의 가변 인자를 작성할 수 있었던 이유 => 인자 개수에 상관 없이 튜플 하나로 패킹 되어서 내부에서 처리

```python
def my_func(*objects):
    print(objects)         # (1, 2, 3, 4, 5)
    print(type(objects))   # <class 'tuple'>

my_func(1, 2, 3, 4, 5)
```
* `print(*objects, sep=' ', end='\n', file=sys.stdout, flush=False)` — `objects`를 텍스트 스트림 `file`로 인쇄하는데, `sep`로 구분되고 `end`가 뒤에 붙임. 암단, `sep`, `end`, `file` 및 `flush`는 반드시 키워드 인자로 지정해야 함. 모든 비 키워드 인자는 `str()`이 하듯이 문자열로 변환 후 스트림에 쓰이는데, `sep`로 구분되고 `end`가 뒤에 붙음

### 언패킹(UnPacking)
* 패킹된 변수의 값을 개별적인 변수로 분리하여 할당하는 것

```python
packed_values = 1, 2, 3, 4, 5
a, b, c, d, e = packed_values

print(a, b, c, d, e)  # 1 2 3 4 5
```

### "*"을 활용한 언패킹
* `*`는 리스트의 요소를 언패킹하여 인자로 전달

```python
def my_function(x, y, z):
    print(x, y, z)

names = ['alice', 'jane', 'peter']
my_function(*names)  # alice jane peter
```

### "**"을 활용한 언패킹
* `**`는 딕셔너리의 키-값 쌍을 언패킹하여 함수의 키워드 인자로 전달

```python
def my_function(x, y, z):
    print(x, y, z)

my_dict = {'x': 1, 'y': 2, 'z': 3}
my_function(**my_dict)  # 1 2 3
```

### "*", "**" 패킹/언패킹 연산자 정리
* **`*`:** 패킹 연산자로 사용될 때 여러 개의 인자를 하나의 튜플로 묶음. 언패킹 연산자로 사용될 때 시퀀스나 반복 가능한 객체를 각각의 요소로 언패킹하여 함수의 인자로 전달
* **`**`:** 언패킹 연산자로 사용될 때 딕셔너리의 키-값 쌍을 언패킹하여 함수의 키워드 인자로 전달

---

## 5. 모듈(Module)

* 과학자, 수학자가 모든 이론을 새로 만들거나 증명하지 않는 것처럼 개발자 또한 프로그램 전체를 모두 혼자 힘으로 작성하는 것은 드문 일
* 다른 프로그래머가 이미 작성해 놓은 수천, 수백만 줄의 코드를 활용하는 것은 생산성에서 매우 중요한 일

### 모듈이란?
* 한 파일로 묶인 변수와 함수의 모음
* 특정한 기능을 하는 코드가 작성된 파이썬 파일(`.py`)

### 모듈 예시 — math 내장 모듈
* 파이썬이 미리 작성해둔 수학 관련 변수와 함수가 작성된 모듈

```python
import math

print(math.pi)         # 3.141592653589793
print(math.sqrt(4))    # 2.0
```
참고: `https://docs.python.org/3/library/math.html`

### 모듈을 가져오는 방법
**import 문 사용**
```python
import math
print(math.sqrt(4))
```

**from 절 사용**
```python
from math import sqrt
print(sqrt(4))
```

### 모듈 사용하기 — '.'(dot) 연산자
* "점의 왼쪽 객체에서 점의 오른쪽 이름을 찾아라"라는 의미

```python
# 모듈명.변수명
print(math.pi)

# 모듈명.함수명
print(math.sqrt(4))
```

### 모듈 주의사항
* 서로 다른 모듈이 같은 이름의 함수를 제공할 경우 문제 발생
* 마지막에 import된 이름으로 대체됨

```python
from math import pi, sqrt
from my_math import sqrt

# 모듈 내 모든 요소를 한번에 import 하는 * 표기는 권장하지 않음
from math import *
```

### 'as' 키워드
* `as` 키워드를 사용하여 별칭(alias)을 부여
* 두 개 이상의 모듈에서 동일한 이름의 변수, 함수 클래스 등을 가져올 때 발생하는 이름 충돌 해결

```python
from math import sqrt
from my_math import sqrt as my_sqrt

sqrt(4)
my_sqrt(4)
```

### 사용자 정의 모듈
1. 모듈 `my_math.py` 작성
2. 두 수의 합을 구하는 `add` 함수 작성
3. `my_math` 모듈 import 후 `add` 함수 호출

```python
# my_math.py
def add(x, y):
    return x + y
```
```python
# sample.py
import my_math
print(my_math.add(1, 2))  # 3
```

---

## 6. 파이썬 표준 라이브러리

### 파이썬 표준 라이브러리(Python Standard Library, PSL)
* 파이썬 언어와 함께 제공되는 다양한 모듈과 패키지의 모음

### 패키지(Package)
* 연관된 모듈들을 하나의 디렉토리에 모아 놓은 것

### 패키지 사용하기
* 아래와 같은 디렉토리 구조로 작성
* 패키지 3개: `my_package`, `math`, `statistics`
* 모듈 2개: `my_math`, `tools`

```
sample.py
my_package/
  math/
    my_math.py
  statistics/
    tools.py
```

```python
# my_package/math/my_math.py
def add(x, y):
    return x + y
```
```python
# my_package/statistics/tools.py
def mod(x, y):
    return x % y
```
```python
# sample.py
from my_package.math import my_math
from my_package.statistics import tools

print(my_math.add(1, 2))  # 3
print(tools.mod(1, 2))    # 1
```

### 파이썬 표준 라이브러리(PSL) 내부 패키지
* 설치 없이 바로 `import` 하여 사용

### 외부 패키지
* `pip`를 사용하여 설치 후 import 필요

### 파이썬 패키지 관리자(Package Installer for Python, pip)
* 외부 패키지들을 설치하도록 도와주는 파이썬의 패키지 관리 시스템
* PyPI(Python Package Index)에 저장된 외부 패키지들을 설치

### 패키지 설치
* 최신 버전 / 특정 버전 / 최소 버전을 명시하여 설치할 수 있음

```bash
$ pip install SomePackage
$ pip install SomePackage==1.0.5
$ pip install SomePackage>=1.0.4
```

### request 외부 패키지 설치 및 사용 예시
```bash
$ pip install requests
```
```python
import requests

url = 'https://random-data-api.com/api/v2/users'
response = requests.get(url).json()

print(response)
```

### 패키지 사용 목적
* 모듈들의 이름 공간을 구분하여 충돌을 방지
* 모듈들을 효율적으로 관리하고 재사용할 수 있도록 돕는 역할

### 패키지 사용 시 주의사항
* 버전 충돌
* 라이선스 확인
* 보안

---

## 7. 참고

### 람다 표현식(Lambda Expressions)
* 익명 함수를 만드는 데 사용되는 표현식 => 한 줄로 간단한 함수를 정의
* **구조:** `lambda 매개변수: 표현식`
  * `lambda` 키워드: 람다 함수를 선언하기 위해 사용되는 키워드
  * 매개변수: 함수에 전달되는 매개변수들. 여러 개의 매개변수가 있을 경우 쉼표로 구분
  * 표현식: 함수의 실행되는 코드 블록으로, 결과값을 반환하는 표현식으로 작성

### 람다 표현식 예시
```python
def addition(x, y):
    return x + y

# 위 함수와 동일한 람다 표현식
addition = lambda x, y: x + y

result = addition(3, 5)
print(result)  # 8
```
* 간단한 연산이나 함수를 한 줄로 표현할 때 사용
* 함수를 매개변수로 전달하는 경우에도 유용하게 활용

### 람다 표현식 활용(with map 함수)
```python
numbers = [1, 2, 3, 4, 5]

def square(x):
    return x**2

# lambda 미사용
squared1 = list(map(square, numbers))
print(squared1)  # [1, 4, 9, 16, 25]

# lambda 사용
squared2 = list(map(lambda x: x**2, numbers))
print(squared2)  # [1, 4, 9, 16, 25]
```

### 모듈 내부 살펴보기
* 내장 함수 `help`를 사용해 모듈에 무엇이 들어있는 지 확인 가능

```python
help(math)
"""
NAME
    math
DESCRIPTION
    This module provides access to the mathematical functions
    defined by the C standard.
FUNCTIONS
    acos(x, /)
        Return the arc cosine (measured in radians) of x.
        The result is between 0 and pi
...
"""
```

---

## 핵심 요약
* **재귀 함수**는 자기 자신을 호출하는 함수로, 명확한 종료 조건(base case)이 있어야 하며 수학적으로 정의된 문제를 간결하게 표현할 때 유용하다.
* **map(function, iterable)**은 모든 요소에 함수를 적용하고, **zip(*iterables)**은 여러 iterable을 튜플로 묶어주며, 둘 다 반환값은 `list()`로 변환해야 실제 값을 확인할 수 있다.
* 함수는 동사로 시작하는 이름과 **단일 책임 원칙**(하나의 함수는 하나의 책임만)을 지켜 설계해야 유지보수성이 높아진다.
* **Packing**은 여러 값을 튜플로 묶는 것(`*b`로 나머지 요소 패킹 가능), **Unpacking**은 반대로 컬렉션을 개별 변수/인자로 분리하는 것이며, 함수 호출 시 `*리스트`(위치 인자로 언패킹), `**딕셔너리`(키워드 인자로 언패킹)로 활용한다.
* **모듈**은 변수/함수를 묶은 `.py` 파일이고, **패키지**는 관련 모듈들을 모은 디렉토리이며, `import`/`from ... import`/`as`(별칭)로 가져와 사용한다. 표준 라이브러리는 설치 없이, 외부 패키지는 `pip install`로 설치 후 사용한다.
* **람다 표현식**(`lambda 매개변수: 표현식`)은 간단한 함수를 한 줄로 정의할 때, 특히 `map`처럼 함수를 인자로 전달하는 상황에서 유용하다.
