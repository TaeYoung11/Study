# Python Controll Flow & Function 1 정리

파이썬의 **제어문**(조건문, 반복문, 반복 제어)과 **함수**(정의, 호출, 매개변수/인자, Scope), 그리고 **List Comprehension**을 정리한 문서입니다.

---

## 1. 제어문(Control Statement)

### 제어문이란?
* 코드의 실행 흐름을 제어하는 데 사용되는 구문
* 조건에 따라 코드 블록을 실행하거나 반복적으로 코드를 실행
* **제어문 종류:** 조건문(`if`, `elif`, `else`), 반복문(`for`, `while`), 반복문 제어(`break`, `continue`, `pass`)

---

## 2. 조건문(Conditional Statement)

### 조건문이란?
* 주어진 조건식을 평가하여 해당 조건이 참(True)인 경우에만 코드 블록을 실행하거나 건너뜀
* 파이썬 조건문에 사용되는 키워드: `if`, `elif`, `else`

### "if" statement
* `if`(주어진 조건이 참인지 확인하여, 조건을 만족하면 해당 블록을 실행)
* `elif`(앞선 조건들이 거짓일 때, 새로운 조건이 참인지 확인하여 실행)
* `else`(앞선 모든 조건이 거짓일 때 실행)

```python
if 표현식:
    코드 블록
elif 표현식:
    코드 블록
else:
    코드 블록
```

### if 조건문 예시
```python
a = 5
if a > 3:
    print('3 초과')
else:
    print('3 이하')
print(a)
```
```python
a = 3
if a > 3:
    print('3 초과')
else:
    print('3 이하')
print(a)
```

### 복수 조건문
* 조건식을 동시에 검사하는 것이 아니라 **"순차적"**으로 비교

```python
dust = 35
if dust > 150:
    print('매우 나쁨')
elif dust > 80:
    print('나쁨')
elif dust > 30:
    print('보통')
else:
    print('좋음')
```

### 중첩 조건문
```python
dust = 480
if dust > 150:
    print('매우 나쁨')
    if dust > 300:
        print('위험해요! 나가지 마세요!')
elif dust > 80:
    print('나쁨')
elif dust > 30:
    print('보통')
else:
    print('좋음')
```

---

## 3. 반복문(Loop Statement)

### 반복문이란?
* 주어진 코드 블록을 여러 번 반복해서 실행하는 구문
* 파이썬 반복문에 사용되는 키워드: `for`(특정 작업을 반복적으로 수행), `while`(주어진 조건이 참인 동안 반복해서 수행)

### "for" statement
* 임의의 시퀀스의 항목들을 그 시퀀스에 들어있는 순서대로 반복
* 특정 작업을 반복적으로 수행

```python
for 변수 in 반복 가능한 객체:
    코드 블록
```
* **반복 가능한 객체(iterable):** 반복문에서 순회할 수 있는 객체. 시퀀스 객체 뿐만 아니라 dict, set 등도 포함

### for 문 작동원리
* 리스트 내 첫 항목이 반복 변수에 할당되고 코드블록이 실행
* 다음으로 반복 변수에 리스트의 2번째 항목이 할당되고 코드블록이 다시 실행
* ... 마지막으로 반복 변수에 리스트의 마지막 요소가 할당되고 코드블록이 실행

```python
items = ['apple', 'banana', 'coconut']

for item in items:
    print(item)
# apple
# banana
# coconut
```

### 다양한 순회
**문자열 순회**
```python
country = 'Korea'
for char in country:
    print(char)
# K o r e a (한 줄씩)
```

**range 순회**
```python
for i in range(5):
    print(i)
# 0 1 2 3 4
```

**딕셔너리 순회**
```python
my_dict = {'x': 10, 'y': 20, 'z': 30}
for key in my_dict:
    print(key)
    print(my_dict[key])
# x 10 / y 20 / z 30
```

### 인덱스로 리스트 순회
* 리스트의 요소가 아닌 인덱스로 접근하여 해당 요소들을 변경하기

```python
numbers = [4, 6, 10, -8, 5]
for i in range(len(numbers)):
    numbers[i] = numbers[i] * 2

print(numbers)  # [8, 12, 20, -16, 10]
```

### 중첩된 반복문
* 안쪽 반복문은 outers 리스트의 각 항목에 대해 한 번씩 실행됨
* print가 호출되는 횟수 => `len(outers) * len(inners)`

```python
outers = ['A', 'B']
inners = ['c', 'd']

for outer in outers:
    for inner in inners:
        print(outer, inner)
# A c / A d / B c / B d
```

### 중첩 리스트 순회
* 안쪽 리스트 요소에 접근하려면 바깥 리스트를 순회하면서 중첩 반복을 사용해 각 안 반복을 순회

```python
elements = [['A', 'B'], ['c', 'd']]
for elem in elements:
    for item in elem:
        print(item)
# A B c d
```

### "while" statement
* 주어진 조건식이 참(True)인 동안 코드를 반복해서 실행
* 조건식이 거짓(False)가 될 때 까지 반복

```python
while 조건식:
    코드 블록
```
=> 반드시 **"종료 조건"**이 필요

### while 문 예시
```python
a = 0
while a < 3:
    print(a)
    a += 1
print('끝')
# 0 1 2 끝
```

### 사용자 입력에 따른 반복
* `while` 문을 사용한 특정 입력 값에 대한 종료 조건 활용하기 => 반드시 **"종료 조건"**이 필요

```python
number = int(input('양의 정수를 입력해주세요.: '))
while number <= 0:
    if number < 0:
        print('음수를 입력했습니다.')
    else:
        print('0은 양의 정수가 아닙니다.')
    number = int(input('양의 정수를 입력해주세요.: '))
print('잘했습니다!')
```

### 적절한 반복문 활용하기
* **for:** 반복 횟수가 명확하게 정해져 있는 경우에 유용. 예를 들어 리스트, 튜플, 문자열 등과 같은 시퀀스 형식의 데이터를 처리할 때
* **while:** 반복 횟수가 불명확하거나 조건에 따라 반복을 종료해야 할 유용. 예를 들어 사용자의 입력을 받아서 특정 조건이 충족될 때까지 반복하는 경우

---

## 4. 반복 제어

### 반복 제어란?
* `for`문과 `while`은 매 반복마다 본문 내 모든 코드를 실행하지만 때때로 일부만 실행하는 것이 필요할 때가 있음
* 파이썬 반복 제어에 사용되는 키워드: `break`, `continue`, `pass`

### 반복문 제어 예시
* `break`(반복을 즉시 중지), `continue`(다음 반복으로 건너뜀), `pass`(아무런 동작도 수행하지 않고 넘어감)

```python
# break
for i in range(10):
    if i == 5:
        break
    print(i)  # 0 1 2 3 4

# continue
for i in range(10):
    if i % 2 == 0:
        continue
    print(i)  # 1 3 5 7 9

# pass
for i in range(10):
    pass  # 아무 작업도 안함
```

### break 예시
```python
number = int(input('양의 정수를 입력해주세요.: '))
while number <= 0:
    if number == -9999:
        print('프로그램을 종료합니다.')
        break
    if number < 0:
        print('음수를 입력했습니다.')
    else:
        print('0은 양의 정수가 아닙니다.')
    number = int(input('양의 정수를 입력해주세요.: '))
print('잘했습니다!')
```

**리스트에서 첫 번째 짝수만 찾은 후 반복 종료하기**
```python
numbers = [1, 3, 5, 6, 7, 9, 10, 11]
found_even = False

for num in numbers:
    if num % 2 == 0:
        print('첫 번째 짝수를 찾았습니다:', num)
        found_even = True
        break

if not found_even:
    print('짝수를 찾지 못했습니다')
```

### continue 예시 — 리스트에서 홀수만 출력하기
* 현재 반복문의 남은 코드를 건너뛰고 다음 반복으로 넘어감

```python
numbers = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]

for num in numbers:
    if num % 2 == 0:
        continue
    print(num)
# 1 3 5 7 9
```

### pass 예시
* 조건문에서 아무런 동작을 수행하지 않아야 할 때

```python
if condition:
    pass  # 아무런 동작도 수행하지 않음
else:
    # 다른 동작 수행
```

* 무한 루프에서 조건이 충족되지 않을 때 pass를 사용하여 루프를 계속 진행하는 방법

```python
while True:
    if condition:
        break
    elif condition:
        pass  # 루프 계속 진행
    else:
        print('..')
```

* 코드 작성 중 미완성 부분 — 구현해야 할 부분이 나중에 추가될 수 있고, 코드를 컴파일하는 동안 오류가 발생하지 않음

```python
def my_function():
    pass
```

---

## 5. 함수(Functions)

### 함수란?
* 특정 작업을 수행하기 위한 재사용 가능한 코드 묶음
* **함수를 사용하는 이유:** 두 수의 합을 구하는 함수를 정의하고 사용함으로써 코드의 중복을 방지. 재사용성이 높아지고, 코드의 가독성과 유지보수성 향상

### 함수 호출(Function Call)
* 함수를 실행하기 위해 함수의 이름을 사용하여 해당 함수의 코드 블록을 실행하는 것

```python
function_name(arguments)
```

```python
# 두 수의 합을 구하는 코드
num1 = 5
num2 = 3
sum_result = num1 + num2
print(sum_result)
```
```python
# 두 수의 합을 구하는 함수
def get_sum(num1, num2):
    return num1 + num2

# 함수를 호출하여 결과 출력
num1 = 5
num2 = 3
sum_result = get_sum(num1, num2)
print(sum_result)
```

### 함수 구조
```python
def make_sum(pram1, pram2):
    """이것은 두 수를 받아
    두 수의 합을 반환하는 함수입니다.
    >>> make_sum(1, 2)
    3
    """
    return pram1 + pram2
```
* `parameter`(INPUT x): `make_sum(pram1, pram2)`의 `pram1`, `pram2`
* `Docstring`: 함수 body 앞에 선택적으로 작성 가능한 함수 설명서
* `function body`: 함수가 실행될 때 수행되는 코드를 정의
* `return value`(OUTPUT f(x)): `return pram1 + pram2`가 반환하는 값

### 함수 정의와 호출
1. **함수 정의:** `def` 키워드로 시작. `def` 키워드 이후 함수 이름 작성. 괄호안에 매개변수를 정의할 수 있음. 매개변수(parameter)는 함수에 전달되는 값을 나타냄
2. **함수 body:** 콜론(`:`) 다음에 들여쓰기 된 코드블록. 함수가 실행될 때 수행되는 코드를 정의
3. **Docstring:** 함수 body 앞에 선택적으로 작성 가능한 함수 설명서
4. **함수 반환 값:** 함수는 필요한 경우 결과를 반환할 수 있음. `return` 키워드 이후에 반환할 값을 명시. `return` 문은 함수의 실행을 종료하고, 결과를 호출 부분으로 반환
5. **함수 호출:** 함수를 사용하기 위해서는 호출이 필요. 함수의 이름과 소괄호를 활용해 호출. 필요한 경우 인자(argument)를 전달해야 함. 호출 부분에서 전달된 인자는 함수 정의 시 지정한 매개변수에 대입됨

```python
result = make_sum(100, 30)
print(result)  # 130
```

---

## 6. 매개변수와 인자

### 매개변수(parameter)
* 함수를 정의할 때, 함수가 받을 값을 나타내는 변수

### 인자(argument)
* 함수를 호출할 때, 실제로 전달되는 값

```python
def add_numbers(x, y):  # x와 y는 매개변수(parameter)
    result = x + y
    return result

a = 2
b = 3
sum_result = add_numbers(a, b)  # a와 b는 인자(argument)
print(sum_result)
```

### 다양한 인자 종류
위치 인자, 기본 인자 값, 키워드 인자, 임의의 인자 목록, 임의의 키워드 인자 목록

### 위치 인자(Positional Arguments)
* 함수 호출 시 인자의 위치에 따라 전달되는 인자
* 위치인자는 함수 호출 시 반드시 값을 전달해야 함

```python
def greet(name, age):
    print(f'안녕하세요, {name}님! {age}살이시군요.')

greet('Alice', 25)  # 안녕하세요, Alice님! 25살이시군요.
greet(25, 'Alice')  # 안녕하세요, 25님! Alice살이시군요. (위치가 바뀌면 의미도 바뀜)
greet('Alice')      # TypeError: greet() missing 1 required positional argument: 'age'
```

### 기본 인자 값(Default Argument Values)
* 함수 정의에서 매개변수에 기본 값을 할당하는 것
* 함수 호출 시 인자를 전달하지 않으면, 기본값이 매개변수에 할당됨

```python
def greet(name, age=30):
    print(f'안녕하세요, {name}님! {age}살이시군요.')

greet('Bob')          # 안녕하세요, Bob님! 30살이시군요.
greet('Charlie', 40)  # 안녕하세요, Charlie님! 40살이시군요.
```

### 키워드 인자(Keyword Arguments)
* 함수 호출 시 인자의 이름과 함께 값을 전달하는 인자
* 매개변수와 인자를 일치시키지 않고, 특정 매개변수에 값을 할당할 수 있음
* 인자의 순서는 중요하지 않으며, 인자의 이름을 명시하여 전달
* 단, 호출 시 키워드 인자는 위치 인자 뒤에 위치해야 함

```python
def greet(name, age):
    print(f'안녕하세요, {name}님! {age}살이시군요.')

greet(name='Dave', age=35)  # 안녕하세요, Dave님! 35살이시군요.
greet(age=35, name='Dave')  # 안녕하세요, Dave님! 35살이시군요.
greet(age=35, 'Dave')       # positional argument follows keyword argument (에러)
```

### 임의의 인자 목록(Arbitrary Argument Lists)
* 정해지지 않은 개수의 인자를 처리하는 인자
* 함수 정의 시 매개변수 앞에 `*`를 붙여 사용
* 여러 개의 인자를 tuple로 처리

```python
def calculate_sum(*args):
    print(args)         # (1, 100, 5000, 30)
    print(type(args))   # <class 'tuple'>

calculate_sum(1, 100, 5000, 30)
```

### 임의의 키워드 인자 목록(Arbitrary Keyword Argument Lists)
* 정해지지 않은 개수의 키워드 인자를 처리하는 인자
* 함수 정의 시 매개변수 앞에 `**`를 붙여 사용
* 여러 개의 인자를 dictionary로 묶어 처리

```python
def print_info(**kwargs):
    print(kwargs)

print_info(name='Eve', age=30)  # {'name': 'Eve', 'age': 30}
```

### 함수 인자 권장 작성순서
* 위치 → 기본 → 가변 → 가변 키워드
* 호출 시 인자를 전달하는 과정에서 혼란을 줄일 수 있도록 함
* 단, 모든 상황에 적용되는 절대적인 규칙은 아니며, 상황에 따라 유연하게 조정될 수 있음

```python
def func(pos1, pos2, default_arg='default', *args, **kwargs):
    ...
```

### 인자의 모든 종류를 적용한 예시
```python
def func(pos1, pos2, default_arg='default', *args, **kwargs):
    print('pos1:', pos1)
    print('pos2:', pos2)
    print('default_arg:', default_arg)
    print('args:', args)
    print('kwargs:', kwargs)

func(1, 2, 3, 4, 5, 6, key1='value1', key2='value2')
"""
pos1: 1
pos2: 2
default_arg: 3
args: (4, 5, 6)
kwargs: {'key1': 'value1', 'key2': 'value2'}
"""
```

---

## 7. 함수와 Scope

### Python의 범위(scope)
* 함수는 코드 내부에 local scope를 생성하며, 그 외의 공간인 global scope로 구분

### 범위와 변수 관계
* **scope:** global scope(코드 어디에서든 참조할 수 있는 공간), local scope(함수가 만든 scope, 함수 내부에서만 참조 가능)
* **variable:** global variable(global scope에 정의된 변수), local variable(local scope에 정의된 변수)

### Scope 예시
* `num`은 local scope에 존재하기 때문에 global scope에서 사용할 수 없음 => 이는 변수의 수명주기와 연관이 있음

```python
def func():
    num = 20
    print('local', num)  # local 20

func()
print('global', num)  # NameError: name 'num' is not defined
```

### 변수 수명주기(lifecycle)
변수의 수명주기는 변수가 선언되는 위치와 scope에 따라 결정됨
1. **built-in scope:** 파이썬이 실행된 이후부터 영원히 유지
2. **global scope:** 모듈이 호출된 시점 이후 혹은 인터프리터가 끝날 때까지 유지
3. **local scope:** 함수가 호출될 때 생성되고, 함수가 종료될 때까지 유지

### 이름 검색 규칙(Name Resolution)
* 파이썬에서 사용되는 이름(식별자)들은 특정한 이름공간(namespace)에 저장되어 있음
* 아래와 같은 순서로 이름을 찾아나가며, **LEGB Rule**이라고 부름
  1. **Local scope:** 지역 범위(현재 작업 중인 범위)
  2. **Enclosed scope:** 지역 범위 한 단계 위 범위
  3. **Global scope:** 최상단에 위치한 범위
  4. **Built-in scope:** 모든 것을 담고 있는 범위(정의하지 않고 사용할 수 있는 모든 것)

=> 함수 내에서는 바깥 Scope의 변수에 접근 가능하나 수정은 할 수 없음

### LEGB Rule 예시
* `sum`이라는 이름을 global scope에서 사용하게 되면서 기존에 built-in scope에 있던 내장함수 `sum`을 사용하지 못하게 됨
* `sum`을 참조 시 LEGB Rule에 따라 global에서 먼저 찾기 때문

```python
print(sum)              # <built-in function sum>
print(sum(range(3)))    # 3

sum = 5
print(sum)               # 5
print(sum(range(3)))     # TypeError: 'int' object is not callable
# sum 변수 객체 삭제를 위해 del sum을 입력 후 진행
```

### LEGB Rule 퀴즈
```python
a = 1
b = 2

def enclosed():
    a = 10
    c = 3

    def local(c):
        print(a, b, c)  # 10 2 500

    local(500)
    print(a, b, c)  # 10 2 3

enclosed()
print(a, b)  # 1 2
```
* `local(c)` 함수 내부의 `print(a, b, c)`에서 `a`는 enclosed scope, `b`는 global scope, `c`는 local scope(매개변수)에서 각각 참조됨. `local(500)` 호출 시 `c`는 500이 되지만, `enclosed()`의 `c`(3)에는 영향을 주지 않음

### global keyword
* 변수의 스코프를 전역 범위로 지정하기 위해 사용
* 일반적으로 함수 내에서 전역 변수를 수정하려는 경우에 사용

```python
num = 0  # 전역 변수

def increment():
    global num  # num을 전역 변수로 선언
    num += 1

print(num)  # 0
increment()
print(num)  # 1
```

### global keyword 주의사항
* **global 키워드 선언 전에 참조 불가**

```python
num = 0
def increment():
    # SyntaxError: name 'num' is used prior to global declaration
    print(num)
    global num
    num += 1
```

* **매개변수에는 global 키워드 사용 불가**

```python
num = 0
def increment(num):
    # "num" is assigned before global declaration
    global num
    num += 1
```

---

## 8. List Comprehension

### List Comprehension이란?
* 간결하고 효율적인 리스트 생성 방법

```python
[expression for 변수 in iterable]
list(expression for 변수 in iterable)

[expression for 변수 in iterable if 조건식]
list(expression for 변수 in iterable if 조건식)
```

### List Comprehension 사용 전/후 비교
**사용 전**
```python
numbers = [1, 2, 3, 4, 5]
squared_numbers = []
for num in numbers:
    squared_numbers.append(num**2)
print(squared_numbers)  # [1, 4, 9, 16, 25]
```

**사용 후**
```python
numbers = [1, 2, 3, 4, 5]
squared_numbers = [num**2 for num in numbers]
print(squared_numbers)  # [1, 4, 9, 16, 25]
```

### List Comprehension 활용 예시 — 2차원 리스트 생성 시(인접행렬 생성 시)
```python
data1 = [[0] * 5 for _ in range(5)]
# 또는
data2 = [[0 for _ in range(5)] for _ in range(5)]

# [[0, 0, 0, 0, 0],
#  [0, 0, 0, 0, 0],
#  [0, 0, 0, 0, 0],
#  [0, 0, 0, 0, 0],
#  [0, 0, 0, 0, 0]]
```

### 어떤 코드가 가독성이 좋아 보이나요?
```python
result = [i for i in range(10) if i % 2 == 1]
```
```python
result = []
for i in range(10):
    if i % 2 == 1:
        result.append(i)
```
* 짧고 간단한 로직은 List Comprehension이 더 가독성이 좋을 수 있으나, 로직이 복잡해질수록 일반 for문이 더 읽기 쉬울 수 있음 — 상황에 맞게 선택

---

## 9. 참고 — enumerate

### enumerate(iterable, start=0)
* iterable 객체의 각 요소에 대해 인덱스와 함께 반환하는 내장함수

```python
fruits = ['apple', 'banana', 'cherry']

for index, fruit in enumerate(fruits):
    print(f'인덱스 {index}: {fruit}')
"""
인덱스 0: apple
인덱스 1: banana
인덱스 2: cherry
"""
```

---

## 핵심 요약
* **조건문**(`if`/`elif`/`else`)은 조건식을 순차적으로 평가하며, **반복문**은 `for`(반복 가능한 객체를 순회)와 `while`(조건이 참인 동안 반복, 종료 조건 필수)로 나뉜다.
* 반복 제어 키워드는 `break`(즉시 중지), `continue`(다음 반복으로 건너뜀), `pass`(아무 동작 없이 통과, 미완성 코드의 자리표시자)로 구분된다.
* **함수**는 `def`로 정의하며 매개변수(parameter, 정의 시점의 변수)와 인자(argument, 호출 시점의 값)를 구분해야 하고, 인자에는 위치 인자·기본 인자 값·키워드 인자·`*args`(임의 인자, 튜플)·`**kwargs`(임의 키워드 인자, 딕셔너리)가 있으며 권장 작성 순서는 위치→기본→가변→가변 키워드다.
* **Scope**는 `local → enclosed → global → built-in` 순서로 이름을 찾는 **LEGB Rule**을 따르며, 함수 내부에서 바깥 scope 변수를 참조는 가능하나 수정하려면 `global` 키워드를 (사용 전에 참조하지 않고, 매개변수가 아닌 변수에 대해) 선언해야 한다.
* **List Comprehension**(`[expression for 변수 in iterable if 조건식]`)은 짧고 간단한 로직에서 가독성 있게 리스트를 생성하는 방법이며, `enumerate()`로 반복 시 인덱스와 값을 동시에 얻을 수 있다.
