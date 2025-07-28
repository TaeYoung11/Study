# Python Basic Syntax 2 정리

파이썬의 나머지 **Sequence Types**(list, tuple, range), **Non-sequence Types**(dict, set), **Other Types**(None, Boolean), **Collection** 정리, **Type Conversion**(형변환), 그리고 다양한 **연산자**를 정리한 문서입니다.

---

## 1. Data Types 복습

* **데이터 타입:** 값의 종류와 그 값에 적용 가능한 연산과 동작을 결정하는 속성
* Numeric Type: int, float, complex / Sequence Types: list, tuple, range / Non-sequence Types: set, dict / Text Sequence Type: str / 기타: Boolean, None, Functions
* **데이터 타입이 필요한 이유:** 값들을 구분하고, 어떻게 다뤄야 하는지를 알 수 있음. 요리 재료마다 특정한 도구가 필요하듯이 각 데이터 타입 값들도 각자에게 적합한 도구를 가짐. 타입을 명시적으로 지정하면 코드를 읽는 사람이 변수의 의도를 더 쉽게 이해할 수 있고, 잘못된 데이터 타입으로 인한 오류를 미리 예방

### Sequence Types (복습)
* 여러 개의 값들을 순서대로 나열하여 저장하는 자료형(str, list, tuple, range)
* **특징:** 순서(Sequence), 인덱싱(Indexing), 슬라이싱(Slicing), 길이(Length), 반복(Iteration)

---

## 2. Sequence Types: list

### 리스트(list)
* 여러 개의 값을 순서대로 저장하는 변경 가능한 시퀀스 자료형

### 리스트 표현
* 0개 이상의 객체를 포함하며 데이터 목록을 저장
* 대괄호(`[]`)로 표기
* 데이터는 어떤 자료형도 저장할 수 있음

```python
my_list_1 = []
my_list_2 = [1, 'a', 3, 'b', 5]
my_list_3 = [1, 2, 3, 'Python', ['hello', 'world', '!!!']]
```

### 리스트 시퀀스의 특징 — 인덱싱 / 슬라이싱 / 길이
```python
my_list = [1, 'a', 3, 'b', 5]

# 인덱싱
print(my_list[1])  # a

# 슬라이싱
print(my_list[2:4])   # [3, 'b']
print(my_list[:3])    # [1, 'a', 3]
print(my_list[3:])    # ['b', 5]
print(my_list[0:5:2]) # [1, 3, 5]
print(my_list[::-1])  # [5, 'b', 3, 'a', 1]

# 길이
print(len(my_list))   # 5
```

### 중첩된 리스트 접근 예
```python
my_list = [1, 2, 3, 'Python', ['hello', 'world', '!!!']]

print(len(my_list))        # 5
print(my_list[4][-1])      # !!!
print(my_list[-1][1][0])   # w
```

### 리스트는 가변(변경 가능)
```python
my_list = [1, 2, 3]
my_list[0] = 100

print(my_list)  # [100, 2, 3]
```

---

## 3. Sequence Types: tuple

### 튜플(tuple)
* 여러 개 값을 순서대로 저장하는 변경 **불가능한** 시퀀스 자료형

### 튜플 표현
* 0개 이상의 객체를 포함하며 데이터 목록을 저장
* 소괄호(`()`)로 표기
* 데이터는 어떤 자료형도 저장할 수 있음
* 단일 요소 튜플을 만들 때는 반드시 Trailing comma(후행 쉼표)를 사용해야 함

```python
my_tuple_1 = ()
my_tuple_2 = (1,)
my_tuple_3 = (1, 'a', 3, 'b', 5)
```

### 튜플 시퀀스의 특징 — 인덱싱 / 슬라이싱 / 길이
```python
my_tuple = (1, 'a', 3, 'b', 5)

# 인덱싱
print(my_tuple[1])  # a

# 슬라이싱
print(my_tuple[2:4])    # (3, 'b')
print(my_tuple[:3])     # (1, 'a', 3)
print(my_tuple[3:])     # ('b', 5)
print(my_tuple[0:5:2])  # (1, 3, 5)
print(my_tuple[::-1])   # (5, 'b', 3, 'a', 1)

# 길이
print(len(my_tuple))    # 5
```

### 튜플은 불변(변경 불가)
```python
my_tuple = (1, 'a', 3, 'b', 5)
# TypeError: 'tuple' object does not support item assignment
my_tuple[1] = 'z'
```

### 튜플은 어디에 쓰일까?
* 튜플의 불변 특성을 사용하여 내부 동작과 안전한 데이터 전달에 사용됨
* 다중 할당, 값 교환, 그룹화, 함수 다중 반환 값 등

```python
# 다중 할당
x, y = 10, 20
print(x)  # 10
print(y)  # 20

# 값 교환
x, y = 1, 2
x, y = y, x
# 실제 내부 동작
temp = (y, x)  # 튜플 생성
x, y = temp    # 튜플 언패킹
print(x, y)  # 2 1

# 그룹화
student = ('Kim', 20, 'CS')
name, age, major = student  # 언패킹
print(name, age, major)  # Kim 20 CS
```

---

## 4. Sequence Types: range

### range
* 연속된 정수 시퀀스를 생성하는 변경 불가능한 자료형

### range 기본 구문
* 모든 매개변수는 정수만 사용 가능

```
range(시작 값, 끝 값, 증가 값)
```

### range 매개변수별 특징
* `range(n)`: 0부터 n-1까지 1씩 증가
* `range(n, m)`: n부터 m-1까지 1씩 증가
* `range(n, m, step)`: n부터 m-1까지 step만큼 증가

```python
my_range_1 = range(5)
my_range_2 = range(1, 10)
my_range_3 = range(5, 0, -1)

print(my_range_1)  # range(0, 5)
print(my_range_2)  # range(1, 10)
print(my_range_3)  # range(5, 0, -1)

print(list(my_range_1))  # [0, 1, 2, 3, 4]
print(list(my_range_2))  # [1, 2, 3, 4, 5, 6, 7, 8, 9]
print(list(my_range_3))  # [5, 4, 3, 2, 1]
```

### range 증가 값 규칙
* 기본 증가값은 1
* 음수 증가값 → 감소하는 수열 생성
* 양수 증가값 → 증가하는 수열 생성
* 증가 값이 0이면 에러

### range 값의 범위 규칙
* **음수 증가 시:** 시작 값이 끝 값보다 커야 함 — 시작 값이 끝 값보다 작은 경우 빈 range 반환

```python
print(list(range(5, 1, -1)))  # [5, 4, 3, 2]
print(list(range(1, 5, -1)))  # []
```

* **양수 증가 시:** 시작 값이 끝 값보다 작아야 함 — 시작 값이 끝 값보다 큰 경우 빈 range 반환

```python
print(list(range(1, 5)))   # [1, 2, 3, 4]
print(list(range(5, 1)))   # []
```

### range 활용 예시
* 주로 반복문과 함께 활용

```python
for i in range(1, 10):
    print(i)  # 1 2 3 4 5 6 7 8 9

for i in range(1, 10, 2):
    print(i)  # 1 3 5 7 9
```

---

## 5. Non-Sequence Types: dict

### 딕셔너리(dict)
* Key-Value 쌍으로 이루어진 순서와 중복이 없는 변경 가능한 자료형

### 딕셔너리 표현
* key는 변경 불가능한 자료형만 사용 가능(str, int, float, tuple, range …)
* value는 모든 자료형 사용 가능
* 중괄호(`{}`)로 표기

```python
my_dict_1 = {}
my_dict_2 = {'key': 'value'}
my_dict_3 = {'apple': 12, 'list': [1, 2, 3]}

print(my_dict_1)  # {}
print(my_dict_2)  # {'key': 'value'}
print(my_dict_3)  # {'apple': 12, 'list': [1, 2, 3]}
```

### 딕셔너리 사용 예 — Key를 통해 value에 접근
```python
my_dict = {'apple': 12, 'list': [1, 2, 3]}

print(my_dict['apple'])  # 12
print(my_dict['list'])   # [1, 2, 3]

# 추가
my_dict['banana'] = 50
print(my_dict)  # {'apple': 12, 'list': [1, 2, 3], 'banana': 50}

# 변경
my_dict['apple'] = 100
print(my_dict)  # {'apple': 100, 'list': [1, 2, 3], 'banana': 50}
```

---

## 6. Non-Sequence Types: set

### 세트(set)
* 순서와 중복이 없는 변경 가능한 자료형

### 세트 표현
* 수학에서의 집합과 동일한 연산 처리 가능
* 중괄호(`{}`)로 표기

```python
my_set_1 = set()
my_set_2 = {1, 2, 3}
my_set_3 = {1, 1, 1}

print(my_set_1)  # set()
print(my_set_2)  # {1, 2, 3}
print(my_set_3)  # {1}
```

### 세트의 집합 연산
```python
my_set_1 = {1, 2, 3}
my_set_2 = {3, 6, 9}

# 합집합
print(my_set_1 | my_set_2)  # {1, 2, 3, 6, 9}

# 차집합
print(my_set_1 - my_set_2)  # {1, 2}

# 교집합
print(my_set_1 & my_set_2)  # {3}
```

---

## 7. Other Types: None / Boolean

### None
* 파이썬에서 '값이 없음'을 표현하는 자료형

```python
variable = None
print(variable)  # None
```

### Boolean
* 참(True)과 거짓(False)을 표현하는 자료형

### Boolean 표현
* 비교/논리 연산의 평가 결과로 사용됨
* 주로 조건/반복문과 함께 사용

```python
bool_1 = True
bool_2 = False

print(bool_1)      # True
print(bool_2)      # False
print(3 > 1)       # True
print('3' != 3)    # True
```

---

## 8. Collection

### Collection이란?
* 여러 개의 항목 또는 요소를 담는 자료 구조
* str, list, tuple, set, dict

### Collection 정리
| 컬렉션 | 변경 가능 여부 | 순서 여부 | 구분 |
| --- | --- | --- | --- |
| str | X | O | 시퀀스 |
| list | O | O | 시퀀스 |
| tuple | X | O | 시퀀스 |
| dict | O | X | 비시퀀스 |
| set | O | X | 비시퀀스 |

### 불변과 가변의 차이
```python
my_str = 'hello'
# TypeError: 'str' object does not support item assignment
my_str[0] = 'z'

my_list = [1, 2, 3]
my_list[0] = 100
# [100, 2, 3]
print(my_list)
```
* 불변(Immutable) 객체는 한 번 생성되면 값을 변경할 수 없어 값을 바꾸려면 새로운 객체를 만들어야 하고, 가변(Mutable) 객체는 같은 메모리 주소를 참조하는 상태로 내부 값만 바꿀 수 있음 (Python Tutor로 프레임/객체 참조 흐름을 시각적으로 확인 가능)

---

## 9. Type Conversion (형변환)

### 형변환(Type Conversion)
* 한 데이터 타입을 다른 데이터 타입으로 변환하는 과정
* 암시적 형변환 / 명시적 형변환

### 암시적 형변환(Implicit Type Conversion)
* 파이썬이 자동으로 수행하는 형변환
* 예: 정수와 실수의 연산에서 정수가 실수로 변환됨. Boolean과 Numeric Type 에서만 가능

```python
print(3 + 5.0)      # 8.0
print(True + 3)     # 4
print(True + False) # 1
```

### 명시적 형변환(Explicit Type Conversion)
* 프로그래머가 직접 지정하는 형변환
* 암시적 형변환이 아닌 경우를 모두 포함
* 예: `str` → `int`는 형식에 맞는 숫자만 가능

```python
print(int('1'))      # 1
print(int('3.5'))    # ValueError: invalid literal for int() with base 10: '3.5'
print(int(3.5))       # 3
print(float('3.5'))   # 3.5
```
* `int` → `str`은 모두 가능: `print(str(1) + '등')  # 1등`

### 컬렉션 간 형변환 정리
| From \ To | str | list | tuple | range | set | dict |
| --- | --- | --- | --- | --- | --- | --- |
| str | - | O | O | X | O | X |
| list | O | - | O | X | O | X |
| tuple | O | O | - | X | O | X |
| range | O | O | O | - | O | X |
| set | O | O | O | X | - | X |
| dict | O(key만) | O(key만) | O(key만) | X | O(key만) | - |

---

## 10. 연산자

### 산술 연산자
숫자 값에 대해 기본적인 수학 연산을 수행하는 연산자

| 기호 | 연산자 |
| --- | --- |
| - | 음수 부호 |
| + | 덧셈 |
| - | 뺄셈 |
| * | 곱셈 |
| / | 나눗셈 |
| // | 정수 나눗셈(몫) |
| % | 나머지 |
| ** | 지수(거듭제곱) |

### 복합 연산자
연산과 할당이 함께 이뤄짐

| 기호 | 예시 | 의미 |
| --- | --- | --- |
| += | `a += b` | `a = a + b` |
| -= | `a -= b` | `a = a - b` |
| *= | `a *= b` | `a = a * b` |
| /= | `a /= b` | `a = a / b` |
| //= | `a //= b` | `a = a // b` |
| %= | `a %= b` | `a = a % b` |
| **= | `a **= b` | `a = a ** b` |

```python
y = 10
y -= 4
print(y)  # 6

z = 7
z *= 2
print(z)  # 14

w = 15
w /= 4
print(w)  # 3.75

q = 20
q //= 3
print(q)  # 6
```

### 비교 연산자
| 기호 | 내용 |
| --- | --- |
| < | 미만 |
| <= | 이하 |
| > | 초과 |
| >= | 이상 |
| == | 같음 |
| != | 같지 않음 |
| is | 같음 |
| is not | 같지 않음 |

### is 비교 연산자
* 메모리 내에서 같은 객체를 참조하는지 확인
* `==`는 동등성(equality), `is`는 식별성(identity)
* 값을 비교하는 `==`와 다름

| 기호 | 내용 |
| --- | --- |
| is | 같음 |
| is not | 같지 않음 |

### 비교 연산자 예
```python
print(3 > 6)          # False
print(2.0 == 2)       # True
print(2 != 2)         # False
print('HI' == 'hi')   # False
print(1 == True)      # True

# ==은 값(데이터)을 비교하는 것이지만 is는 레퍼런스(주소)를 비교하기 때문에
# 아래 조건은 항상 False이기 때문에 is 대신 ==를 사용해야 한다는 것을 알림
print(1 is True)      # False
print(2 is 2.0)       # False
```

### 논리 연산자
| 기호 | 연산자 | 내용 |
| --- | --- | --- |
| and | 논리곱 | 두 피연산자 모두 True인 경우에만 전체 표현식을 True로 평가 |
| or | 논리합 | 두 피연산자 중 하나라도 True인 경우 전체 표현식을 True로 평가 |
| not | 논리부정 | 단일 피연산자를 부정 |

```python
print(True and False)  # False
print(True or False)   # True
print(not True)        # False
print(not 0)           # True
```

* 비교 연산자와 함께 사용 가능

```python
num = 15
result = (num > 10) and (num % 2 == 0)
print(result)  # False

name = 'Alice'
age = 25
result = (name == 'Alice') or (age == 30)
print(result)  # True
```

### 단축평가(Short-circuit Evaluation)
* 논리 연산에서 두 번째 피연산자를 평가하지 않고 결과를 결정하는 동작
* **and:** 첫 번째 피연산자가 False인 경우, 전체 표현식은 False로 결정, 두 번째 피연산자는 평가되지 않고 그 값이 무시. 첫 번째 피연산자가 True인 경우, 전체 표현식의 결과는 두 번째 피연산자에 의해 결정, 두 번째 피연산자가 평가되고 그 결과가 전체 표현식의 결과로 반환
* **or:** 첫 번째 피연산자가 True인 경우, 전체 표현식은 True로 결정, 두 번째 피연산자는 평가되지 않고 그 값이 무시. 첫 번째 피연산자가 False인 경우, 전체 표현식의 결과는 두 번째 피연산자에 의해 결정, 두 번째 피연산자가 평가되고 그 결과가 전체 표현식의 결과로 반환

**단축 평가 예시**
```python
vowels = 'aeiou'

print(('a' and 'b') in vowels)  # False ('a and b'는 'b'로 평가됨, 'b' not in vowels)
print(('b' and 'a') in vowels)  # True

print(3 and 5)   # 5 (첫 값이 참이면 두 번째 값 반환)
print(3 and 0)   # 0
print(0 and 3)   # 0 (첫 값이 거짓이면 그 값 반환)
print(0 and 0)   # 0

print(5 or 3)    # 5 (첫 값이 참이면 그 값 반환)
print(3 or 0)    # 3
print(0 or 3)    # 3 (첫 값이 거짓이면 두 번째 값 반환)
print(0 or 0)    # 0
```

### 단축 평가를 사용해야 하는 이유
* 코드 실행을 최적화하고, 불필요한 연산을 피할 수 있도록 함
* 직관적인 조건 처리

### 단축 평가 사용 시 주의해야 할 점
* 지나치게 의존 시 다른 팀원이 코드 의도를 파악하기 어려울 수 있음
* 명확하게 이해하지 못한 채 사용할 경우, 논리적 실수가 발생할 수 있음

### 멤버쉽 연산자
특정 값이 시퀀스나 다른 컬렉션에 속하는지 여부를 확인

| 기호 | 내용 |
| --- | --- |
| in | 왼쪽 피연산자가 오른쪽 피연산자의 시퀀스에 속하는지를 확인 |
| not in | 왼쪽 피연산자가 오른쪽 피연산자의 시퀀스에 속하지 않는지를 확인 |

```python
word = 'hello'
numbers = [1, 2, 3, 4, 5]

print('h' in word)         # True
print('z' in word)         # False
print(4 not in numbers)    # False
print(6 not in numbers)    # True
```

### 시퀀스형 연산자
`+`와 `*`는 시퀀스 간 연산에서 산술 연산자일때와 다른 역할을 가짐

| 연산자 | 내용 |
| --- | --- |
| + | 결합 연산자 |
| * | 반복 연산자 |

```python
# Gildong Hong
print('Gildong' + ' Hong')

# hihihihihi
print('hi' * 5)

# [1, 2, 'a', 'b']
print([1, 2] + ['a', 'b'])

# [1, 2, 1, 2]
print([1, 2] * 2)
```

### 연산자 우선순위 정리
| 우선순위 | 연산자 | 내용 |
| --- | --- | --- |
| 높음 | `()` | 소괄호 grouping |
|  | `[]` | 인덱싱, 슬라이싱 |
|  | `**` | 거듭제곱 |
|  | `+`, `-` | 단항 연산자 양수/음수 |
|  | `*`, `/`, `//`, `%` | 산술 연산자 |
|  | `+`, `-` | 산술 연산자 |
|  | `<`, `<=`, `>`, `>=`, `==`, `!=` | 비교 연산자 |
|  | `is`, `is not` | 객체 비교 |
|  | `in`, `not in` | 멤버쉽 연산자 |
|  | `not` | 논리 부정 |
|  | `and` | 논리 AND |
| 낮음 | `or` | 논리 OR |

---

## 핵심 요약
* **list**는 가변(mutable) 시퀀스, **tuple**은 불변(immutable) 시퀀스로, 튜플은 불변성 덕분에 다중 할당·값 교환·함수 다중 반환 값 전달에 안전하게 활용된다.
* **range**는 `range(시작, 끝, 증가)`로 연속된 정수 시퀀스를 생성하는 불변 자료형이며, 증가 값의 부호와 시작/끝 값의 대소 관계가 맞지 않으면 빈 range가 된다.
* **dict**(Key-Value, 순서 있음, key는 불변 타입만 가능)와 **set**(중복 없음, 순서 없음, 합집합/차집합/교집합 연산 지원)은 비시퀀스 컬렉션이다.
* Collection은 변경 가능 여부(list/dict/set은 O, str/tuple은 X)와 순서 여부(str/list/tuple은 O, dict/set은 X)로 구분되며, 형변환은 파이썬이 자동으로 수행하는 **암시적 형변환**과 프로그래머가 직접 지정하는 **명시적 형변환**으로 나뉜다.
* 연산자는 산술 → 복합(연산+할당) → 비교(`==`는 값, `is`는 참조 비교) → 논리(`and`/`or`/`not`, 단축평가로 두 번째 피연산자를 생략하기도 함) → 멤버쉽(`in`/`not in`) → 시퀀스형(`+`는 결합, `*`는 반복)까지 다양하며, 우선순위는 `()` > `[]` > `**` > 단항 `+`/`-` > `*`/`/`/`//`/`%` > `+`/`-` > 비교 > `is` > `in` > `not` > `and` > `or` 순이다.
