# Python DataStructure 1 정리

Python **메서드(method)**의 개념, 시퀀스 데이터 구조인 **문자열**과 **리스트**의 다양한 메서드, 그리고 객체와 참조, 얕은 복사/깊은 복사, 문자 유형 판별 메서드를 정리한 문서입니다.

---

## 1. Data Structure 개요

### 데이터 구조(Data Structure, 자료 구조)
* 여러 데이터를 효과적으로 사용, 관리하기 위한 구조(str, list, dict 등)
* 컴퓨터 공학에서는 '자료 구조'라고 함

### 데이터 구조 활용
* 문자열, 리스트, 딕셔너리 등 각 데이터 구조의 메서드를 호출하여 다양한 기능을 활용하기

### 메서드(method)
* 객체에 속한 함수(객체의 상태를 조작하거나 동작을 수행)
* 클래스(class) 내부에 정의되는 함수
* 클래스는 파이썬에서 '타입을 표현하는 방법'이며 이미 은연중에 사용해왔음
* 예를 들어 `help` 함수를 통해 `str`을 호출해보면 class 였다는 것을 확인 가능

```python
print(help(str))
"""
Help on class str in module builtins:

class str(object)
 |  str(object='') -> str
 |  str(bytes_or_buffer[, encoding[, errors]]) -> str
 |
 |  Create a new string object from the given object. If encoding or
 |  errors is specified, then the object must expose a data buffer
...
"""
```

### 메서드는 어딘가(클래스)에 속해 있는 함수
* 각 데이터 타입별로 다양한 기능을 가진 메서드가 존재

### 메서드(method) 호출 방법
```
데이터 타입 객체.메서드()
```

### 메서드(method) 호출 예시
```python
# 문자열 메서드 예시
print('hello'.capitalize())  # Hello

# 리스트 메서드 예시
numbers = [1, 2, 3]
numbers.append(4)
print(numbers)  # [1, 2, 3, 4]
```

---

## 2. 시퀀스 데이터 구조 — 문자열

### 문자열 조회/탐색 및 검증 메서드

| 메서드 | 설명 |
| --- | --- |
| `s.find(x)` | x의 첫 번째 위치를 반환, 없으면 -1을 반환 |
| `s.index(x)` | x의 첫 번째 위치를 반환, 없으면 오류 발생 |
| `s.isupper()` | 문자열 내의 모든 문자가 대문자인지 확인 |
| `s.islower()` | 문자열 내의 모든 문자가 소문자인지 확인 |
| `s.isalpha()` | 문자열 내의 모든 문자가 알파벳인지 확인 (단순 알파벳이 아닌 유니코드 상 Letter, 한국어도 포함) |

```python
print('banana'.find('a'))  # 1
print('banana'.find('z'))  # -1

print('banana'.index('a'))  # 1
print('banana'.index('z'))  # ValueError: substring not found
```

```python
string1 = 'HELLO'
string2 = 'Hello'
print(string1.isupper())  # True
print(string2.isupper())  # False
print(string1.islower())  # False
print(string2.islower())  # False
```

```python
string1 = 'Hello'
string2 = '123heis98576ssh'
print(string1.isalpha())  # True
print(string2.isalpha())  # False
```

### 문자열 조작 메서드(새 문자열 반환)

| 메서드 | 설명 |
| --- | --- |
| `s.replace(old, new[, count])` | 바꿀 대상 글자를 새로운 글자로 바꿔서 반환 |
| `s.strip([chars])` | 문자열의 시작과 끝에 있는 공백 혹은 지정한 문자를 제거 |
| `s.split(sep=None, maxsplit=-1)` | sep를 구분자 문자열로 사용하여 문자열에 있는 단어들의 리스트를 반환 |
| `'separator'.join(iterable)` | 구분자로 iterable의 문자열을 연결한 문자열을 반환 |
| `s.capitalize()` | 가장 첫 번째 글자를 대문자로 변경 |
| `s.title()` | 문자열 내 띄어쓰기 기준으로 각 단어의 첫 글자는 대문자로, 나머지는 소문자로 변환 |
| `s.upper()` | 모두 대문자로 변경 |
| `s.lower()` | 모두 소문자로 변경 |
| `s.swapcase()` | 대 ↔ 소문자 서로 변경 |

```python
text = 'Hello, world! world world'
new_text1 = text.replace('world', 'Python')
new_text2 = text.replace('world', 'Python', 1)
print(new_text1)  # Hello, Python! Python Python
print(new_text2)  # Hello, Python! world world
```

```python
text = '  Hello, world!  '
new_text = text.strip()
print(new_text)  # 'Hello, world!'
```

```python
text = 'Hello, world!'
words1 = text.split(',')
words2 = text.split()
print(words1)  # ['Hello', ' world!']
print(words2)  # ['Hello,', 'world!']
```

```python
words = ['Hello', 'world!']
text = '-'.join(words)
print(text)  # 'Hello-world!'
```

```python
text = 'heLLo, woRld!'
new_text1 = text.capitalize()
new_text2 = text.title()
new_text3 = text.upper()
new_text4 = text.lower()
new_text5 = text.swapcase()

print(new_text1)  # Hello, world!
print(new_text2)  # Hello, World!
print(new_text3)  # HELLO, WORLD!
print(new_text4)  # hello, world!
print(new_text5)  # HEllO, WOrLD!
```

---

## 3. 시퀀스 데이터 구조 — 리스트

### 리스트 값 추가 및 삭제 메서드

| 메서드 | 설명 |
| --- | --- |
| `L.append(x)` | 리스트 마지막에 항목 x를 추가 |
| `L.extend(m)` | Iterable m의 모든 항목들을 리스트 끝에 추가(`+=`과 같은 기능) |
| `L.insert(i, x)` | 리스트의 저장된 인덱스 i에 항목 x를 삽입 |
| `L.remove(x)` | 리스트 가장 왼쪽에 있는 항목(첫 번째) x를 제거, 항목이 존재하지 않을 경우 ValueError |
| `L.pop()` | 리스트 가장 오른쪽에 있는 항목(마지막)을 반환 후 제거 |
| `L.pop(i)` | 리스트의 인덱스 i에 있는 항목을 반환 후 제거 |
| `L.clear()` | 리스트의 모든 항목 삭제 |

**`.append(x)`** — 리스트 마지막에 항목 x를 추가
```python
my_list = [1, 2, 3]
my_list.append(4)
print(my_list)  # [1, 2, 3, 4]
```

**`.extend(iterable)`** — 리스트 마지막에 항목 x를 추가
```python
my_list = [1, 2, 3]
my_list.extend([5, 6, 7])
print(my_list)  # [1, 2, 3, 5, 6, 7]
```

**`.extend(iterable)` 주의사항** — append()와 비교, 반복 가능한 객체가 아니면 추가 불가
```python
my_list = [1, 2, 3]
my_list.extend(100)
# TypeError: 'int' object is not iterable
```

**`.insert(i, x)`** — 리스트의 저장한 인덱스 i 위치에 항목 x를 삽입
```python
my_list = [1, 2, 3]
my_list.insert(1, 5)
print(my_list)  # [1, 5, 2, 3]
```

**`.remove(x)`** — 리스트에서 첫번째로 일치하는 항목을 삭제
```python
my_list = [1, 2, 3, 2, 2, 2]
my_list.remove(2)
print(my_list)  # [1, 3, 2, 2, 2]
```

**`.pop(i)`** — 리스트에서 지정한 인덱스의 항목을 제거하고 반환, 작성하지 않을 경우 마지막 항목을 제거
```python
my_list = [1, 2, 3, 4, 5]
item1 = my_list.pop()
item2 = my_list.pop(0)
print(item1)      # 5
print(item2)      # 1
print(my_list)    # [2, 3, 4]
```

**`.clear()`** — 리스트의 모든 항목을 삭제
```python
my_list = [1, 2, 3]
my_list.clear()
print(my_list)  # []
```

### 리스트 탐색 및 정렬 메서드

| 메서드 | 설명 |
| --- | --- |
| `L.index(x)` | 리스트에서 첫 번째로 일치하는 항목 x의 인덱스를 반환 |
| `L.count(x)` | 리스트에서 항목 x의 개수를 반환 |
| `L.reverse()` | 리스트의 순서를 역순으로 변경(정렬 X) |
| `L.sort()` | 리스트를 정렬(매개변수 이용가능) |

```python
my_list = [1, 2, 3]
index = my_list.index(2)
print(index)  # 1
```

```python
my_list = [1, 2, 2, 3, 3, 3]
count = my_list.count(3)
print(count)  # 3
```

```python
my_list = [1, 3, 2, 8, 1, 9]
my_list.reverse()
print(my_list.reverse())  # None (reverse는 반환값이 없음)
print(my_list)  # [9, 1, 8, 2, 3, 1]
```

```python
my_list = [3, 2, 100, 1]
my_list.sort()
print(my_list)  # [1, 2, 3, 100]

# 내림차순 정렬
my_list.sort(reverse=True)
print(my_list)  # [100, 3, 2, 1]
```

### 다양한 리스트 메서드
참고: `https://docs.python.org/3.9/tutorial/datastructures.html#data-structures`

---

## 4. 참고 — 객체와 참조

### 가변/불변 객체 개념
1. **Mutable(가변) 객체:** 생성 후 내용을 변경할 수 있는 객체 (예: 리스트(list), 딕셔너리(dict), 집합(set))
2. **Immutable(불변) 객체:** 생성 후 내용을 변경할 수 없는 객체 (예: 정수(int), 실수(float), 문자열(str), 튜플(tuple))

### 변수 할당의 의미
* 파이썬에서 변수 할당은 객체에 대한 참조를 생성하는 과정
* 변수는 객체의 메모리 주소를 가리키는 Label 역할을 함
* `=` 연산자를 사용하여 변수에 값을 할당
* 할당 시 새로운 객체가 생성되거나 기존 객체에 대한 참조가 생성됨

### 메모리 참조 방식
* 변수는 객체의 '메모리 주소'를 저장
* 여러 변수가 동일한 객체를 참조할 수 있음

### 가변 객체 예시
```python
a = [1, 2, 3, 4]
b = a
b[0] = 100

print(a)  # [100, 2, 3, 4]
print(b)  # [100, 2, 3, 4]
print(a is b)  # True
```
* `a`와 `b`는 같은 리스트 객체를 참조하므로, `b`를 통한 변경이 `a`에도 반영됨

### 불변 객체 예시
```python
a = 20
b = a
b = 10

print(a)  # 20
print(b)  # 10
print(a is b)  # False
```
* `b = 10`은 `b`가 새로운 객체(10)를 참조하도록 재할당하는 것일 뿐, `a`가 참조하는 객체(20)에는 영향이 없음

### id() 함수를 사용한 메모리 주소 확인
* `id()` 함수를 사용하여 객체의 메모리 주소를 확인 가능
* `is` 연산자를 통해 두 변수가 같은 객체를 참조하는지 확인 가능

```python
x = [1, 2, 3]
y = x
z = [1, 2, 3]

print(f'x의 id: {id(x)}')  # 예: 1682231207424
print(f'y의 id: {id(y)}')  # 1682231207424 (x와 동일)
print(f'z의 id: {id(z)}')  # 1682231224896 (다름)
print(f'x와 y는 같은 객체인가? {x is y}')  # True
print(f'x와 z는 같은 객체인가? {x is z}')  # False
```

### 가변/불변 메모리 관리 방식
* **가변 객체:** 생성 후에도 그 내용을 수정할 수 있음. 객체의 내용이 변경되어도 같은 메모리 주소를 유지
* **불변 객체:** 생성 후 그값을 변경할 수 없음. 새로운 값을 할당하면 새로운 객체가 생성되고, 변수는 새 객체를 참조하게 됨

### 이러한 동작 방식의 이유
1. **성능 최적화:** 불변 객체는 변경이 불가능하므로, 여러 변수가 같은 객체를 안전하게 공유할 수 있음. 가변 객체는 내용 수정이 빈번한 경우 새 객체를 생성하지 않고 직접 수정하여 성능을 향상시킴
2. **메모리 효율성:** 불변 객체는 동일한 값을 가진 여러 객체가 메모리를 공유할 수 있어 효율적. 가변 객체는 크기가 큰 데이터를 효율적으로 수정할 수 있음

---

## 5. 참고 — 얕은 복사(Shallow Copy)

### 얕은 복사란?
* 객체의 최상위 요소만 새로운 메모리에 복사하는 방법
* 내부에 중첩된 객체가 있다면 그 객체의 참조만 복사됨

### 얕은 복사 구현 방법
* 리스트 슬라이싱, `copy()` 메서드, `list()` 함수

### 얕은 복사 예시 — 1차원 리스트에서의 얕은 복사
```python
a = [1, 2, 3]
b = a[:]
c = a.copy()

b[0] = 100
c[0] = 999
print(a)  # [1, 2, 3]
print(b)  # [100, 2, 3]
print(c)  # [999, 2, 3]
```
* 1차원 리스트에서는 얕은 복사로도 완전히 독립적인 사본을 만들 수 있음(내부 요소가 불변 객체이므로)

### 얕은 복사의 한계 — 2차원 리스트와 같이 변경 가능한 객체 안에 변경 가능한 객체가 있는 경우
```python
a = [1, 2, [3, 4, 5]]
b = a[:]

b[0] = 999
print(a)  # [1, 2, [3, 4, 5]]
print(b)  # [999, 2, [3, 4, 5]]

b[2][1] = 100
print(a)  # [1, 2, [3, 100, 5]]
print(b)  # [999, 2, [3, 100, 5]]

print(f'a[2]와 b[2]가 같은 객체인가? {a[2] is b[2]}')  # True
```
* `a`와 `b`의 주소는 다르지만 내부 객체의 주소는 같기 때문에 함께 변경됨

### 1차원 리스트와 다차원 리스트에서의 차이점
* **1차원 리스트:** 얕은 복사로 충분히 독립적인 복사본을 만들 수 있음
* **다차원 리스트:** 최상위 리스트만 복사되고, 내부 리스트는 여전히 원본과 같은 객체를 참조

---

## 6. 참고 — 깊은 복사(Deep Copy)

### 깊은 복사란?
* 객체의 모든 수준의 요소를 새로운 메모리에 복사하는 방법
* 중첩된 객체까지 모두 새로운 객체로 생성됨
* `copy` 모듈에서 제공하는 `deepcopy()` 함수를 사용

```python
import copy
new_object = copy.deepcopy(original_object)
```

### 깊은 복사 예시
```python
import copy

a = [1, 2, [3, 4, 5]]
b = copy.deepcopy(a)

b[2][1] = 100
print(a)  # [1, 2, [3, 4, 5]]
print(b)  # [1, 2, [3, 100, 5]]
print(f'a[2]와 b[2]가 같은 객체인가? {a[2] is b[2]}')  # False
```

### 중첩된 객체에서의 깊은 복사
```python
original = {'a': [1, 2, 3], 'b': {'c': 4, 'd': [5, 6]}}
copied = copy.deepcopy(original)

print(f'원본: {original}')  # {'a': [1, 2, 3], 'b': {'c': 4, 'd': [5, 6]}}
print(f'복사본: {copied}')  # {'a': [1, 2, 3], 'b': {'c': 4, 'd': [5, 6]}}
print(f"original['b']와 copied['b']가 같은 객체인가? {original['b'] is copied['b']}")  # False
```

---

## 7. 참고 — 문자 유형 판별 메서드

### 문자열에 포함된 문자들의 유형을 판별하는 메서드
* **`isdecimal()`:** 문자열이 모두 숫자 문자(0~9)로만 이루어져 있어야 True
* **`isdigit()`:** `isdecimal()`과 비슷하지만, 유니코드 숫자도 인식(`'①'`도 숫자로 인식)
* **`isnumeric()`:** `isdigit()`과 유사하지만, 몇 가지 추가적인 유니코드 문자들을 인식(분수, 지수, 루트 기호도 숫자로 인식)

### 관계: isdecimal() ⊆ isdigit() ⊆ isnumeric()

| isdecimal() | isdigit() | isnumeric() | 예시 |
| --- | --- | --- | --- |
| True | True | True | "038", "٥٣٧٤"(아랍 숫자), "０３８"(전각 숫자) |
| False | True | True | "⁵³", "ㅁㅣㆍ", "①③⑧" |
| False | False | True | "½", "Ⅷ"(로마 숫자), "壹貳參"(한자 숫자) |
| False | False | False | "abc", "38.0", "-38" |

---

## 핵심 요약
* **메서드(method)**는 클래스 내부에 정의된 함수로, `데이터 타입 객체.메서드()` 형태로 호출하며 데이터 타입별로 다양한 메서드를 제공한다.
* **문자열 메서드**는 조회/검증(`find`, `index`, `isupper`, `islower`, `isalpha`)과 새 문자열을 반환하는 조작(`replace`, `strip`, `split`, `join`, `capitalize`, `title`, `upper`, `lower`, `swapcase`)으로 나뉘며, 문자열은 불변이므로 원본은 바뀌지 않는다.
* **리스트 메서드**는 값 추가/삭제(`append`, `extend`, `insert`, `remove`, `pop`, `clear`)와 탐색/정렬(`index`, `count`, `reverse`, `sort`)로 나뉘며, 리스트는 가변 객체이므로 원본이 직접 변경된다.
* 변수 할당은 객체에 대한 참조(메모리 주소)를 생성하는 것이며, **가변 객체**(list, dict, set)는 내용 수정이 가능해 같은 객체를 참조하는 다른 변수에도 영향을 주지만, **불변 객체**(int, float, str, tuple)는 재할당 시 새 객체를 참조하게 될 뿐 기존 참조에는 영향이 없다.
* **얕은 복사**(슬라이싱, `copy()`, `list()`)는 최상위 요소만 복사해 중첩된 가변 객체는 원본과 참조를 공유하지만, **깊은 복사**(`copy.deepcopy()`)는 중첩된 모든 요소까지 새 객체로 복사해 완전히 독립적인 사본을 만든다.
* 문자 유형 판별은 `isdecimal() ⊆ isdigit() ⊆ isnumeric()` 관계를 가지며, 뒤로 갈수록 더 넓은 범위의 유니코드 숫자 표현(분수, 로마 숫자, 한자 숫자 등)을 인식한다.
