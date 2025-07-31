# Python DataStructure 2 정리

비시퀀스 데이터 구조인 **딕셔너리(dictionary)**와 **세트(set)**의 다양한 메서드, 그리고 **메서드 체이닝**, **해시 테이블**, **파이썬 문법 규격(BNF/EBNF)**을 정리한 문서입니다.

---

## 1. Data Structure 복습

* **데이터 구조:** 여러 데이터를 효과적으로 사용, 관리하기 위한 구조(str, list, dict 등)
* **메서드(method):** 객체에 속한 함수(객체의 상태를 조작하거나 동작을 수행). 클래스(class) 내부에 정의되는 함수. 데이터 타입 객체.메서드() 형태로 호출

---

## 2. 비시퀀스 데이터 구조 — dictionary

### dictionary
* 고유한 항목들의 정렬되지 않은 컬렉션
* 키(key)와 값(value)을 한 쌍으로 묶어 저장하는 자료형

```python
dict_sample = {
    'say': 'hello',
    1: 'test',
    'hello': 'world'
}
```

### 딕셔너리 관련 메서드

| 메서드 | 설명 |
| --- | --- |
| `D.clear()` | 딕셔너리 D의 모든 키/값 쌍을 제거 |
| `D.get(k)` | 키 k에 연결된 값을 반환(키가 없으면 None을 반환) |
| `D.get(k, v)` | 키 k에 연결된 값을 반환하거나 키가 없으면 기본 값으로 v를 반환 |
| `D.keys()` | 딕셔너리 D의 키를 모은 객체를 반환 |
| `D.values()` | 딕셔너리 D의 값을 모은 객체를 반환 |
| `D.items()` | 딕셔너리 D의 키/값 쌍을 모은 객체를 반환 |
| `D.pop(k)` | 딕셔너리 D에서 키 k를 제거하고 연결됐던 값을 반환(없으면 오류) |
| `D.pop(k, v)` | 딕셔너리 D에서 키 k를 제거하고 연결됐던 값을 반환(없으면 v를 반환) |
| `D.setdefault(k)` | 딕셔너리 D에서 키 k와 연결된 값을 반환 |
| `D.setdefault(k, v)` | 딕셔너리 D에서 키 k와 연결된 값을 반환, k가 D의 키가 아니면 값 v와 연결한 키 k를 D에 추가하고 v를 반환 |
| `D.update(other)` | other 내 각 키에 대해 D에 있는 키면 D에 있는 그 키의 값을 other에 있는 값으로 대체. other에 있는 키에 대해 D에 없으면 키/값 쌍을 D에 추가 |

### `.clear()` — 딕셔너리의 모든 키/값 쌍을 제거
```python
person = {'name': 'Alice', 'age': 25}
person.clear()
print(person)  # {}
```

### `.get(key[, default])` — 키 연결된 값을 반환하거나 키가 없으면 None 혹은 기본 값을 반환
```python
person = {'name': 'Alice', 'age': 25}
print(person.get('name'))              # Alice
print(person.get('country'))           # None
print(person.get('country', 'Unknown'))# Unknown
print(person['country'])               # KeyError: 'country'
```

### `.keys()` — 딕셔너리 키를 모은 객체를 반환
```python
person = {'name': 'Alice', 'age': 25}
print(person.keys())  # dict_keys(['name', 'age'])
for item in person.keys():
    print(item)
# name
# age
```

### `.values()` — 딕셔너리 값을 모은 객체를 반환
```python
person = {'name': 'Alice', 'age': 25}
print(person.values())  # dict_values(['Alice', 25])
for item in person.values():
    print(item)
# Alice
# 25
```

### `.items()` — 딕셔너리 키/값 쌍을 모은 객체를 반환
```python
person = {'name': 'Alice', 'age': 25}
print(person.items())  # dict_items([('name', 'Alice'), ('age', 25)])
for key, value in person.items():
    print(key, value)
# name Alice
# age 25
```

### `.pop(key[, default])` — 키를 제거하고 연결됐던 값을 반환(없으면 에러나 default를 반환)
```python
person = {'name': 'Alice', 'age': 25}
print(person.pop('age'))            # 25
print(person)                       # {'name': 'Alice'}
print(person.pop('country', None))  # None
print(person.pop('country'))        # KeyError
```

### `.setdefault(key[, default])` — 키와 연결된 값을 반환, 키가 없다면 default와 연결한 키를 딕셔너리에 추가하고 default를 반환
```python
person = {'name': 'Alice', 'age': 25}
print(person.setdefault('country', 'KOREA'))  # KOREA
print(person)  # {'name': 'Alice', 'age': 25, 'country': 'KOREA'}
```

### `.update([other])` — other가 제공하는 키/값 쌍으로 딕셔너리를 갱신, 기존 키는 덮어씀
```python
person = {'name': 'Alice', 'age': 25}
other_person = {'name': 'Jane', 'country': 'KOREA'}
person.update(other_person)
print(person)  # {'name': 'Jane', 'age': 25, 'country': 'KOREA'}

person.update(age=100, address='SEOUL')
print(person)  # {'name': 'Jane', 'age': 100, 'country': 'KOREA', 'address': 'SEOUL'}
```

참고: `https://docs.python.org/3/library/stdtypes.html#dict`

---

## 3. 비시퀀스 데이터 구조 — set

### set
* 고유한 항목들의 정렬되지 않은 컬렉션
* 중복을 허용하지 않는 자료구조
* 집합 이론에서 착안되어 만들어 짐

```python
sample_set = {1, 2, 3, 4}
```

### 세트 메서드

| 메서드 | 설명 |
| --- | --- |
| `s.add(x)` | 세트 s에 항목 x를 추가. 이미 x가 있다면 변화 없음 |
| `s.clear()` | 세트 s의 모든 항목을 제거 |
| `s.remove(x)` | 세트 s에서 항목 x를 제거, 항목 x가 없을 경우 Key error |
| `s.pop()` | 세트 s에서 임의의 항목을 반환하고, 해당 항목을 제거 |
| `s.discard(x)` | 세트 s에서 항목 x를 제거 |
| `s.update(iterable)` | 세트 s에 다른 iterable 요소를 추가 |

### `.add(x)` — 세트에 x를 추가
```python
my_set = {'a', 'b', 'c', 1, 2, 3}
my_set.add(4)
print(my_set)  # {1, 'b', 3, 2, 'c', 'd', 'a'}(순서는 매번 다를 수 있음)

my_set.add(4)  # 이미 있으므로 변화 없음
print(my_set)
```

### `.clear()` — 세트의 모든 항목을 제거
```python
my_set = {'a', 'b', 'c', 1, 2, 3}
my_set.clear()
print(my_set)  # set()
```

### `.remove(x)` — 세트에서 항목 x를 제거
```python
my_set = {'a', 'b', 'c', 1, 2, 3}
my_set.remove(2)
print(my_set)  # {'b', 1, 3, 'c', 'a'}

my_set.remove(10)  # KeyError: 10
```

### `.pop()` — 세트에서 임의의 요소를 제거하고 반환
```python
my_set = {'a', 'b', 'c', 1, 2, 3}
element = my_set.pop()
print(element)   # 1(예시)
print(my_set)    # {2, 3, 'b', 'a', 'c'}
```

### `.discard()` — 세트에서 항목 x를 제거, remove와 달리 에러 없음
```python
my_set = {'a', 'b', 'c', 1, 2, 3}
my_set.discard(2)
print(my_set)  # {1, 3, 'a', 'c', 'b'}

my_set.discard(10)  # 에러 없이 무시됨
```

### `.update(iterable)` — 세트에 다른 iterable 요소를 추가
```python
my_set = {'a', 'b', 'c', 1, 2, 3}
my_set.update([1, 4, 5])
print(my_set)  # {'c', 2, 3, 1, 'b', 4, 5, 'a'}
```

### 세트의 집합 메서드

| 메서드 | 설명 | 연산자 |
| --- | --- | --- |
| `set1.difference(set2)` | set1에는 들어있지만 set2에는 없는 항목으로 세트를 생성 후 반환 | `set1 - set2` |
| `set1.intersection(set2)` | set1과 set2 모두 들어있는 항목으로 세트를 생성 후 반환 | `set1 & set2` |
| `set1.issubset(set2)` | set1의 항목이 모두 set2에 들어있으면 True를 반환 | `set1 <= set2` |
| `set1.issuperset(set2)` | set1가 set2의 항목을 모두 포함하면 True를 반환 | `set1 >= set2` |
| `set1.union(set2)` | set1 또는 set2에(혹은 둘 다) 들어있는 항목으로 세트를 생성 후 반환 | `set1 \| set2` |

```python
set1 = {0, 1, 2, 3, 4}
set2 = {1, 3, 5, 7, 9}
set3 = {0, 1}

print(set1.difference(set2))    # {0, 2, 4}
print(set1.intersection(set2))  # {1, 3}
print(set1.issubset(set2))      # False
print(set3.issubset(set1))      # True
print(set1.issuperset(set2))    # False
print(set1.union(set2))         # {0, 1, 2, 3, 4, 5, 7, 9}
```

---

## 4. 참고 — 메서드 체이닝(Method Chaining)

### 메서드 체이닝이란?
* 여러 메서드를 연속해서 호출하는 방식

```python
text = 'heLLo, woRld!'
new_text = text.swapcase().replace('l', 'z')
print(new_text)  # HEzzO, WOrLD!
```
* 코드는 다음 순서로 실행됨
  1. `text.swapcase()`: 대소문자를 반전시킴 — `'heLLo, woRld!'` → `'HEllO, WOrLd!'`
  2. `.replace('l', 'z')`: 소문자 `'l'`을 `'z'`로 교체 — `'HEllO, WOrLd!'` → `'HEzzO, WOrLd!'`

### 문자열에서의 메서드 체이닝 예시
```python
# 1. 단계별로 실행하기
text = 'heLLo, woRld!'
step1 = text.swapcase()
print('1단계 결과:', step1)  # HEllO, WOrLd!

step2 = step1.replace('l', 'z')
print('2단계 결과:', step2)  # HEzzO, WOrLD!

# 2. 한 줄로 실행하기(위와 동일한 결과)
new_text = text.swapcase().replace('l', 'z')
print('최종 결과:', new_text)  # HEzzO, WOrLD!
```

### 리스트에서의 메서드 체이닝 예시
* `copy()`로 리스트를 복사한 후, `sorted()` 함수로 정렬

```python
numbers = [3, 1, 4, 1, 5, 9, 2]
result = numbers.copy().sort()
print(numbers)  # [3, 1, 4, 1, 5, 9, 2] (원본은 변경되지 않음)
print(result)   # None (sort() 메서드는 None을 반환하기 때문)

# 올바른 체이닝 예시
sorted_numbers = sorted(numbers.copy())
print(sorted_numbers)  # [1, 1, 2, 3, 4, 5, 9]
```

### 메서드 체이닝 주의사항
* 모든 메서드가 체이닝을 지원하는 것은 아님 — 메서드가 객체를 반환할때만 체이닝이 가능
* `None`을 반환하는 메서드는 메서드 체이닝이 불가능 (예: 리스트의 `append()`, `sort()`)
* 메서드 체이닝을 사용할 때는 각 메서드의 반환 값을 잘 이해하고 있어야 함

---

## 5. 참고 — 해시 테이블(Hash Table)

### 해시 테이블이란?
* 해쉬 함수를 사용하여 변환한 값을 색인(index)으로 삼아 키(key)와 데이터(value)를 저장하는 자료구조
* 데이터를 효율적으로 저장하고 검색하기 위해 사용

### 해시 테이블 원리
* 키를 해시 함수를 통해 해시 값으로 변환하고, 이 해시 값을 인덱스로 사용하여 데이터를 저장하거나 검색
* 데이터 검색이 매우 빠르게 이루어짐

```
keys → hash function → buckets
John Smith  ─┐
Lisa Smith  ─┼─→ [00][01: 521-8976][02: 521-1234][03]...[13][14: 521-9655][15]
Sandra Dee  ─┘
```

### 해시(Hash)
* 임의의 크기를 가진 데이터를 고정된 크기의 고유한 값으로 변환하는 것
* 이렇게 생성된 고유한 값은 주로 해당 데이터를 식별하는 데 사용될 수 있음 — 일종의 "지문"과 같은 역할, 데이터를 고유하게 식별

### 해시 함수(Hash Function)
* 임의의 길이의 데이터를 입력 받아 고정된 길이의 데이터(해시 값)를 출력하는 함수
* 주로 해시 테이블 자료구조에 사용되며, 매우 빠른 데이터 검색을 위한 컴퓨터 소프트웨어에서 유용하게 사용

### set의 pop 메서드 예시 — 정수
* 정수 값 자체가 곧 해시 값

```python
my_set = {3, 2, 1, 9, 100, 4, 87, 39, 10, 52}
print(my_set.pop())  # 1
print(my_set.pop())  # 2
print(my_set.pop())  # 3
print(my_set.pop())  # 100
# ... 정수는 오름차순에 가까운 해시 값 순서대로 반환됨
```

### set의 pop 메서드 예시 — 문자열
* 반환 값이 매번 다름

```python
my_str_set = {'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j'}
print(my_str_set.pop())
print(my_str_set.pop())
# 실행마다 다른 문자가 반환됨(해시 값 기반 순서)
```

### 파이썬에서의 해시 함수
* 파이썬에서 해시 함수의 동작 방식은 객체의 타입에 따라 달라짐
* 정수와 문자열은 서로 다른 타입이며, 이들의 해시 값을 계산하는 방식도 다름

```python
print(hash(1))  # 1
print(hash(1))  # 1

print(hash('a'))  # 실행시마다 다름
print(hash('a'))  # 실행시마다 다름
```

### 파이썬에서의 해시 함수 — 정수
* 같은 정수는 항상 같은 해시 값을 가짐
* 해시 테이블에 정수를 저장할 때 효율적인 방법
* 예를 들어, `hash(1)`과 `hash(2)`는 항상 서로 다른 해시 값을 갖지만, `hash(1)`은 항상 동일한 해시 값을 갖게 됨

### 파이썬에서의 해시 함수 — 문자열
* 문자열은 가변적인 길이를 갖고 있고, 문자열에 포함된 각 문자들의 유니코드 포인트 등을 기반으로 해시 값을 계산
* 이로 인해 문자열의 해시 값은 실행 시마다 다르게 계산됨

### set의 pop 메서드의 결과와 해시 테이블의 관계
* `set`의 `pop()`은 "임의의 요소"를 제거하고 반환함
* 실행할 때마다 다른 요소를 얻는다는 의미에서의 "무작위"가 아니라 "임의"라는 의미에서의 "무작위" — By "arbitrary" the docs don't mean "random"
=> 해시 테이블에 나타나는 순서대로 반환하는 것

### hashable
* `hash()` 함수의 인자로 전달해서 결과를 반환 받을 수 있는 객체
* 대부분의 불변형 데이터 타입은 hashable
* 단, tuple의 경우 불변형이지만 해시 불가능한 객체를 참조 할 때는 tuple 자체도 해시 불가능해짐

```python
print(hash(1))
print(hash(1.0))
print(hash('1'))
print(hash((1, 2, 3)))

# TypeError: unhashable type: 'list'
print(hash((1, 2, [3, 4])))
```

### hashable과 불변성 간의 관계
* 해시 테이블의 키는 불변해야 함 — 객체가 생성된 후에 그 값을 변경할 수 없어야 함
* 불변 객체는 해시 값이 변하지 않으므로 동일한 값에 대해 일관된 해시 값을 유지할 수 있음
* 단, "hash 가능하다 != 불변하다"

### 가변형 객체가 hashable 하지 않은 이유
* 값이 변경될 수 있기 때문에 동일한 객체에 대한 해시 값이 변경될 가능성이 있음(해시 테이블의 무결성 유지 불가)
* 가변형 객체가 변경되면 해시 값이 변경되기 때문에, 같은 객체에 대한 서로 다른 해시 값이 반환될 수 있음(해시 값의 일관성 유지 불가)

```python
# TypeError: unhashable type: 'list'
print(hash([1, 2, 3]))

my_set = {[1, 2, 3], 1, 2, 3, 4, 5}
# TypeError: unhashable type: 'list'

my_dict = {{3, 2}: 'a'}
# TypeError: unhashable type: 'set'
```

### hashable 객체가 필요한 이유
1. **해시 테이블 기반 자료 구조 사용:** set과 dict의 key, 중복 값 방지, 빠른 검색과 조회
2. **불변성을 통한 일관된 해시 값**
3. **안정성과 예측 가능성 유지**

---

## 6. 참고 — 파이썬 문법 규격

### 파이썬 문법 규격
* 파이썬 공식문서 예시

```
6. 표현식

이 장은 파이썬에서 사용되는 표현식 요소들의 의미를 설명합니다.

문법 유의 사항: 여기까지 아이거는 앞에서는, 어휘 분석이 아닌 문법을 설명하기 위해 확장 BNF 표기법을 사용합니다. 문법 규칙 다음 같은 형태를 가지고,

name ::= othername
```
참고: `https://docs.python.org/3.9/reference/expressions.html`

### BNF(Backus-Naur Form)
* 프로그래밍 언어의 문법을 표현하기 위한 표기법

### EBNF(Extended Backus-Naur Form)
* BNF를 확장한 표기법
* 메타 기호를 추가하여 더 간결하고 표현력이 강해진 형태

### 대표적인 EBNF 메타기호
| 메타 기호 | 의미 |
| --- | --- |
| `[]` | 선택적 요소 |
| `{}` | 0번 이상 반복 |
| `()` | 그룹화 |

### EBNF 메타기호 [] 사용 예시 — 딕셔너리의 pop 메서드
```
pop(key[, default])
```
"If key is in the dictionary, remove it and return its value, else return default. If default is not given and key is not in the dictionary, a KeyError is raised."
* `[, default]`는 대괄호로 감싸져 있으므로 `default`가 선택적 인자임을 의미

### BNF와 같은 표기법을 사용하는 이유
* 서로 다른 프로그래밍 언어, 데이터 형식, 프로토콜 등의 문법을 통일하여 정의하기 위함

---

## 핵심 요약
* **dictionary**는 키-값 쌍을 저장하는 자료형으로 `get`/`pop`/`setdefault`/`update` 등으로 안전하게 조회·삭제·병합할 수 있고, **set**은 중복 없는 컬렉션으로 `add`/`remove`/`discard`(에러 없이 제거) 외에 `difference`/`intersection`/`issubset`/`union` 등 집합 연산 메서드를 제공한다.
* **메서드 체이닝**은 메서드가 객체를 반환할 때만 가능하며, `sort()`처럼 `None`을 반환하는 메서드는 체이닝할 수 없으므로 각 메서드의 반환값을 확인해야 한다.
* **해시 테이블**은 키를 해시 함수로 변환한 값을 인덱스로 사용해 빠른 저장·검색을 제공하며, 파이썬의 `set`/`dict`가 이를 기반으로 동작한다. **hashable**(hash() 적용 가능) 객체는 대부분 불변 타입이며, 가변 객체(list, set, dict)는 값이 바뀌면 해시 값의 일관성이 깨지므로 hashable하지 않다.
* 파이썬 공식 문서는 **BNF/EBNF** 표기법(`[]`는 선택적 요소, `{}`는 0번 이상 반복, `()`는 그룹화)으로 문법을 정의하며, 이를 이해하면 공식 문서의 함수 시그니처(예: `pop(key[, default])`)를 정확히 해석할 수 있다.
