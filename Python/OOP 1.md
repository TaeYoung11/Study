# Python OOP 1 정리

**프로그래밍 패러다임**(절차 지향 vs 객체 지향), **객체와 클래스**의 개념, 클래스의 구성요소(클래스 변수/인스턴스 변수), 그리고 **메서드**(인스턴스/클래스/스태틱 메서드)를 정리한 문서입니다.

---

## 1. 프로그래밍 패러다임

### 절차 지향 프로그래밍(Procedural Programming)
* 프로그램을 함수와 로직(절차) 중심으로 작성
* 데이터를 함수에 전달하며 순차적으로 처리

### 절차 지향 사고 예시 — 변수와 함수를 별개로 다룸
```python
name = 'Alice'
age = 25

def introduce(name, age):
    print(f'안녕하세요, {name}입니다. 나이는 {age}살입니다.')

introduce(name, age)
```

### 절차 지향 프로그래밍 특징
* 데이터를 다시 재사용하거나 하기보다는 처음부터 끝까지 실행되는 결과물이 중요

### 절차 지향적 프로그래밍의 한계
1. **복잡성 증가:** 프로그램 규모가 커질수록 데이터와 함수의 관리가 어려움, 전역 변수의 증가로 인한 관리의 어려움
2. **유지보수 문제:** 코드 수정 시 영향 범위 파악이 어려움

### 객체 지향 프로그래밍(Object Oriented Programming)
* 데이터와 함수를 하나의 단위(객체)로 묶어서 관리
* 객체들을 조합하고 재활용하는 방식으로 프로그램 구성

### 객체 지향 사고 예시 — 사람(객체) 안에 name, age와 이와 관련된 기능(메서드) 포함
```python
class Person:
    def __init__(self, name, age):
        self.name = name
        self.age = age

    def introduce(self):
        print(f'안녕하세요, {self.name}입니다. 나이는 {self.age}살입니다.')

alice = Person('Alice', 25)
alice.introduce()  # 객체가 자신의 정보를 출력
```

### 객체 지향 프로그래밍 특징
* 프로그램을 데이터(변수)와 그 데이터를 처리하는 함수(메서드)를 하나의 단위(객체)로 묶어서 조직적으로 관리
* 데이터와 메서드의 결합
* 주방 도구(프라이팬, 칼), 재료(야채, 고기), 행동(볶기, 썰기)를 각각 별개로 생각하지 않고, "볶음밥 기계"라는 객체로 만들어 놓고 그 기계가 알아서 해당 행동과 재료를 관리하는 방식

### 절차 지향 & 객체 지향
* **절차 지향:** 데이터와 해당 데이터를 처리하는 함수(절차)가 분리. 함수 호출의 흐름이 중요. "어떤 순서로 처리할까?"
* **객체 지향:** 데이터와 해당 데이터를 처리하는 메서드(함수)를 하나의 객체(클래스)로 묶음. 객체 간 상호작용과 메시지 전달이 중요. "어떤 객체가 이 문제를 해결할까?", "이 객체는 어떤 속성과 기능을 가질까?"

### 객체지향 — "데이터가 살아나다"
* 객체 지향은 수동적인 데이터가 능동적인 객체로 변화한 것
* 절차 지향에서는 데이터가 함수의 매개변수로 전달되어 처리되는 수동적 존재였지만, 객체 지향에서는 데이터와 해당 데이터를 처리하는 메서드가 하나의 객체로 통합되어 스스로 기능을 수행하는 능동적 존재가 됨
* 이는 코드의 구조화와 재사용성을 높이는 동시에, 실제 세계의 모델링 방식과 더 유사한 프로그래밍을 가능하게 함

### 절차 지향과 객체 지향은 대조되는 개념이 아니다
* 객체 지향은 기존 절차 지향을 기반으로 두고 보완하기 위해 객체라는 개념을 도입해 상속, 코드 재사용성, 유지보수성 등의 이점을 가지는 패러다임

---

## 2. 객체와 클래스

### 객체(Object)
* 실제 존재하는 사물을 추상화한 것
* "속성"과 "동작"을 가짐
* 예) "강아지"라는 객체는 이름, 품종, 나이(특징)와 짖기, 뛰기(행동) 등으로 표현할 수 있음

### 클래스(Class)
* 객체를 만들기 위한 설계도
* 데이터와 기능을 함께 묶는 방법을 제공
* 파이썬에서 타입을 표현하는 방법
* 클래스로부터 여러 개의 객체를 쉽게 찍어낼 수 있음
* 예) "강아지"의 이름, 품종, 나이 같은 데이터를 어떻게 표현할 지(속성)와 짖기, 달리기 같은 기능(메서드)을 정의

### 객체 예시 — 가수(속성/동작)
* **속성(정보):** 직업(가수), 생년월일(1993년 5월 16일), 국적(대한민국)
* **동작(행동):** 랩(), 댄스(), 바이브레이션()

### 객체와 클래스 — 클래스(가수)로부터 객체(아이유, BTS...)가 만들어짐

### 객체 특징
* **속성(Attribute):** 객체의 상태/데이터
* **메서드(Method):** 객체의 행동/기능
* **고유성:** 각 객체는 고유한 특성을 가짐

---

## 3. 클래스 기초

### 클래스(Class)
* 데이터와 기능을 하나의 틀로 묶어 관리하는 방법
* 사용자 정의 객체를 만드는 수단이자 속성과 메서드를 정의

### 클래스 정의
* `class` 키워드
* 클래스 이름은 파스칼 케이스(Pascal Case) 방식으로 작성

```python
class MyClass:
    pass
```

### 클래스 예시
* 데이터와 기능을 하나의 틀로 묶어 관리하는 방법
* `__init__` 메서드는 '생성자 메서드'로 불리며, 새로운 객체를 만들 때 필요한 초기값을 설정(메서드 챕터에서 진행)

```python
class Person:
    def __init__(self, name, age):
        self.name = name  # 인스턴스 속성
        self.age = age    # 인스턴스 속성

    def introduce(self):
        print(f'안녕하세요. 저는 {self.name}, 나이는 {self.age}살입니다.')
```

### 인스턴스(Instance)
* 클래스를 통해 생성된 객체

### 인스턴스 예시
* 클래스가 설계도라면, 인스턴스는 그 설계도로부터 실제로 만든 '개별 물건'
* `Person("Alice", 25)`라고 하면 Person이라는 설계도로부터 이름이 Alice이고, 나이가 25인 '사람 객체'가 탄생

```python
p1 = Person('Alice', 25)
p1.introduce()  # "안녕하세요. 저는 Alice, 나이는 25살입니다."

p2 = Person('Bella', 30)
p2.introduce()  # "안녕하세요. 저는 Bella, 나이는 30살입니다."
```
* 아이유는 객체다. (O) / 아이유는 인스턴스다. (△, 문맥에 따라) / 아이유는 가수의 인스턴스다. (O)

### 클래스와 인스턴스
* 변수 `name`의 타입은 `str` 클래스다.
* 변수 `name`은 `str` 클래스의 인스턴스이다.
* 우리가 사용해왔던 데이터 타입은 사실 모두 클래스였다. => 결국 클래스를 만든다 == 타입을 만든다.

```python
name = 'Alice'
print(type(name))  # <class 'str'>
```

* 결국 문자열 타입의 변수는 `str` 클래스로 생성된 인스턴스다.

```python
print(help(str))
"""
class str(object)
 |  str(object='') -> str
 |  str(bytes_or_buffer[, encoding[, errors]]) -> str
 |
 |  Create a new string object from the given object. If encoding or
 |  errors is specified, then the object must expose a data buffer
 |  that will be decoded using the given encoding and error handler.
 |  Otherwise, ...
"""
```

### 클래스 — 문자열 타입 / 리스트 타입
* 문자열 타입(클래스)의 객체(인스턴스) 예) `''`, `'hello'`, `'파이썬'` — `"hello".upper()` => 문자열.대문자로() == 객체.행동() == 인스턴스.메서드()
* 리스트 타입(클래스)의 객체(인스턴스) 예) `[1, 2, 3]`, `[1]`, `[]`, `['hi']` — `[1, 2, 3].sort()` => 리스트.정렬해() == 객체.행동() == 인스턴스.메서드()

### 하나의 객체(object)는 특정 클래스의 인스턴스(instance)이다.
* `123`, `900`, `5`는 클래스 `int`의 인스턴스
* `'hello'`, `'bye'`는 클래스 `str`의 인스턴스
* `[232, 89, 1]`, `[]`은 클래스 `list`의 인스턴스

---

## 4. 클래스 구성요소

### 클래스 구조
* **생성자 메서드:** 인스턴스 생성 시 자동 호출되는 특별한 메서드. `__init__`이라는 이름의 메서드로 정의. 인스턴스 변수의 초기화 담당

```python
class Circle:
    pi = 3.14

    def __init__(self, radius):
        self.radius = radius

# 인스턴스 생성
c1 = Circle(1)
c2 = Circle(2)
```

### 인스턴스 변수(속성)
* 각 인스턴스별 고유한 속성 정의
* `self.변수명` 형태로 정의
* 인스턴스마다 독립적인 값 유지

```python
# 인스턴스 변수(속성)
print(c1.radius)  # 1
print(c2.radius)  # 2
```

### 클래스 변수(속성)
* 모든 인스턴스가 공유하는 속성
* 클래스 내부에서 직접 정의

```python
class Circle:
    pi = 3.14  # 클래스 변수(속성)

    def __init__(self, radius):
        self.radius = radius

# 클래스 변수(속성)
print(c1.pi)  # 3.14
print(c2.pi)  # 3.14
```

### 클래스 변수와 인스턴스 변수
* 클래스 변수와 동일한 이름으로 인스턴스 변수 생성 시 클래스 변수가 아닌 인스턴스 변수를 먼저 참조하게 됨
* `class.class_variable`로 클래스 변수 참조 가능

```python
class Circle:
    pi = 3.14

    def __init__(self, radius):
        self.radius = radius

c1 = Circle(5)
c2 = Circle(10)
print(c1.radius)  # 5
print(c2.radius)  # 10

# c1의 인스턴스 변수 pi를 생성
c1.pi = 100
print(c1.pi)      # 100
print(Circle.pi)  # 3.14

# c2는 인스턴스 변수 pi가 없으므로 클래스 변수 pi를 참조
print(c2.pi)      # 3.14
```

---

## 5. 메서드(Method)

### 메서드란?
* 클래스 내부에 정의된 함수로, 해당 객체가 어떻게 동작할지를 정의
* **메서드 종류:** 1. 인스턴스 메서드 / 2. 클래스 메서드 / 3. 스태틱 메서드

### 인스턴스 메서드(instance method)
* 클래스로부터 생성된 각 인스턴스에서 호출할 수 있는 메서드
* 인스턴스의 상태를 조작하거나 동작을 수행

### 인스턴스 메서드 구조
* 클래스 내부에 정의되는 메서드의 기본
* 반드시 첫 번째 인자로 인스턴스 자신(self)을 받음
* 인스턴스의 속성에 접근하거나 변경 가능

```python
class MyClass:
    def instance_method(self, arg1, ...):
        pass
```
* `self`는 매개변수 이름일 뿐이며 다른 이름으로 설정 가능하지만 다른 이름을 사용하지 않을 것을 강력히 권장

### self 동작 원리
* `upper` 메서드를 사용해 문자열 `'hello'`를 대문자로 변경하기: `'hello'.upper()`
* 하지만 실제 파이썬 내부 동작은 다음과 같이 진행됨: `str.upper('hello')`
* `str` 클래스가 `upper` 메서드를 호출했고, 그 첫번째 인자로 문자열 인스턴스가 들어간 것

=> 인스턴스 메서드의 첫번째 인자가 반드시 인스턴스 자기 자신인 이유

* 객체 지향 방식의 메서드로 호출하는 표현(단축형 호출): `'hello'.upper()` == `str.upper('hello')`
* `'hello'`라는 문자열 객체가 단순히 어딘가의 함수로 들어가는 인자로 활용되는 것이 아닌 객체 스스로 메서드를 호출하여 코드를 동작하는 객체 지향적인 표현인 것

### 인스턴스 메서드 활용
```python
class Counter:
    def __init__(self):
        self.count = 0

    def increment(self):
        self.count += 1

c = Counter()
c.increment()
print(c.count)  # 1
```

### 생성자 메서드(constructor method)
* 인스턴스 객체가 생성될 때 자동으로 호출되는 메서드
* 인스턴스 변수들의 초기값을 설정

```python
class Person:
    def __init__(self, name):
        # 왼쪽 name: 인스턴스 변수 name
        # 오른쪽 name: 생성자 메서드의 매개변수 이름
        self.name = name
        print('인스턴스가 생성되었습니다.')

    def greeting(self):
        print(f'안녕하세요 {self.name}입니다.')

person1 = Person('지민')  # 인스턴스가 생성되었습니다.
person1.greeting()        # 안녕하세요 지민입니다.
# Person.greeting(person1)
```

### 클래스 메서드(class method)
* 클래스가 호출하는 메서드
* 클래스 변수를 조작하거나 클래스 레벨의 동작을 수행

### 클래스 메서드 구조
* `@classmethod` 데코레이터를 사용하여 정의
* 호출 시, 첫번째 인자로 해당 메서드를 호출하는 클래스(cls)가 전달됨
* 클래스를 인자로 받아 클래스 속성을 변경하거나 읽는 데 사용

```python
class MyClass:
    @classmethod
    def class_method(cls, arg1, ...):
        pass
```
* `cls`는 매개변수 이름일 뿐이며 다른 이름으로 설정 가능하지만 다른 이름을 사용하지 않을 것을 강력히 권장

### 클래스 메서드 활용
```python
class Person:
    population = 0

    def __init__(self, name):
        self.name = name
        Person.increase_population()

    @classmethod
    def increase_population(cls):
        cls.population += 1

person1 = Person('Alice')
person2 = Person('Bella')
print(Person.population)  # 2
```

### 스태틱(정적) 메서드(static method)
* 클래스, 인스턴스와 상관없이 독립적으로 동작하는 메서드

### 스태틱 메서드 구조
* `@staticmethod` 데코레이터를 사용하여 정의
* 호출 시 자동으로 전달 받는 인자가 없음(self, cls를 받지 않음)
* 인스턴스나 클래스 속성에 직접 접근하지 않는 '도우미 함수'와 비슷한 역할

```python
class MyClass:
    @staticmethod
    def static_method(arg1, ...):
        pass
```

### 스태틱 메서드 활용 — 수학에 관련 기능을 담은 MathUtils 클래스에서 덧셈 기능을 제공하는 예시
```python
class MathUtils:
    @staticmethod
    def add(a, b):
        return a + b

print(MathUtils.add(3, 5))  # 8
```

### 입출금이 가능한 은행 계좌 클래스 만들기
```python
class BankAccount:
    interest_rate = 0.02  # 이자율

    def __init__(self, owner, balance=0):
        self.owner = owner        # 계좌 소유자
        self.balance = balance    # 초기 잔액

    # 입금
    def deposit(self, amount):
        self.balance += amount

    # 출금
    def withdraw(self, amount):
        if self.balance >= amount:
            self.balance -= amount
        else:
            print('잔액 부족!')

    # 이자율 설정
    @classmethod
    def set_interest_rate(cls, rate):
        cls.interest_rate = rate

    # 금액이 양수인지 검증
    @staticmethod
    def is_positive(amount):
        return amount > 0
```

### 입출금이 가능한 은행 계좌 클래스 활용
```python
# 계좌 개설(인스턴스 생성)
alice_acc = BankAccount('Alice', 1000)

# 입금 및 출금(인스턴스 메서드 호출)
alice_acc.deposit(500)
alice_acc.withdraw(200)

# 잔액 확인(인스턴스 변수 참조)
print(alice_acc.balance)  # 1300

# 이자율 변경(클래스 메서드 호출)
BankAccount.set_interest_rate(0.03)
print(BankAccount.interest_rate)  # 0.03

# 잔액이 양수인지 확인(정적 메서드 호출)
print(BankAccount.is_positive(alice_acc.balance))  # True
```

### 메서드 정리
* **인스턴스 메서드:** 인스턴스의 상태를 변경하거나, 해당 인스턴스의 특정 동작을 수행
* **클래스 메서드:** 인스턴스의 상태에 의존하지 않는 기능을 정의. 클래스 변수를 조작하거나 클래스 레벨의 동작을 수행
* **스태틱 메서드:** 클래스 및 인스턴스와 관련이 없는 일반적인 기능을 수행

### 누가 어떤 메서드를 사용해야 할까?
* **클래스가 사용해야 할 것:** 클래스 메서드, 스태틱 메서드
* **인스턴스가 사용해야 할 것:** 인스턴스 메서드

**예시 클래스로 클래스와 인스턴스가 각각 모든 메서드를 호출해보기**
```python
class MyClass:
    def instance_method(self):
        return 'instance method', self

    @classmethod
    def class_method(cls):
        return 'class method', cls

    @staticmethod
    def static_method():
        return 'static method'
```

### 클래스가 할 수 있는 것
* 클래스는 모든 메서드를 호출 할 수 있음
* 하지만 클래스는 클래스 메서드와 스태틱 메서드만 사용하도록 한다.

```python
instance = MyClass()

print(MyClass.instance_method(instance))
# ('instance method', <__main__.MyClass object at0x_028F10>)
print(MyClass.class_method())     # ('class method', <class '__main__.MyClass'>)
print(MyClass.static_method())    # static method
```

### 인스턴스가 할 수 있는 것
* 인스턴스는 모든 메서드를 호출 할 수 있음
* 하지만 인스턴스는 인스턴스 메서드만 사용하도록 한다.

```python
instance = MyClass()

print(instance.instance_method())
# ('instance method', <__main__.MyClass object at 0x0000_84EAF10>)
print(instance.class_method())    # ('class method', <class '__main__.MyClass'>)
print(instance.static_method())   # static method
```

### 할 수 있다 != 써도 된다
* 각자의 메서드는 OOP 패러다임에 따라 명확한 목적에 따라 설계된 것이기 때문에 클래스와 인스턴스 각자 올바른 메서드만 사용한다.

---

## 6. 참고 — 클래스와 인스턴스 간 이름 공간

### 클래스와 인스턴스 간의 이름 공간
* 클래스를 정의하면, 클래스와 해당하는 이름 공간 생성
* 인스턴스를 만들면, 인스턴스 객체가 생성되고 **독립적인** 이름 공간 생성
* 인스턴스에서 특정 속성에 접근하면, 인스턴스 → 클래스 순으로 탐색

```python
class Person:
    blood_color = 'red'

    def __init__(self, name):
        self.name = name

    def talk(self):
        print(self.name)

p1 = Person('unknown')
p1.talk()  # p1은 인스턴스 변수가 정의되어 있지 않아 클래스 변수(unknown)가 출력됨

# p2 인스턴스 변수 설정 전/후
p2 = Person()  # unknown
p2.name = 'Kim'
p2.talk()  # Kim

print(Person.name)  # unknown (클래스 변수 name 값이 Kim으로 변경된 것이 아닌 p2 인스턴스의 인스턴스 변수 name이 Kim으로 저장됨)
print(p2.name)       # Kim
```

### 독립적인 이름공간을 가지는 이점
* 각 인스턴스는 독립적인 메모리 공간을 가지며, 클래스와 다른 인스턴스 간에는 서로의 데이터나 상태에 직접적인 접근이 불가능
* 객체 지향 프로그래밍의 중요한 특성 중 하나로, 클래스와 인스턴스를 모듈화하고 각각의 객체가 독립적으로 동작하도록 보장
* 이를 통해 클래스와 인스턴스는 다른 객체들과의 상호작용에서 서로 충돌이나 영향을 주지 않으면서 독립적으로 동작할 수 있음
* 코드의 가독성, 유지보수성, 재사용성을 높이는데 도움을 줌

---

## 7. 참고 — 매직 메서드와 데코레이터

### 매직 메서드(magic method)
* Double underscore(`__`)가 있는 메서드는 특수한 동작을 위해 만들어진 메서드
* 인스턴스 메서드
* 특정 상황에 자동으로 호출됨
* 스페셜 메서드 혹은 매직 메서드라고 불림
* 예시: `__str__(self)`, `__len__(self)`, `__lt__(self, other)`, `__le__(self, other)`, `__eq__(self, other)`, `__gt__(self, other)`, `__ge__(self, other)`, `__ne__(self, other)`

### 매직 메서드 `__str__` 예시
* `__str__(self)`: 내장함수 `print`에 의해 호출되어 객체 출력을 문자열 표현으로 변경

```python
class Circle:
    def __init__(self, radius):
        self.radius = radius

    def __str__(self):
        return f'원의 반지름: {self.radius}'

c1 = Circle(10)
c2 = Circle(1)

print(c1)  # 원의 반지름: 10
print(c2)  # 원의 반지름: 1
```

### 데코레이터(Decorator)
* 다른 함수의 코드를 유지한 채로 수정하거나 확장하기 위해 사용되는 함수

**데코레이터 정의**
```python
def my_decorator(func):
    def wrapper():
        # 함수 실행 전에 수행할 작업
        print('함수 실행 전')
        # 원본 함수 호출
        result = func()
        # 함수 실행 후에 수행할 작업
        print('함수 실행 후')
        return result
    return wrapper
```

**데코레이터 사용**
```python
@my_decorator
def my_function():
    print('원본 함수 실행')

my_function()
# 함수 실행 전
# 원본 함수 실행
# 함수 실행 후
```

---

## 핵심 요약
* **절차 지향**은 데이터와 함수(절차)가 분리되어 순차적으로 실행되는 방식이고, **객체 지향**은 데이터와 이를 처리하는 메서드를 하나의 객체(클래스)로 묶어 관리하는 방식으로, 둘은 대조 개념이 아니라 객체 지향이 절차 지향을 보완하는 관계다.
* **클래스**는 객체를 만드는 설계도이고, **인스턴스**는 클래스로부터 생성된 실제 객체이며, 파이썬에서 사용해온 모든 데이터 타입(int, str, list 등)도 사실 클래스이고 그 값들은 해당 클래스의 인스턴스다.
* **클래스 변수**는 모든 인스턴스가 공유하는 속성이고, **인스턴스 변수**(`self.변수명`)는 각 인스턴스별 고유한 속성이며, 클래스 변수와 동일한 이름의 인스턴스 변수가 생기면 인스턴스 변수가 우선 참조된다.
* 메서드는 **인스턴스 메서드**(첫 인자 `self`, 인스턴스 상태 조작), **클래스 메서드**(`@classmethod`, 첫 인자 `cls`, 클래스 변수 조작), **스태틱 메서드**(`@staticmethod`, 인자 없음, 독립적인 도우미 함수)로 나뉘며, 클래스는 클래스/스태틱 메서드를, 인스턴스는 인스턴스 메서드를 사용하는 것이 원칙이다(호출 자체는 가능해도 지켜야 할 규칙).
* 클래스와 인스턴스는 각각 독립적인 **이름 공간(namespace)**을 가지며, 인스턴스에서 속성을 조회할 때는 인스턴스 → 클래스 순서로 탐색한다.
* **매직 메서드**(`__str__` 등, double underscore)는 특정 상황에 자동으로 호출되는 특수 메서드이고, **데코레이터**는 원본 함수의 코드를 유지한 채 기능을 확장하는 함수(`@classmethod`, `@staticmethod`도 데코레이터의 일종)다.
