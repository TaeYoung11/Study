# Python OOP 2 & Exception 정리

**상속**(클래스 상속, 다중 상속, MRO, super()), 클래스의 실전 활용, 그리고 **에러와 예외**(내장 예외, try-except, else/finally)를 정리한 문서입니다.

---

## 1. 상속(Inheritance)

### 상속이란?
* 한 클래스(부모)의 속성과 메서드를 다른 클래스(자식)가 물려받는 것

### 상속이 필요한 이유
1. **코드 재사용:** 상속을 통해 기존 클래스의 속성과 메서드를 재사용할 수 있음. 기존 클래스를 수정하지 않고도 기능을 확장할 수 있음
2. **계층 구조:** 상속을 통해 클래스들 간의 계층 구조를 형성할 수 있음. 부모 클래스와 자식 클래스 간의 관계를 표현하고, 더 구체적인 클래스를 만들 수 있음 (예: Motor Vehicle → Bus, Truck, Car)
3. **유지 보수의 용이성:** 상속을 통해 기존 클래스의 수정이 필요할 경우, 해당 클래스만 수정하면 되므로 유지보수가 용이해짐. 코드의 일관성을 유지하고, 수정이 필요한 범위를 최소화할 수 있음

### 상속 예시 — 캐릭터
* 클래스(속성, 메서드)를 기반으로 전사(힘, 명예, 베기, 찌르기), 마법사(마력, 순간이동, 회복) 등 하위 클래스로 확장

```python
class Animal:
    def eat(self):
        print('먹는 중')

class Dog(Animal):
    def bark(self):
        print('멍멍')

my_dog = Dog()
my_dog.bark()  # 멍멍

# 부모 클래스(Animal) 메서드 사용 가능
my_dog.eat()  # 먹는 중
```

### 상속 없이 구현하는 경우 — 학생/교수 정보를 별도로 표현하기 어려움
```python
class Person:
    def __init__(self, name, age):
        self.name = name
        self.age = age

    def talk(self):
        print(f'반갑습니다. {self.name}입니다.')

s1 = Person('김학생', 23)
s1.talk()  # 반갑습니다. 김학생입니다.

p1 = Person('박교수', 59)
p1.talk()  # 반갑습니다. 박교수입니다.
```

* 교수/학생 클래스로 분리 했지만 메서드가 중복으로 정의될 수 있음

```python
class Professor:
    def __init__(self, name, age, department):
        self.name = name
        self.age = age
        self.department = department

    def talk(self):  # 중복
        print(f'반갑습니다. {self.name}입니다.')

class Student:
    def __init__(self, name, age, gpa):
        self.name = name
        self.age = age
        self.gpa = gpa

    def talk(self):  # 중복
        print(f'반갑습니다. {self.name}입니다.')
```

### 상속을 사용한 계층구조 변경
```python
class Person:
    def __init__(self, name, age):
        self.name = name
        self.age = age

    def talk(self):  # 메서드 재사용
        print(f'반갑습니다. {self.name}입니다.')

class Professor(Person):
    def __init__(self, name, age, department):
        self.name = name
        self.age = age
        self.department = department

class Student(Person):
    def __init__(self, name, age, gpa):
        self.name = name
        self.age = age
        self.gpa = gpa

p1 = Professor('박교수', 49, '컴퓨터공학과')
s1 = Student('김학생', 20, 3.5)

# 부모 Person 클래스를 활용
p1.talk()  # 반갑습니다. 박교수입니다.
s1.talk()  # 반갑습니다. 김학생입니다.
```

### 메서드 오버라이딩(Method Overriding)
* 자식 클래스가 부모 클래스의 메서드를 재정의

```python
class Animal:
    def eat(self):
        print('먹는 중')

class Dog(Animal):
    # 부모 클래스의 메서드를 재정의
    def eat(self):
        print('멍멍')

my_dog = Dog()
my_dog.eat()  # 멍멍
```

### 다중 상속
* 둘 이상의 상위 클래스로부터 여러 행동이나 특징을 상속받을 수 있는 것
* 상속받은 모든 클래스의 요소를 활용 가능
* 중복된 속성이나 메서드가 있는 경우 **상속 순서에 의해 결정됨**

### 다중 상속 예시
```python
class Person:
    def __init__(self, name):
        self.name = name

    def greeting(self):
        return f'안녕, {self.name}'

class Mom(Person):
    gene = 'XX'

    def swim(self):
        return '엄마가 수영'

class Dad(Person):
    gene = 'XY'

    def walk(self):
        return '아빠가 걷기'

class FirstChild(Dad, Mom):
    def swim(self):
        return '첫째가 수영'

    def cry(self):
        return '첫째가 응애'

baby1 = FirstChild('아기')
print(baby1.cry())    # 첫째가 응애
print(baby1.swim())   # 첫째가 수영 (FirstChild에서 재정의)
print(baby1.walk())   # 아빠가 걷기
print(baby1.gene)     # XY (Dad, Mom 순서로 상속했으므로 Dad의 gene이 우선)
```
* `class FirstChild(Dad, Mom)`처럼 다중 상속 순서에 따라, `Dad`와 `Mom` 모두에게 있는 속성/메서드는 먼저 명시된 `Dad`의 것이 채택됨

### 다이아몬드 문제(The diamond problem)
* 두 클래스 B와 C가 A에서 상속되고 클래스 D가 B와 C 모두에게서 상속될 때 발생하는 모호함
* B와 C가 재정의한 메서드가 A에 있고 D가 이를 재정의하지 않은 경우라면
=> D는 B의 메서드 중 어떤 버전을 상속하는가? 아니면 C의 메서드 버전을 상속하는가?

```
      A
    /   \
   B     C
    \   /
      D
```
```python
class D(B, C):
    pass
```

### 파이썬에서의 해결책 — MRO(Method Resolution Order)
* MRO(Method Resolution Order) 알고리즘을 사용하여 클래스 목록을 생성
* 부모 클래스로부터 상속된 속성들의 검색을 깊이 우선으로, 왼쪽에서 오른쪽으로, 계층 구조에서 겹치는 같은 클래스를 두 번 검색하지 않음
* 그래서, 속성이 D에서 발견되지 않으면, B에서 찾고, 거기에서도 발견되지 않으면, C에서 찾고, 이런 식으로 진행됨
* **MRO(Method Resolution Order):** 파이썬이 메서드를 찾는 순서에 대한 규칙(메서드 결정 순서)

---

## 2. super() 메서드

### super()란?
* 부모 클래스(또는 상위 클래스)의 메서드를 호출하기 위해 사용하는 내장 함수
* 다중 상속 상황에서 특히 유용하며, MRO를 따르기 때문에 여러 부모 클래스를 가진 자식 클래스에서 다음에 호출해야 할 부모 메서드를 순서대로 호출할 수 있게 함

### super의 2가지 사용 사례
1. **단일 상속 구조:** 명시적으로 이름을 지정하지 않고 부모 클래스를 참조할 수 있으므로, 코드를 더 유지 관리하기 쉽게 만들 수 있음. 클래스 이름이 변경되거나 부모 클래스가 교체되어도 super()를 사용하면 코드 수정이 더 적게 필요
2. **다중 상속 구조:** MRO를 따른 메서드 호출. 복잡한 다중 상속 구조에서 발생할 수 있는 문제를 방지

### super의 사용 예시(단일 상속)
```python
# 사용 전
class Person:
    def __init__(self, name, age, number, email):
        self.name = name
        self.age = age
        self.number = number
        self.email = email

class Student(Person):
    def __init__(self, name, age, number, email, student_id):
        self.name = name
        self.age = age
        self.number = number
        self.email = email
        self.student_id = student_id
```
```python
# 사용 후
class Person:
    def __init__(self, name, age, number, email):
        self.name = name
        self.age = age
        self.number = number
        self.email = email

class Student(Person):
    def __init__(self, name, age, number, email, student_id):
        # super()를 통해 Person의 __init__ 메서드 호출
        super().__init__(name, age, number, email)
        self.student_id = student_id
```
* `Student`의 생성자에서 `super().__init__()`를 호출하면, `Person`의 `__init__()` 메서드가 호출되어 name, age, number, email 속성을 초기화한 뒤 Student 고유의 student_id 속성을 추가
* 이때 `Person` 클래스를 직접 명시하지 않고 `super()`를 사용하므로, 나중에 `Person` 클래스 이름이 바뀌거나 상속 구조가 변경되어도 `super()` 호출 부분을 그대로 사용할 수 있어 유지보수성이 향상

### super의 사용 예시(다중 상속)
```python
class ParentA:
    def __init__(self):
        self.value_a = 'ParentA'

    def show_value(self):
        print(f'Value from ParentA: {self.value_a}')

class ParentB:
    def __init__(self):
        self.value_b = 'ParentB'

    def show_value(self):
        print(f'Value from ParentB: {self.value_b}')

class Child(ParentA, ParentB):
    def __init__(self):
        super().__init__()  # ParentA 클래스의 __init__ 메서드 호출
        self.value_c = 'Child'

    def show_value(self):
        super().show_value()  # ParentA 클래스의 show_value 메서드 호출
        print(f'Value from Child: {self.value_c}')

child = Child()
child.show_value()
"""
Value from ParentA: ParentA
Value from Child: Child
"""
print(child.value_c)  # Child
print(child.value_a)  # ParentA
```
1. `Child` 클래스는 `ParentA`, `ParentB`를 순서대로 상속
2. `child = Child()`를 실행하면 `Child`의 init 메서드에서 `super().__init__()`를 호출
3. MRO에 의해 `Child → ParentA → ParentB` 순으로 메서드를 찾는데, 이 상황에서 `super().__init__()`는 바로 다음 순서에 해당하는 `ParentA`의 init을 호출
4. `ParentA`의 init이 실행되어 `value_a`가 자동으로 초기화되지 않음. 만약 `ParentA`의 init 안에서도 `super().__init__()`를 호출한다면, 그 다음으로 `ParentB`의 init이 실행되어 `value_b`도 초기화될 수 있음(이렇게 여러 부모 클래스의 초기화가 순서대로 이루어질 수 있음)
5. `child.show_value()`를 호출하면 `Child`의 `show_value`에서 `super().show_value()`를 호출
6. `show_value()` 메서드를 찾기 위해 `Child → ParentA → ParentB` 순서로 탐색하므로, 첫 번째로 `ParentA`의 `show_value()`가 실행됨

### super의 이점
* 다중 상속 상황에서 `super()`는 다음에 호출해야 할 부모 메서드를 MRO 순서에 따라 결정하기 때문에, 명시적으로 특정 부모 클래스를 가리키지 않고도 올바른 순서로 부모 초기화나 메서드 호출이 가능
* 이를 통해 복잡한 상속 구조에서도 코드를 유연하고 깔끔하게 유지할 수 있음

### super 정리
* `super()`를 사용할 때는 MRO를 잘 이해하고 있어야 함
* `ClassName.__mro__` 또는 `ClassName.mro()`를 확인해 MRO 순서를 파악한 뒤 적절히 활용하는 연습을 하면, 보다 복잡한 상속 구조에서도 코드를 잘 관리할 수 있음

### mro(), \_\_mro\_\_ 사용 예시
```python
class A:
    def __init__(self):
        print('A Constructor')

class B(A):
    def __init__(self):
        super().__init__()
        print('B Constructor')

class C(A):
    def __init__(self):
        super().__init__()
        print('C Constructor')

class D(B, C):
    def __init__(self):
        super().__init__()
        print('D Constructor')

print(D.mro())
# [<class '__main__.D'>, <class '__main__.B'>, <class '__main__.C'>, <class '__main__.A'>, <class 'object'>]
print(D.__mro__)
# (<class '__main__.D'>, <class '__main__.B'>, <class '__main__.C'>, <class '__main__.A'>, <class 'object'>)
```

### MRO가 필요한 이유
* 부모 클래스들이 여러 번 액세스 되지 않도록, 각 클래스에서 지정된 왼쪽에서 오른쪽으로 가는 순서를 보존하고, 각 부모를 오직 한번만 호출하고, 부모들의 우선순위에 영향을 주지 않으면서 서브 클래스를 만드는 단조적인 구조 형성
=> 프로그래밍 언어의 신뢰성 있고 확장성 있는 클래스를 설계할 수 있도록 도움
=> 클래스 간의 메서드 호출 순서가 예측 가능하게 유지되며, 코드의 재사용성과 유지보수성이 향상

---

## 3. 클래스의 의미와 활용

### 왜 클래스를 배웠을까?
* 지금까지 우리는 변수와 함수만으로도 간단한 프로그램을 만들 수 있었습니다.
* 그러나 프로그램 규모가 커지면 서로 관련 있는 정보와 기능을 따로따로 관리하기가 점점 어려워집니다.
* 클래스를 사용하면 관련된 데이터와 기능을 '한 덩어리'로 묶어 구조를 명확히 할 수 있습니다.
* 이로써 작성한 코드가 훨씬 깔끔해지고, 나중에 수정하거나 기능을 추가할 때 더 쉽고 안전해집니다.

### 실제 개발 상황 속 클래스
* 예시: 도서 관리 프로그램
  * 책을 나타내는 클래스를 만들고(title, author, price 같은 속성과 print_info() 같은 기능), 이 클래스를 이용해 여러 권의 책 객체를 다룰 수 있음
  * 이렇게 하면 책 관련 코드와 로직이 한 곳에 모여 있어, 재사용과 관리가 편해짐

### 알고리즘 문제풀이와 OOP
* 알고리즘 문제는 보통 "입력 받고 → 계산한 뒤 → 결과 출력"하는 짧고 단순한 구조
* 클래스가 없이도 문제 해결이 충분히 가능하며 그래서 현재 단계에서 알고리즘 문제를 풀 때 클래스가 크게 필요하지 않을 수 있음
* 하지만 현실의 문제는 훨씬 복잡하며, 나중에 여러분이 여러 사람이 함께 작업하는 큰 프로젝트를 하거나, 데이터와 기능이 복잡하게 얽힌 프로그램을 만들게 되면, 클래스를 통해 구조를 잘 짜는 것이 필수가 됨
* 그러면 프로그램을 이해하기도 쉽고, 오류를 찾거나 기능을 개선하는 데에도 훨씬 유리
* **정리하자면,** 지금 당장 알고리즘 문제에는 클래스가 안 쓰일 수도 있지만, 앞으로 더 복잡한 프로그램을 만들 때는 객체 지향 개념이 여러분을 도와 큰 그림을 잡고 효율적으로 코드를 관리하게 해줄 것
* 클래스는 앞으로 여러분이 더 큰 세계의 코딩 작업에 나아갈 때 튼튼한 기둥 역할을 할 것. 지금은 예제가 단순하고, 당장 활용하지 않을 수도 있지만, 미래에 실제 웹 서비스나 큰 규모 프로젝트를 다룰 때 OOP 개념이 크게 빛을 발할 것

---

## 4. 에러와 예외 — 버그와 디버깅

### 버그(bug)
* 소프트웨어에서 발생하는 오류 또는 결함
* 프로그램의 예상된 동작과 실제 동작 사이의 불일치
* **버그의 기원:** 최초의 버그는 1945년 프로그래밍 언어의 일종인 코볼 발명자 그레이스 호퍼가 발견. 역사상 최초의 컴퓨터 버그는 Mark Ⅱ라는 컴퓨터 회로에 벌레(나방)가 들어가 합선을 일으켜 비정상적으로 동작한 것을 기록한 것. "버그"라는 용어는 이전부터 사용되어 왔지만 이 사건을 계기로 컴퓨터 시스템에서 발생하는 오류 또는 결함을 지칭하는 용어로 널리 사용되기 시작

### 디버깅(Debugging)
* 소프트웨어에서 발생하는 버그를 찾아내고 수정하는 과정
* 프로그램의 오작동 원인을 식별하여 수정하는 작업
* 말 그대로 벌레(버그)를 제거하는 작업

### 디버깅(Debugging) 방법
1. **print 함수 활용:** 특정 함수 결과, 반복/조건 결과 등 나눠서 생각, 코드를 bisection으로 나눠서 생각
2. **개발 환경(text editor, IDE) 등에서 제공하는 기능 활용:** breakpoint, 변수 조회 등
3. **Python tutor 활용**(단순 파이썬 코드인 경우)
4. **뇌 컴파일, 눈 디버깅 등**

---

## 5. 에러(Error)

### 에러란?
* 프로그램 실행 중에 발생하는 예외 상황

### 파이썬의 에러 유형
* **문법 에러(Syntax Error):** 프로그램의 구문이 올바르지 않은 경우 발생(오타, 괄호 및 콜론 누락 등의 문법적 오류)
* **예외(Exception):** 프로그램 실행 중에 감지되는 에러

### 문법 에러 예시
```python
# Invalid syntax (문법 오류)
while  # SyntaxError: invalid syntax

# assign to literal (잘못된 할당)
5 = 3  # SyntaxError: cannot assign to literal here. Maybe you meant '==' instead of '='?

# EOL (End of Line)
print('hello
# SyntaxError: unterminated string literal (detected at line 1)

# EOF (End of File)
print(
# SyntaxError: '(' was never closed
```

### 예외(Exception)
* 프로그램 실행 중에 감지되는 에러

### 내장 예외(Built-in Exceptions)
* 예외 상황을 나타내는 예외 클래스들
* 파이썬에서 이미 정의되어 있으며, 특정 예외 상황에 대한 처리를 위해 사용

참고: `https://docs.python.org/3/library/exceptions.html#ValueError`

| 예외 | 발생 상황 |
| --- | --- |
| `ZeroDivisionError` | 나누기 또는 모듈로 연산의 두 번째 인자가 0일 때 발생 |
| `NameError` | 지역 또는 전역 이름을 찾을 수 없을 때 발생 |
| `TypeError` | 타입 불일치, 인자 초과/누락 |
| `ValueError` | 연산이나 함수에 문제가 없지만 부적절한 값을 가진 인자를 받았고, 상황이 IndexError처럼 더 구체적인 예외로 설명되지 않는 경우 발생 |
| `IndexError` | 시퀀스 인덱스가 범위를 벗어날 때 발생 |
| `KeyError` | 딕셔너리에 해당 키가 존재하지 않는 경우 |
| `ModuleNotFoundError` | 모듈을 찾을 수 없을 때 발생 |
| `ImportError` | import 하려는 이름을 찾을 수 없을 때 발생 |
| `KeyboardInterrupt` | 사용자가 Control-C 또는 Delete를 누를 때 발생, 무한루프 시 강제 종료 |
| `IndentationError` | 잘못된 들여쓰기와 관련된 문법 오류 |

```python
10 / 0  # ZeroDivisionError: division by zero

print(name_error)
# NameError: name 'name_error' is not defined. Did you mean: 'NameError'?

'2' + 2  # TypeError: can only concatenate str (not "int") to str
sum()    # TypeError: sum() takes at least 1 positional argument (0 given)
sum(1, 2, 3)  # TypeError: sum() takes at most 2 arguments (3 given)

import random
random.sample(1, 2)
# TypeError: Population must be a sequence. For dicts or sets, use sorted(d).

int('1.5')       # ValueError: invalid literal for int() with base 10: '1.5'
range(3).index(6) # ValueError: 6 is not in range

empty_list = []
empty_list[2]  # IndexError: list index out of range

person = {'name': 'Alice'}
person['age']  # KeyError: 'age'

import hahaha  # ModuleNotFoundError: No module named 'hahaha'
from random import hahaha
# ImportError: cannot import name 'hahaha' from 'random'

for i in range(10):
print(i)  # IndentationError: expected an indented block after 'for' statement on line 1
```

---

## 6. 예외 처리

### 예외 처리(Exception Handling)
* 예외 발생시 프로그램이 비정상적으로 종료되지 않고, 적절하게 처리할 수 있도록 하는 방법

### 예외 처리 사용 구문
* **try:** 예외가 발생할 수 있는 코드 작성
* **except:** 예외가 발생했을 때 실행할 코드 작성
* **else:** 예외가 발생하지 않았을 때 실행할 코드 작성
* **finally:** 예외 발생 여부와 상관없이 항상 실행할 코드 작성

```python
try:
    x = int(input('숫자를 입력하세요: '))
    y = 10 / x
except ZeroDivisionError:
    print('0으로 나눌 수 없습니다.')
except ValueError:
    print('유효한 숫자가 아닙니다.')
else:
    print(f'결과: {y}')
finally:
    print('프로그램이 종료되었습니다.')
```

### try & except

### try-except 구조
* `try` 블록 안에는 예외가 발생할 수 있는 코드를 작성
* `except` 블록 안에는 예외가 발생했을 때 처리할 코드를 작성
* 예외 발생시 프로그램 흐름은 `try` 블록을 빠져나와 해당 예외에 대응하는 `except` 블록으로 이동

```python
try:
    # 예외가 발생할 수 있는 코드
except 예외:
    # 예외 처리 코드
```

### 예외 처리 예시
```python
try:
    result = 10 / 0
except ZeroDivisionError:
    print('0으로 나눌 수 없습니다.')
# 0으로 나눌 수 없습니다.
```
```python
try:
    num = int(input('숫자입력: '))
except ValueError:
    print('숫자가 아닙니다.')
"""
숫자입력: a
숫자가 아닙니다.
"""
```

### 복수 예외 처리 연습
* 100을 사용자가 입력한 값으로 나누고 출력하는 코드를 작성 하시오.
* 먼저, 발생 가능한 에러가 무엇인지 예상해보기

```python
num = int(input('100으로 나눌 값을 입력하시오: '))
print(100 / num)
```
* 발생 가능한 에러: `int('a')`(문자열을 int로 형변환: ValueError), `100 / int('0')`(0으로 숫자를 나눔: ZeroDivisionError)

* 발생가능한 에러를 모두 명시하거나 & 별도로 작성하기

```python
try:
    num = int(input('100으로 나눌 값을 입력하시오: '))
    print(100 / num)
except (ValueError, ZeroDivisionError):
    print('제대로 입력해주세요.')
```
```python
try:
    num = int(input('100으로 나눌 값을 입력하시오: '))
    print(100 / num)
except ValueError:
    print('숫자를 넣어주세요.')
except ZeroDivisionError:
    print('0으로 나눌 수 없습니다.')
except:
    print('에러가 발생하였습니다.')
```

### else & finally
* `else` 블록은 예외가 발생하지 않았을 때 추가 작업을 진행
* `finally` 블록은 예외 발생 여부와 상관없이 항상 실행할 코드를 작성

```python
try:
    x = int(input('숫자를 입력하세요: '))
    y = 10 / x
except ZeroDivisionError:
    print('0으로 나눌 수 없습니다.')
except ValueError:
    print('유효한 숫자가 아닙니다.')
else:
    print(f'결과: {y}')
finally:
    print('프로그램이 종료되었습니다.')
```

---

## 7. 참고 — 예외 처리 주의사항

### 내장 예외의 상속 계층구조 주의
* 아래와 같이 예외를 작성하면 코드는 2번째 except 절에 이후로 도달하지 못함

```python
try:
    num = int(input('100으로 나눌 값을 입력하시오: '))
    print(100 / num)
except BaseException:
    print('숫자를 넣어주세요.')
except ZeroDivisionError:  # 이 블록에 도달하지 못함
    print('0으로 나눌 수 없습니다.')
except:
    print('에러가 발생하였습니다.')
```
* 내장 예외 클래스는 상속 계층구조를 가지기 때문에 except 절로 분기 시 반드시 하위 클래스를 먼저 확인 할 수 있도록 작성해야 함
* 참고: `https://docs.python.org/ko/3/library/exceptions.html#exception-hierarchy` (BaseException → Exception → ArithmeticError/LookupError/OSError/NameError/... 등 계층 구조를 가짐, ZeroDivisionError는 ArithmeticError의 하위 클래스)

```python
try:
    num = int(input('100으로 나눌 값을 입력하시오: '))
    print(100 / num)
except BaseException:  # 하위 예외 클래스부터 확인해야 함
    print('숫자를 넣어주세요.')
except ZeroDivisionError:
    print('0으로 나눌 수 없습니다.')
except:
    print('에러가 발생하였습니다.')
```

---

## 8. 참고 — 예외 객체 다루기

### as 키워드
* **예외객체:** 예외가 발생했을 때 예외에 대한 정보를 담고 있는 객체
* `except` 블록에서 예외 객체를 받아 상세한 예외 정보를 활용 가능

```python
my_list = []

try:
    number = my_list[1]
except IndexError as error:
    print(f'{error}가 발생했습니다.')
# list index out of range가 발생했습니다.
```

### try-except와 if-else
* try-except와 if-else를 함께 사용할 수 있음

```python
try:
    x = int(input('숫자를 입력하세요: '))
    if x < 0:
        print('음수는 허용되지 않습니다.')
    else:
        print('입력한 숫자:', x)
except ValueError:
    print('오류 발생')
```

---

## 핵심 요약
* **상속**은 부모 클래스의 속성/메서드를 자식 클래스가 물려받아 코드 재사용성과 계층 구조를 확보하는 방법이며, 자식 클래스가 부모의 메서드를 다시 정의하는 것을 **메서드 오버라이딩**이라 한다.
* **다중 상속**은 상속 순서에 따라 중복된 속성/메서드의 우선순위가 결정되며, 이때 발생하는 **다이아몬드 문제**를 파이썬은 **MRO(Method Resolution Order)** 알고리즘(깊이 우선, 왼쪽→오른쪽, 중복 검색 없음)으로 해결한다.
* **super()**는 부모 클래스를 명시하지 않고도 MRO 순서에 따라 다음 부모의 메서드를 호출하는 내장 함수로, 단일 상속에서는 유지보수성을, 다중 상속에서는 예측 가능한 초기화 순서를 보장한다. `클래스.mro()` 또는 `클래스.__mro__`로 MRO 순서를 확인할 수 있다.
* **에러**는 문법 에러(SyntaxError)와 실행 중 감지되는 예외(Exception)로 나뉘며, 파이썬은 `ZeroDivisionError`/`NameError`/`TypeError`/`ValueError`/`IndexError`/`KeyError`/`ModuleNotFoundError`/`ImportError`/`KeyboardInterrupt`/`IndentationError` 등 다양한 내장 예외를 제공한다.
* **예외 처리**는 `try`(예외 발생 가능 코드) → `except`(예외 처리) → `else`(예외 없을 때) → `finally`(항상 실행) 구조로 작성하며, 내장 예외는 상속 계층구조를 가지므로 `except` 절은 반드시 하위 클래스(더 구체적인 예외)를 먼저 작성해야 하고, `except 예외 as error`로 예외 객체의 상세 정보를 받아 활용할 수 있다.
