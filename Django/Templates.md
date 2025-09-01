# Django Templates 정리

Django의 **Template System**을 이용해 동적인 HTML을 만드는 방법, 중복을 줄이는 **템플릿 상속**, 그리고 요청/응답의 흐름과 **URL 설계**를 정리한 문서입니다.

---

## 1. Template System

### 템플릿 언어 (DTL, Django Template Language)

* View에서 전달한 데이터(context)를 HTML 안에 끼워 넣기 위한 문법

```html
<!-- 변수 출력 -->
<p>{{ article.title }}</p>

<!-- 태그(제어문) -->
{% if articles %}
  <ul>
  {% for article in articles %}
    <li>{{ article.title }}</li>
  {% endfor %}
  </ul>
{% else %}
  <p>게시글이 없습니다.</p>
{% endif %}

<!-- 필터: 값을 가공해서 출력 -->
<p>{{ article.content|truncatewords:20 }}</p>
<p>{{ article.created_at|date:"Y-m-d" }}</p>
```

* `{{ }}`: 변수 출력, `{% %}`: 태그(반복문/조건문 등 로직), `|`: 필터(값 가공)

---

## 2. 템플릿 상속 (Template Inheritance)

* 모든 페이지에 공통으로 들어가는 부분(헤더, 내비게이션, 푸터)을 매번 반복 작성하지 않도록, **부모 템플릿**을 만들고 각 페이지는 필요한 부분만 채우는 방식

```html
<!-- base.html (부모 템플릿) -->
<html>
<head><title>{% block title %}My Site{% endblock %}</title></head>
<body>
  <header>공통 헤더</header>
  {% block content %}
  {% endblock %}
  <footer>공통 푸터</footer>
</body>
</html>
```

```html
<!-- index.html (자식 템플릿) -->
{% extends "base.html" %}

{% block title %}게시글 목록{% endblock %}

{% block content %}
  <h1>게시글 목록</h1>
{% endblock %}
```

* `{% extends %}`: 부모 템플릿 상속
* `{% block %}`: 자식 템플릿에서 덮어쓸 수 있는 영역 정의
* `{% include %}`: 상속과 달리, 특정 위치에 다른 템플릿 조각을 그대로 삽입 (네비게이션 바 등 재사용 컴포넌트에 적합)

---

## 3. 요청과 응답

```python
# views.py
from django.http import HttpResponse
from django.shortcuts import render, redirect

def hello(request):
    return HttpResponse("Hello, Django!")     # 단순 문자열 응답

def index(request):
    return render(request, "index.html", {"key": "value"})   # 템플릿 렌더링 응답

def moved(request):
    return redirect("index")                    # 다른 URL로 리다이렉트
```

* `request` 객체를 통해 요청 메서드(`request.method`), 쿼리 파라미터(`request.GET`), 폼 데이터(`request.POST`) 등에 접근 가능

---

## 4. Django URLs

```python
# config/urls.py
from django.urls import path, include

urlpatterns = [
    path("articles/", include("articles.urls")),   # 앱 단위 URL 포함
]

# articles/urls.py
from django.urls import path
from . import views

app_name = "articles"        # URL 이름 공간(namespace)
urlpatterns = [
    path("", views.index, name="index"),
    path("<int:pk>/", views.detail, name="detail"),   # 변수 URL (정수 pk)
]
```

---

## 5. URL 이름 지정과 이름 공간

* URL 경로를 하드코딩하면, 나중에 경로가 바뀔 때 템플릿/뷰 코드 전체를 찾아 수정해야 함 → `name`으로 URL에 이름을 붙이고, **이름으로 참조**하는 것이 안전함

```html
<!-- 템플릿에서 이름으로 URL 참조 -->
<a href="{% url 'articles:detail' article.pk %}">상세보기</a>
```

```python
# views.py에서도 이름으로 참조
from django.urls import reverse
return redirect(reverse("articles:detail", args=[article.pk]))
```

* **URL 이름 공간(namespace):** `app_name`을 지정해두면, 여러 앱에 같은 이름(`index` 등)의 URL이 있어도 `articles:index`, `accounts:index`처럼 구분해서 참조할 수 있음

---

## 핵심 요약
* Django Template System은 `{{ }}`(변수), `{% %}`(태그), `|`(필터) 세 가지 문법으로 View의 데이터를 HTML에 렌더링한다.
* **템플릿 상속**(`extends`/`block`)은 공통 레이아웃의 중복을 없애며, `include`는 재사용 가능한 템플릿 조각을 삽입할 때 사용한다.
* URL은 `path()`로 View와 매핑하고, 하드코딩 대신 `name`(및 `app_name`을 통한 네임스페이스)으로 참조해야 경로 변경에 안전하다.
