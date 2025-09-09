# Django Static / Media 정리

지금까지의 CRUD 흐름을 복습하고, CSS/JS/이미지 같은 **정적 파일(Static Files)** 을 다루는 방법과 사용자가 업로드하는 **미디어 파일(Media Files)** 을 처리하는 방법을 정리한 문서입니다.

---

## 1. Django Review

지금까지의 흐름을 정리하면 다음과 같습니다.

```
URL 요청 → urls.py가 View 매칭 → View가 Model(ORM)로 데이터 조회/가공
        → Form(ModelForm)으로 사용자 입력 검증 → Template에 데이터 전달 → HTML 응답
```

* **Model:** 데이터 구조/DB 처리
* **Form/ModelForm:** 사용자 입력 검증
* **View:** 요청을 받아 Model/Form을 조합해 처리
* **Template:** 최종 화면 렌더링

---

## 2. Static Files

* **정적 파일(Static):** 요청에 따라 내용이 바뀌지 않는 파일 (CSS, JS, 로고 이미지 등)

### 설정

```python
# settings.py
STATIC_URL = "static/"                       # 정적 파일에 접근할 URL 접두사
STATICFILES_DIRS = [BASE_DIR / "static"]      # 개발 중 정적 파일을 찾을 추가 경로
```

```
project/
├── static/
│   ├── css/style.css
│   └── images/logo.png
└── articles/
    └── static/articles/js/article.js   # 앱별 static 폴더 (이름 충�놀 방지를 위해 앱 이름으로 감싸는 관례)
```

```html
<!-- 템플릿에서 static 파일 사용 -->
{% load static %}
<link rel="stylesheet" href="{% static 'css/style.css' %}">
<img src="{% static 'images/logo.png' %}" alt="로고">
```

* `{% load static %}`을 템플릿 최상단에 선언해야 `{% static %}` 태그를 사용할 수 있음
* 배포 시에는 `python manage.py collectstatic`으로 모든 앱의 static 파일을 한 곳(`STATIC_ROOT`)으로 모아 서빙

---

## 3. Media Fields

* **미디어 파일(Media):** 사용자가 서비스를 이용하면서 업로드하는 파일 (프로필 사진, 첨부 이미지 등)

### 설정

```python
# settings.py
MEDIA_URL = "media/"                    # 미디어 파일에 접근할 URL 접두사
MEDIA_ROOT = BASE_DIR / "media"          # 실제 업로드된 파일이 저장될 디렉터리
```

```python
# config/urls.py (개발 환경에서 media 파일 서빙)
from django.conf import settings
from django.conf.urls.static import static

urlpatterns = [...] + static(settings.MEDIA_URL, document_root=settings.MEDIA_ROOT)
```

### 모델에 이미지 필드 추가

```python
class Article(models.Model):
    title = models.CharField(max_length=100)
    image = models.ImageField(blank=True, upload_to="images/")   # 업로드 경로 지정
```

```html
<!-- 파일 업로드 폼은 enctype 지정이 필수 -->
<form method="POST" enctype="multipart/form-data">
  {% csrf_token %}
  {{ form.as_p }}
  <input type="submit">
</form>
```

```python
def create(request):
    form = ArticleForm(request.POST, request.FILES)   # 파일 데이터는 request.FILES에 별도로 담김
    if form.is_valid():
        form.save()
        return redirect("articles:index")
    return render(request, "articles/create.html", {"form": form})
```

* `ImageField`를 사용하려면 이미지 처리 라이브러리인 **Pillow** 설치가 필요 (`pip install pillow`)
* 업로드 폼은 반드시 `enctype="multipart/form-data"`를 지정해야 파일이 정상적으로 전송됨

---

## 핵심 요약
* **Static Files**는 CSS/JS/고정 이미지처럼 변하지 않는 자원으로, `{% static %}` 태그와 `STATICFILES_DIRS` 설정으로 관리한다.
* **Media Files**는 사용자가 업로드하는 파일로, `MEDIA_URL`/`MEDIA_ROOT` 설정과 모델의 `ImageField`/`FileField`로 다룬다.
* 파일 업로드 폼은 `enctype="multipart/form-data"`가 필수이며, View에서는 `request.FILES`로 별도로 파일 데이터를 받아 Form에 함께 전달해야 한다.
