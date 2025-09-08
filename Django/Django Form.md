# Django Form 정리

지금까지 `request.POST`로 직접 다루던 사용자 입력을, Django가 제공하는 **Form** 클래스로 다루는 방법과 **ModelForm**, 그리고 실전에서 HTTP 요청을 다루는 노하우를 정리한 문서입니다.

---

## 1. Django Form

### Form이 필요한 이유

* 사용자 입력값을 직접 파싱(`request.POST.get(...)`)하면, **유효성 검증**(필수 입력 여부, 길이 제한, 타입 등)을 매번 수동으로 작성해야 함
* Django의 `Form` 클래스는 검증 로직과 HTML 폼 렌더링을 한 번에 담당해 이런 반복을 줄여줌

```python
# forms.py
from django import forms

class ArticleForm(forms.Form):
    title = forms.CharField(max_length=100)
    content = forms.CharField(widget=forms.Textarea)
```

```python
# views.py
def create(request):
    if request.method == "POST":
        form = ArticleForm(request.POST)
        if form.is_valid():                                   # 유효성 검증
            title = form.cleaned_data.get("title")             # 검증된 데이터 사용
            content = form.cleaned_data.get("content")
            Article.objects.create(title=title, content=content)
            return redirect("articles:index")
    else:
        form = ArticleForm()
    return render(request, "articles/create.html", {"form": form})
```

```html
<!-- 템플릿에서 form을 손쉽게 렌더링 -->
<form method="POST">
  {% csrf_token %}
  {{ form.as_p }}
  <input type="submit">
</form>
```

* `is_valid()`: 정의된 필드 규칙에 따라 데이터를 검증하고, 통과한 값만 `cleaned_data`에 담김
* `{% csrf_token %}`: Django가 자동으로 요구하는 **CSRF(Cross-Site Request Forgery) 방지 토큰**으로, `POST` 폼에는 반드시 포함해야 함

---

## 2. Django ModelForm

* 이미 Model에 정의된 필드 정보(타입, 길이 제한 등)를 그대로 재사용해, Form 클래스의 필드를 중복 작성하지 않도록 해주는 Form

```python
class ArticleForm(forms.ModelForm):
    class Meta:
        model = Article
        fields = "__all__"     # 모델의 모든 필드를 그대로 사용 (특정 필드만 쓰려면 리스트로 지정)
```

```python
def create(request):
    form = ArticleForm(request.POST or None)
    if request.method == "POST" and form.is_valid():
        form.save()                        # 검증된 데이터로 즉시 모델 인스턴스 저장
        return redirect("articles:index")
    return render(request, "articles/create.html", {"form": form})

def update(request, pk):
    article = get_object_or_404(Article, pk=pk)
    form = ArticleForm(request.POST or None, instance=article)   # 기존 인스턴스에 값 채워 수정
    if request.method == "POST" and form.is_valid():
        form.save()
        return redirect("articles:detail", article.pk)
    return render(request, "articles/update.html", {"form": form})
```

* `instance=article`을 지정하면, 폼에 기존 데이터가 채워진 채로 렌더링되며 `save()` 시 새로 만들지 않고 해당 인스턴스를 수정함
* `ModelForm`을 사용하면 Create/Update 로직이 거의 동일한 형태로 통일되어, 코드 중복이 크게 줄어듦

---

## 3. HTTP 요청 다루기

### GET 파라미터로 검색/필터링 구현

```python
def index(request):
    keyword = request.GET.get("q", "")
    articles = Article.objects.filter(title__icontains=keyword) if keyword else Article.objects.all()
    return render(request, "articles/index.html", {"articles": articles, "keyword": keyword})
```

### Form 검증 실패 시 에러 메시지 표시

```html
{% for field in form %}
  {{ field }}
  {% for error in field.errors %}
    <p style="color:red;">{{ error }}</p>
  {% endfor %}
{% endfor %}
```

* Form이 `is_valid()`에서 실패하면, 다시 같은 템플릿을 렌더링할 때 `form` 객체 안에 사용자가 입력했던 값과 에러 메시지가 함께 담겨 있어 그대로 화면에 표시할 수 있음

---

## 핵심 요약
* Django **Form**은 사용자 입력에 대한 유효성 검증과 HTML 렌더링을 함께 담당해, 수동 파싱/검증 코드를 줄여준다.
* **ModelForm**은 Model 정의를 그대로 재사용해 필드를 중복 작성하지 않게 해주며, `instance` 옵션으로 Create/Update 로직을 동일한 패턴으로 통일할 수 있다.
* `is_valid()`로 검증에 실패하면 입력했던 값과 에러 메시지가 Form 객체에 담긴 채 템플릿에 다시 전달되어, 사용자에게 어떤 부분이 잘못되었는지 바로 보여줄 수 있다.
