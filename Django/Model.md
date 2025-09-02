# Django Model 정리

데이터베이스 테이블을 파이썬 클래스로 다룰 수 있게 해주는 **Model**과 **Model Field**, 모델 변경 사항을 DB에 반영하는 **Migrations**, 그리고 데이터를 관리하는 **Admin Site**를 정리한 문서입니다.

---

## 1. Model

* Django의 Model은 **ORM(Object-Relational Mapping)** 을 통해, SQL을 직접 작성하지 않고도 파이썬 클래스/객체로 DB 테이블을 다룰 수 있게 해줌
* 하나의 Model 클래스 = DB의 테이블 하나, 클래스의 속성 = 테이블의 컬럼

```python
# articles/models.py
from django.db import models

class Article(models.Model):
    title = models.CharField(max_length=100)
    content = models.TextField()
    created_at = models.DateTimeField(auto_now_add=True)   # 생성 시각 자동 저장
    updated_at = models.DateTimeField(auto_now=True)         # 수정 시마다 자동 갱신

    def __str__(self):
        return self.title    # Admin/셸에서 객체를 표시할 때 사용할 문자열
```

* 모든 Model은 자동으로 기본키(`id`, AutoField)를 가지며, 별도로 지정하지 않아도 Django가 자동 생성

---

## 2. Model Field

| Field 타입 | 설명 |
| --- | --- |
| `CharField(max_length=N)` | 짧은 문자열 (길이 제한 필수) |
| `TextField()` | 긴 문자열 (길이 제한 없음) |
| `IntegerField()` | 정수 |
| `BooleanField()` | True/False |
| `DateField()` / `DateTimeField()` | 날짜 / 날짜+시간 |
| `ForeignKey(Model, on_delete=...)` | 다른 모델과의 N:1 관계 |

### Field 옵션

```python
class Article(models.Model):
    title = models.CharField(max_length=100, blank=False)   # blank: 폼 검증 시 빈 값 허용 여부
    view_count = models.IntegerField(default=0)               # default: 기본값
    is_published = models.BooleanField(null=True)               # null: DB에 NULL 허용 여부
```

* `blank`는 **폼 유효성 검사** 기준, `null`은 **DB 컬럼**의 NULL 허용 여부 — 둘은 별개의 개념이므로 혼동하지 않아야 함

---

## 3. Migrations

* 모델(파이썬 코드)의 변경 사항을, 실제 데이터베이스의 테이블 구조에 반영하는 절차

```bash
python manage.py makemigrations   # 모델 변경 사항을 감지해 마이그레이션 파일 생성
python manage.py migrate            # 마이그레이션 파일을 실제 DB에 적용

python manage.py showmigrations      # 적용된/미적용된 마이그레이션 목록 확인
```

* `makemigrations`는 "무엇을 바꿀지"를 파일로 기록하고, `migrate`는 "그 내용을 실제로 DB에 실행"하는 별개의 단계
* 마이그레이션 파일은 버전 관리(Git)에 함께 포함해, 팀원 모두가 동일한 DB 구조를 유지할 수 있도록 함

---

## 4. Admin Site

* Django가 기본으로 제공하는 **관리자 페이지**로, 별도의 화면 개발 없이도 데이터를 CRUD할 수 있음

```bash
python manage.py createsuperuser   # 관리자 계정 생성
```

```python
# articles/admin.py
from django.contrib import admin
from .models import Article

admin.site.register(Article)   # Admin 사이트에 모델 등록
```

* `/admin/`으로 접속하면 등록된 모델에 대한 목록 조회, 생성, 수정, 삭제 화면을 자동으로 제공받음
* 개발 초기 단계에서 실제 데이터를 빠르게 넣고 확인하는 용도로 매우 유용함

---

## 핵심 요약
* Django **Model**은 ORM을 통해 파이썬 클래스로 DB 테이블을 정의하며, 클래스 속성이 곧 테이블의 컬럼이 된다.
* **Field**는 데이터 타입뿐 아니라 `blank`(폼 검증), `null`(DB 허용), `default`(기본값) 등 다양한 옵션으로 세부 제약을 설정한다.
* 모델 변경은 `makemigrations`(변경 사항 파일화) → `migrate`(DB 반영) 두 단계로 이루어지며, **Admin Site**는 코드 작성 없이 데이터를 관리할 수 있는 기본 제공 관리자 화면이다.
