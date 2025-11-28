# 관통 PJT JavaScript를 활용한 도서 데이터 분석 프로젝트 정리

도서의 줄거리·리뷰 등 텍스트 데이터를 숫자로 표현하는 **문서 벡터화**와, 벡터화된 문서 간의 유사도를 계산해 "비슷한 책 추천" 기능을 구현하는 **문서 유사도**를 JavaScript로 다루는 방법을 정리한 문서입니다.

---

## 1. 문서 벡터화 (Document Vectorization)

컴퓨터는 텍스트 자체를 비교할 수 없기 때문에, 문서를 숫자로 이루어진 벡터로 변환해야 유사도 등의 수치 연산이 가능해집니다.

### Bag of Words (BoW)

* 문서에 등장하는 단어들의 **등장 횟수**만으로 벡터를 구성 (단어의 순서는 무시)

```javascript
function buildVocabulary(docs) {
  const vocabSet = new Set();
  docs.forEach((doc) => doc.split(" ").forEach((w) => vocabSet.add(w)));
  return [...vocabSet];
}

function toBowVector(doc, vocab) {
  const words = doc.split(" ");
  return vocab.map((v) => words.filter((w) => w === v).length);
}
```

### TF-IDF (Term Frequency - Inverse Document Frequency)

* 단순 등장 횟수(TF)만 쓰면 "은/는/이/가" 같이 흔한 단어의 비중이 과도하게 커지는 문제가 있음
* **TF-IDF**는 "이 단어가 이 문서에서는 자주 나오지만(TF↑), 전체 문서에서는 드물게 등장(IDF↑)" 할수록 더 중요한 단어로 가중치를 부여

```javascript
function tf(word, doc) {
  const words = doc.split(" ");
  return words.filter((w) => w === word).length / words.length;
}

function idf(word, docs) {
  const containing = docs.filter((doc) => doc.includes(word)).length;
  return Math.log(docs.length / (1 + containing));   // 분모에 +1 (스무딩)
}

function tfidfVector(doc, docs, vocab) {
  return vocab.map((word) => tf(word, doc) * idf(word, docs));
}
```

* TF-IDF 값이 높은 단어일수록 그 문서를 대표하는 **키워드**에 가깝다고 해석할 수 있음

---

## 2. 문서 유사도 (Document Similarity)

벡터화된 두 문서(도서 줄거리 등)가 얼마나 비슷한지는 **코사인 유사도(Cosine Similarity)** 로 계산하는 것이 일반적입니다.

### 코사인 유사도

* 두 벡터 사이의 **각도**를 이용해 유사도를 측정 (벡터의 길이가 아니라 방향이 얼마나 비슷한지를 봄)
* 값의 범위: `-1 ~ 1` (텍스트 벡터는 보통 음수가 없어 `0 ~ 1`), **1에 가까울수록 유사**

```
cosine_similarity(A, B) = (A · B) / (|A| * |B|)
```

```javascript
function dot(a, b) {
  return a.reduce((sum, v, i) => sum + v * b[i], 0);
}

function magnitude(v) {
  return Math.sqrt(v.reduce((sum, x) => sum + x * x, 0));
}

function cosineSimilarity(a, b) {
  const denom = magnitude(a) * magnitude(b);
  return denom === 0 ? 0 : dot(a, b) / denom;
}
```

### 도서 추천 기능 구현 흐름

1. 전체 도서의 줄거리(텍스트)를 수집
2. 전체 문서를 기준으로 단어 사전(vocabulary) 구축 후, 각 도서를 TF-IDF 벡터로 변환
3. 사용자가 선택한 도서의 벡터와, 나머지 모든 도서 벡터 간의 코사인 유사도 계산
4. 유사도가 높은 순으로 정렬해 "이 책과 비슷한 책" 목록으로 제공

```javascript
function recommendSimilarBooks(targetIdx, bookVectors, topN = 5) {
  const target = bookVectors[targetIdx];
  return bookVectors
    .map((vec, idx) => ({ idx, score: cosineSimilarity(target, vec) }))
    .filter((item) => item.idx !== targetIdx)
    .sort((a, b) => b.score - a.score)
    .slice(0, topN);
}
```

---

## 핵심 요약
* **문서 벡터화**는 텍스트를 숫자 벡터로 변환하는 과정으로, 단순 등장 횟수를 쓰는 **BoW**보다 흔한 단어의 가중치를 낮추는 **TF-IDF**가 문서의 특징을 더 잘 반영한다.
* **코사인 유사도**는 두 벡터의 각도(방향의 유사성)를 이용해 문서 간 유사도를 `0~1` 사이 값으로 계산하는 방법이다.
* 도서 데이터에 이를 적용하면, 줄거리를 TF-IDF로 벡터화한 뒤 코사인 유사도가 높은 순으로 정렬해 **비슷한 책 추천** 기능을 JavaScript만으로 구현할 수 있다.
