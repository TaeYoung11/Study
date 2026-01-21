# RAG 구조와 생성형 AI 정리

생성형 AI/LLM의 한계와, 이를 보완하는 **RAG(Retrieval-Augmented Generation)**, 그리고 LLM 기반 애플리케이션 프레임워크인 **LangChain**, 마지막으로 **AI Agent**의 개념을 정리한 문서입니다.

---

## 1. 생성형 AI

### 과거 AI(좁은 AI, Narrow AI)
* 각각의 작업마다 별도의 모델이 필요했음 (스팸 메일 필터링 모델, 이미지 분류 모델, 기계 번역 모델 등 개별 목적의 모델)

### 파운데이션 모델과 언어 모델의 계층
* **파운데이션 모델:** 대규모 데이터셋에 기반해 범용적인 작업(텍스트, 이미지 등)을 수행할 수 있는 AI 모델
  * **LLM(Large Language Model):** 대규모 파라미터와 방대한 텍스트 데이터로 학습된 대형 언어 모델 (예: GPT-5)
  * **SLM(Small Language Model):** 파라미터 수와 모델 크기를 줄인 경량화 버전의 언어 모델

### 언어모델의 한계와 멀티모달의 등장
* 세상은 언어로만 표현하기 어렵다. 언어는 단순하지 않다 (표정, 감정, 뉘앙스 등은 텍스트만으로 온전히 전달되지 않음)
* **멀티모달:** 텍스트 기반 언어모델의 한계를 보완해줄 새로운 기술. 언어 단독 모델의 한계를 보완 (텍스트/이미지/음성/영상 등 여러 형태의 입력을 함께 처리 → Transformer 기반 Encoder-Decoder 구조로 Image/Text 등 다양한 출력 생성)

### 추론모델의 등장
* 기억 기반(사전학습) 언어모델의 한계를 보완해줄 기술. 질문(Context) → 근거 생성(Rationale Generation) → 답변 추론(Answer Inference) 단계를 거쳐 답을 도출
* 벤치마크 점수(Competition Math, Competition Code, PhD-Level Science Questions)와 사람의 선호도 평가로 추론모델의 장점을 확인

### LLM의 한계
* **생성형 AI의 문제점**
  * 오래된 정보 (Outdated information)
  * 도메인 특화 능력 부족
  * 거짓말을 잘한다 (Hallucination)
  * 지식 매개변수화(parameterizing knowledge) 효율성이 낮음
* **실제 우리의 요구사항:** 도메인별 정확한 답변, 빈번한 데이터 업데이트, 생성된 콘텐츠의 추적성 및 설명력, 데이터의 개인정보 보호

### LLM 한계 극복 방법 비교
| 방법 | Context Optimization | Model Adaption |
| --- | --- | --- |
| Prompt Engineering | Low | Low |
| RAG (Retrieval-Augmented Generation) | High | Low |
| Fine-Tuning | Low | High |
| Hybrid (RAG + Fine-Tuning) | High | High |

* **Prompt Engineering:** 특정 작업에 대해 모델이 더 잘 반응하도록 입력 텍스트(프롬프트)를 최적화하는 방식 (예: Chain-of-Thought Prompting)
* **RAG:** 입력 프롬프트와 검색 기반의 정보를 결합(증강)하여, 증강된 정보를 기반으로 답변을 생성하도록 하는 방식
* **Fine-Tuning:** 사전 훈련된 모델을 특정 작업이나 데이터셋에 맞게 추가적으로 조정하는 방식

| 구분 | RAG | Fine-tuning |
| --- | --- | --- |
| External knowledge req'd | O | X |
| Changing model behaviour req'd | X | O |
| Reduce hallucinations? | O | X |
| Training data available? | X | O |
| Is data (in)stability dynamic? | O | X |
| Interpretability req'd? | O | X |

---

## 2. RAG (Retrieval-Augmented Generation)

* **Retrieval(검색):** 외부 데이터 및 소스를 검색하여 정보 획득
* **Augmented(증강):** 사용자의 질문을 보강하여 보다 정확한 문맥 재공
* **Generation(생성):** 향상된 정보를 기반으로 더 좋은 답변 생성
* 답변할 때 확실한 출처를 기반으로 생성하게 됨

### RAG의 장점
* 환각 현상(Hallucination) 감소
* 도메인 적응성 개선
* Open domain QA 성능 향상
* 참고한 Knowledge base가 적절한지 판단 가능
* 정보 검색에 강함

### RAG vs Fine-tuning
| | RAG | Fine-tuning |
| --- | --- | --- |
| 장점 | 외부 지식을 추가하여 정확도, 신뢰도를 높일 수 있다 | 특정 작업에 대한 성능을 높일 수 있다(예: 요약 작업 성능 향상) |
| 장점 | 새로운 정보를 추가할 때, 추가 학습이 필요하지 않다 | 매우 구체적인 태스크에 유용하며, 일관된 품질을 제공한다 |
| 단점 | 검색된 정보의 품질에 의존한다 | 많은 양의 학습 데이터가 필요하다 |
| 단점 | 검색 시스템을 동원하여야 하기 때문에 더 많은 컴퓨팅 자원을 사용할 수 있다 | 학습한 데이터 외의 질의에는 좋은 답변을 얻을 수 없다 |

### 정보 검색(Retrieval) 방법의 발전: TF-IDF → BM25 → Semantic Embedder

**역색인(Inverted Index):** "책 1 → 1페이지 호출, 100 → 100페이지 호출"처럼, 각 데이터에 빠르게 접근할 수 있도록 돕는 방식. 각 단어마다 색인 정보를 연결시켜 놓음으로써 단어 기반 검색이 가능하게 함

**TF-IDF (Term Frequency - Inverse Document Frequency)**
```
TF-IDF(t, d, D) = TF(t, d) × IDF(t, D)     t=단어, d=문서, D=전체문서

TF(t, d) = (문서 d에서 단어 t가 등장한 횟수) / (문서 d에 등장한 모든 단어의 수)
IDF(t, D) = log(총 문서의 개수 / 단어 t를 포함하는 문서의 수)
```

**BM25:** TF-IDF의 정보검색에서의 단점을 보완. Q: 사용자가 입력한 쿼리, D: 대조해보려는 문서. 대부분의 텍스트 기반 검색을 진행할 때 가장 자주 쓰이는 방식
```
score(D, Q) = Σ IDF(qᵢ) · f(qᵢ,D)·(k₁+1) / (f(qᵢ,D) + k₁·(1-b+b·|D|/avgdl))
```

* BERT의 임베딩과 BM25의 성능을 비교하면, BM25는 문서의 벡터 크기가 크지만, BERT에서의 문서 벡터는 768차원(논문 기준)으로, 단어 기반이 아닌 문맥 기반의 벡터가 retrieve 시 성능이 더 좋은 경우가 많음

### Sparse Embedding vs Dense Embedding
| 구분 | 특징 |
| --- | --- |
| Sparse embedding | 대부분의 값이 0, 몇몇 위치만 1인 벡터로 표현. 문장에 나오는 단어의 빈도를 기준으로 벡터를 만듦(TF-IDF, BM25 등). 겹치는 단어가 있으면 유사도가 높게 나오지만 단어 간의 의미적 관계를 포착하지 못함 |
| Dense Embedding | 의미를 나타내는 실수 값들로 이루어진 벡터 표현. BERT와 같은 Pretrained Language Model이 주로 사용됨 |

* "회의가 길어져서 점심을 못 먹었다"와 "업무 때문에 식사를 거른 상태다" 두 문장을 Sparse embedding하면 전혀 다르게 보지만, Dense embedding하면 두 문장의 유사도가 높게 나옴 → **의미적 유사성이 필요한 경우는 Dense embedding을 사용**

### RAG 없는 챗봇 vs RAG를 활용한 챗봇
* **RAG 없는 일반 챗봇:** 사용자 질문 → 챗봇이 AI 모델에 질문 전달 → AI 모델 답변 → 챗봇이 사용자에게 답변 (최신/도메인 정보 반영 불가)
* **RAG를 활용한 챗봇:** 사용자 질문 → 챗봇이 관련문서 검색·활용(데이터소스, 최신 분야) → 관련문서를 포함해 길게 질문을 AI 모델에 전달 → AI 모델 답변 → 챗봇이 사용자에게 답변

---

## 3. LangChain

* **LangChain이란?** ChatGPT 프로그램 안에서 벗어나 LLM의 기능을 나만의 코드(Javascript/Python)로 가져와서 이를 자유자재로 사용할 수 있게 해주는 강력한 "프레임워크". LLM으로 하는 모든 것을 LangChain을 통해서 할 수 있음을 의미 (프롬프트 엔지니어링, RAG, Agent, 외부 LLM API 사용 및 Local LLM 구동, Moderation 등)
* LangChain을 통해서 다양한 외부 및 내부 라이브러리 통합을 쉽게 할 수 있고, LLM과 여러 다른 소스들을 Chaining해서 복잡한 애플리케이션도 쉽게 구현

### LangChain의 5가지 구성 요소
| 구성 요소 | 설명 | 예시 |
| --- | --- | --- |
| 1. LLM | 초거대 언어모델로, 생성 모델의 엔진과 같은 역할을 하는 핵심 구성 요소 | GPT-4, PALM, LLAMA, Deepseek |
| 2. Prompts | 초거대 언어모델에게 지시하는 명령문 | Prompt Templates, Chat Prompt Template, Example Selectors, Output Parsers |
| 3. Chain | LLM 사슬을 형성하여 연속적인 LLM 호출이 가능하도록 하는 핵심 구성 요소. 체인을 연결하여 응답 처리를 연속적으로 실행할 수 있도록 연결하는 기능 (Sequential Chain, Router Chain) | LLM Chain, Question Answering, Summarization, Retrival Question/Answering |
| 4. Index | LLM이 문서를 쉽게 탐색할 수 있도록 구조화 하는 모듈. 자체 학습 데이터 셋에 포함되어 있지 않은 특정 외부 데이터 소스 총칭 | Document Loaders, Text Splitters, Vectorstores, Retrievers |
| 5. Agents | LLM이 기존 Prompt Template으로 수행할 수 없는 작업을 가능하게 하는 모듈. LLM과 다른 데이터 소스나 도구 두 가지 이상 조합하여 사용 가능. 선택한 LLM을 추론 엔진으로 사용하여 어떤 작업을 수행할지 결정 | Custom Agent, Custom MultiAction Agent, Conversation Agent |

### Prompt Template 구조
```
Instructions   → 이 프롬프트는 AI 모델이 답변 도우미 역할을 수행하는 것을 지시
Context        → 모델이 질문에 답변할 때, 검색된 문서의 컨텍스트를 사용하도록 지시
Prompt Input   → {question}: 사용자가 입력한 질문이 들어갈 자리 / {context}: 검색된 문서의 컨텍스트가 들어갈 자리
Output Indicator → 모델이 답변을 작성하는 곳 ("Answer in Korean" 등 출력 형식/언어 지정)
```

### Chat Model
* LLM은 다양한 언어 작업을 수행할 수 있는 범용 대규모 언어 모델이라면, **Chat Model은 대화 상호작용 및 대화에 최적화된 LLM의 특화된 버전**
```
Input:  SystemMessage(ChatModel의 페르소나, 역할 지시) + HumanMessage(사용자의 요구사항, 질문)
Output: AIMessage(ChatModel AI가 생성한 답변)
```

### LCEL (LangChain Expression Language)
* 여러 체인을 연결하여 복잡한 워크플로우를 제어하거나 여러 논리적 흐름을 생성 가능
```python
chain = {"question": RunnablePassthrough()} | prompt | llm | StrOutputParser()
```
* `|` : 서로 다른 구성 요소를 연결하고 한 구성 요소의 출력을 다음 구성 요소의 입력으로 전달
* `RunnablePassthrough()`: 사용자의 질문이 "question" 키의 값으로 전달
* `prompt`: 입력 받은 질문이 담긴 "question"을 포함한 프롬프트 템플릿 생성
* `llm`: 프롬프트 템플릿 완성 후, LLM 언어 모델에게 전달하여 답변 생성
* `StrOutputParser()`: 마지막 단계에서 생성된 답변을 문자열로 변환하여 최종 출력 생성

### LangChain으로 할 수 있는 것들
* **데이터 분석하기 - Excel:** `agent.run("...엑셀 데이터를 분석해서 상관, 계약월 등에 대해 heatmap을 예측해줘")` 처럼 자연어로 데이터 분석·시각화를 지시
* **웹에서 정보 수집하기 - URL:** 웹 URL을 넘겨 뉴스 등 웹 콘텐츠를 요약
* **문서 QA 챗봇 만들기:** RAG(Retrieval-Augmented Generation, 검색 증강 생성) 기법을 활용해서 문서를 근거로 하는 QA 챗봇을 개발할 수 있다

### 랭체인의 핵심: Retrieval
* Retrieval은 RAG의 대부분의 구성 요소를 아우르며, 구성 요소 하나하나가 RAG의 품질을 좌우
* **RAG 구조:** 외부 데이터 저장소(Vector DB, Feature Store 등) → 사용자가 질문(Query) → 유사 문장 검색 → Q/A 시스템 → 유사문장 포함 질문 → LLM → 답변
* **PDF 챗봇 구축 예시:** 1) 문서 업로드(Document Loader, PyPDFLoader 활용) → 2) 문서 분할(Text Splitter, PDF 문서를 여러 문서로 분할) → 3) 문서 임베딩(Embed to Vectorstore, LLM이 이해할 수 있도록 문서 수치화) → 4) 임베딩 검색(VectorStore Retriever, 질문과 연관성이 높은 문서 추출) → 5) 답변 생성(QA Chain)

---

## 4. AI Agent의 기본 개념

* **AI Agent란?** 사용자의 목표를 달성하기 위해 스스로 문제를 분석하고, 해결 가능한 작은 작업 단위로 분해(Planning)한 뒤, 필요 시 외부 툴이나 API를 활용하여 작업을 수행하며, 결과를 반복적으로 검토(Self-Reflection)하고 개선하는 시스템
* LLM과 다른 데이터 소스나 도구 두 가지 이상 조합하여 사용 가능. 선택한 LLM을 추론 엔진으로 사용하여 어떤 작업을 수행할지 결정
* **ChatGPT는 AI Agent의 하위 개념 또는 구성 요소로 볼 수 있으며, 단순히 텍스트를 생성하는 언어모델.** AI Agent는 ChatGPT 같은 LLM을 코어 엔진으로 활용하되, 추가적으로 툴 사용, 계획, 자율적 실행 기능이 결합된 시스템

### AI Agent vs ChatGPT
| AI Agent | ChatGPT |
| --- | --- |
| 자율성과 상호작용 능력 | 주로 단일 플러그인을 사용하여 질문에 답변 |
| 사용자가 요구한 작업의 완료를 위해 활용가능한 여러 도구와의 상호작용을 연쇄적으로, 자율적으로 수행할 수 있는 기술 | 기본 ChatGPT는 툴과 직접 상호작용하지 않음 |

### Tool
* 에이전트가 활용할 수 있는 기능적 요소. AI가 혼자 해결하기 어려운 작업을 도와주는 보조 도구. 특정 작업을 위해 외부 기능이나 전문가를 불러오는 개념
* 예: PDF 읽기, 웹 검색, 코드 실행 등
* **역할 기반 설정 + 배경 지식 제공 + 실질적인 작업 처리 능력을 갖춘 자동화 에이전트 구성 가능**

---

## 핵심 요약
* 생성형 AI(LLM)는 오래된 정보, 도메인 특화 부족, 환각(Hallucination) 등의 한계가 있으며, 이를 **Prompt Engineering / RAG / Fine-Tuning**으로 보완합니다. RAG는 추가 학습 없이 외부 지식을 결합해 신뢰도를 높이지만 검색 품질에 의존적이고, Fine-Tuning은 특정 작업에 강하지만 학습 데이터와 비용이 많이 듭니다.
* 정보 검색 기법은 **TF-IDF → BM25 → (Dense) Semantic Embedding**으로 발전해왔으며, Sparse embedding은 단어 일치 기반, Dense embedding은 의미 기반 유사도를 잡아냅니다.
* **LangChain**은 LLM/Prompts/Chain/Index/Agents 5가지 구성요소로 이루어진 프레임워크로, `|` 연산자 기반 LCEL로 여러 컴포넌트를 체이닝해 RAG 파이프라인이나 챗봇을 구축할 수 있습니다.
* **AI Agent**는 LLM을 추론 엔진 삼아 스스로 계획하고 Tool(외부 기능)을 활용해 작업을 수행·검토하는 시스템으로, 단순 텍스트 생성기인 ChatGPT보다 상위의 자율적 개념입니다.
