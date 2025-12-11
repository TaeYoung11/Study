# 관통 PJT MCP 활용 정리

앞서 개념을 살펴본 **MCP(Model Context Protocol)** 를 실제로 프로젝트에 **MCP Server**로 구축하고, 이를 활용해 스스로 도구를 호출하며 작업을 수행하는 **AI Agent**를 만드는 방법을 정리한 문서입니다.

---

## 1. MCP Server 구축

### MCP 서버가 제공하는 세 가지 요소

| 요소 | 설명 |
| --- | --- |
| **Tool(도구)** | AI가 호출해 실행할 수 있는 함수 (예: DB 조회, 외부 API 호출) |
| **Resource(리소스)** | AI가 참고할 수 있는 데이터/문서 (읽기 전용 컨텍스트) |
| **Prompt(프롬프트 템플릿)** | 특정 작업을 위해 미리 정의해둔 프롬프트 양식 |

### 프로젝트 데이터를 다루는 MCP Server 예시

```python
from mcp.server.fastmcp import FastMCP

mcp = FastMCP("book-project-server")

@mcp.tool()
def search_books(keyword: str) -> list[dict]:
    """제목/저자에 keyword가 포함된 도서를 검색해 반환"""
    return Book.objects.filter(title__icontains=keyword).values("id", "title", "author")

@mcp.tool()
def get_book_detail(book_id: int) -> dict:
    """도서 ID로 상세 정보를 조회"""
    book = Book.objects.get(id=book_id)
    return {"title": book.title, "summary": book.summary}

if __name__ == "__main__":
    mcp.run()
```

* 각 `@mcp.tool()` 함수의 **함수명, 파라미터, docstring**이 그대로 AI에게 "이 도구는 무엇을 하는지"를 알려주는 설명이 되므로, 명확하게 작성하는 것이 중요함
* 서버를 실행해두면, MCP를 지원하는 어떤 AI 클라이언트(호스트 애플리케이션)에서도 이 프로젝트의 데이터를 도구로 호출할 수 있게 됨

---

## 2. AI Agent

### Agent란?

* 단순히 한 번의 질문-응답으로 끝나는 것이 아니라, **스스로 상황을 판단해 필요한 도구를 여러 번 호출**하며 목표를 달성해 나가는 AI 시스템
* MCP로 노출된 Tool들은 Agent가 활용할 수 있는 "손과 발"의 역할을 함

### Agent의 동작 루프 (ReAct 패턴)

1. **Reasoning(추론):** 사용자 요청을 이해하고, 다음에 무엇을 해야 할지 계획
2. **Action(행동):** 계획에 따라 필요한 Tool을 호출 (MCP 서버의 도구 실행)
3. **Observation(관찰):** Tool 호출 결과를 확인
4. 목표가 달성될 때까지 1~3을 반복하고, 최종적으로 사용자에게 결과를 정리해 응답

```
사용자: "SF 장르 도서 중 별점이 가장 높은 책을 추천해줘"

Agent 추론: 장르로 도서를 검색하는 도구가 필요하다 → search_books(keyword="SF") 호출
Agent 관찰: 검색된 도서 목록을 확인
Agent 추론: 각 도서의 평점 정보가 필요하다 → get_book_detail(book_id=...) 반복 호출
Agent 관찰: 평점 비교 후 최고점 도서 확인
Agent 응답: "OOO 도서를 추천합니다. (평점: 4.8)"
```

### Agent 설계 시 고려할 점

* **Tool의 책임 범위를 명확히 나누기:** 하나의 Tool이 너무 많은 일을 하면 Agent가 언제 호출해야 할지 판단하기 어려워짐
* **무한 루프 방지:** 최대 반복 횟수(step 제한)를 두어, Agent가 같은 행동을 반복하며 멈추지 않는 상황을 방지
* **결과 검증:** Tool 호출 결과를 그대로 신뢰하지 않고, 다음 단계로 넘어가기 전에 최소한의 유효성 확인 로직을 둘 수 있음

---

## 핵심 요약
* **MCP Server**는 프로젝트의 데이터/기능을 `Tool`(실행 함수), `Resource`(참고 자료), `Prompt`(템플릿) 형태로 노출해, 어떤 AI 클라이언트에서도 표준화된 방식으로 호출할 수 있게 한다.
* **AI Agent**는 한 번의 응답으로 끝나지 않고, **추론(Reasoning) → 행동(Action, Tool 호출) → 관찰(Observation)** 을 반복하며 목표를 달성해가는 AI 시스템이다.
* MCP로 노출한 Tool들이 Agent가 사용할 수 있는 실행 수단이 되므로, Tool의 역할을 명확히 나누고 반복 횟수를 제한하는 것이 안정적인 Agent 설계의 핵심이다.
