# SW문제해결응용 DP 정리

**동적 계획법(Dynamic Programming, DP)** 의 핵심 개념과, 이를 활용한 대표 예제인 **이항계수 구하기**, **동전 거스름돈 문제**를 정리한 문서입니다.

---

## 1. DP(Dynamic Programming)란?

### 개념

* 큰 문제를 작은 부분 문제로 나누어 풀되, **부분 문제의 답을 저장(메모이제이션)** 해두고 재활용하여 **중복 계산을 제거**하는 알고리즘 설계 기법
* 분할 정복과 달리, DP가 다루는 문제는 부분 문제들이 **서로 겹치는(Overlapping Subproblems)** 구조를 가짐

### DP를 적용하기 위한 두 가지 조건

1. **최적 부분 구조(Optimal Substructure):** 부분 문제의 최적해를 이용해 전체 문제의 최적해를 구성할 수 있어야 함
2. **중복되는 부분 문제(Overlapping Subproblems):** 동일한 부분 문제가 여러 번 반복해서 등장해야 함 (그렇지 않다면 굳이 저장할 이유가 없음)

### 구현 방식 — Top-down vs Bottom-up

| 방식 | 설명 | 구현 |
| --- | --- | --- |
| **Top-down (메모이제이션)** | 재귀 함수로 큰 문제부터 시작해서, 이미 계산한 값은 저장해뒀다가 재사용 | 재귀 + 캐시(dict, 배열) |
| **Bottom-up (타뷸레이션)** | 가장 작은 부분 문제부터 차례로 계산하여 테이블(배열)을 채워나감 | 반복문 + 배열 |

```python
# Top-down (메모이제이션) - 피보나치 예시
memo = {}
def fib(n):
    if n <= 1:
        return n
    if n in memo:
        return memo[n]
    memo[n] = fib(n - 1) + fib(n - 2)
    return memo[n]

# Bottom-up (타뷸레이션) - 피보나치 예시
def fib_bottom_up(n):
    dp = [0] * (n + 1)
    dp[1] = 1
    for i in range(2, n + 1):
        dp[i] = dp[i - 1] + dp[i - 2]
    return dp[n]
```

* 재귀 없이 저장된 값만으로 계산하면 시간복잡도가 `O(2^N)` (일반 재귀) 에서 `O(N)` (DP)으로 크게 줄어듦

---

## 2. DP 이항계수 — `nCr` 구하기

* 이항계수 `nCr` (n개 중 r개를 선택하는 조합의 수)은 다음 점화식을 만족
  * `nCr = (n-1)C(r-1) + (n-1)Cr`
  * `nC0 = nCn = 1` (기저 조건)
* 이는 **파스칼의 삼각형(Pascal's Triangle)** 과 동일한 구조로, 매번 팩토리얼을 계산하지 않고도 DP 테이블로 빠르게 구할 수 있음

```python
def binomial_coefficient(n, r):
    dp = [[0] * (r + 1) for _ in range(n + 1)]

    for i in range(n + 1):
        for j in range(min(i, r) + 1):
            if j == 0 or j == i:
                dp[i][j] = 1                       # 기저 조건: nC0 = nCn = 1
            else:
                dp[i][j] = dp[i - 1][j - 1] + dp[i - 1][j]   # 점화식

    return dp[n][r]
```

* 재귀로 그대로 구현하면 동일한 `(n, r)` 조합이 반복 호출되어 비효율적이지만, DP 테이블에 저장하면 각 `(i, j)`는 단 한 번만 계산됨 → 시간복잡도 `O(N*R)`

---

## 3. DP 동전 거스름돈

탐욕 알고리즘 문서에서 살펴본 동전 교환 문제는, **동전 단위가 서로 배수 관계가 아니면 탐욕적 방법으로 최적해를 보장할 수 없었습니다.** 이런 경우 DP로 접근하면 항상 최적해(최소 동전 개수)를 구할 수 있습니다.

* `dp[i]`: 금액 `i`원을 만들 수 있는 **최소 동전 개수**
* 점화식: `dp[i] = min(dp[i], dp[i - coin] + 1)` (사용 가능한 모든 `coin`에 대해)

```python
def min_coins(coins, amount):
    INF = float("inf")
    dp = [INF] * (amount + 1)
    dp[0] = 0   # 0원을 만드는 데 필요한 동전은 0개

    for i in range(1, amount + 1):
        for coin in coins:
            if i - coin >= 0 and dp[i - coin] != INF:
                dp[i] = min(dp[i], dp[i - coin] + 1)

    return dp[amount] if dp[amount] != INF else -1   # 만들 수 없는 경우 -1

coins = [500, 400, 100, 10]   # 배수 관계가 아니어서 탐욕 알고리즘으로는 최적해 보장 안 됨
print(min_coins(coins, 800))
```

* 탐욕 알고리즘은 "지금 당장 가장 좋아 보이는 선택"만 하지만, DP는 `dp[i - coin]`이라는 **모든 하위 문제의 최적해를 이미 계산해둔 상태**에서 최선의 선택을 하므로 동전 단위와 관계없이 항상 정확한 최소 개수를 구함

---

## 핵심 요약
* **DP**는 부분 문제가 중복되는(Overlapping Subproblems) 문제에서, 한 번 계산한 값을 저장해두고 재사용하여 중복 계산을 제거하는 기법이며, Top-down(메모이제이션)과 Bottom-up(타뷸레이션) 두 가지 방식으로 구현할 수 있다.
* **이항계수**는 파스칼의 삼각형과 동일한 점화식(`nCr = (n-1)C(r-1) + (n-1)Cr`)을 DP 테이블로 채워 `O(N*R)`에 구할 수 있다.
* **동전 거스름돈**은 탐욕 알고리즘이 통하지 않는 동전 단위(배수 관계가 아닌 경우)에서도, `dp[i] = min(dp[i], dp[i-coin]+1)` 점화식을 이용한 DP로 항상 정확한 최소 동전 개수를 구할 수 있다.
