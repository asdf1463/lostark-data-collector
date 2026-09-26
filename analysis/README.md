# Lost Ark Market Analysis — 재현 코드

이 폴더의 `lostark_market_analysis.py`는 최종 보고서의 분석과 시각화를 재현합니다.

## 1. 분석 구조

### 아비도스 융화재료
생산재/중간재 성격을 반영하여 아래 동적 회귀를 사용합니다.

```text
Δlog(P_t)
= β0 Δlog(Cost_t)
+ Σ φ_k Δlog(P_{t-k})
+ Σ β_k Δlog(Cost_{t-k})
+ weekday fixed effects
+ ε_t
```

- `Cost_t`: 6개 생활 제작경로 중 최저 제작원가
- 가격/원가 lag: 1~3일
- 6/24, 8/5 전후 ±7일 제외
- 실제 달력상 연속된 lag만 사용
- HAC(Newey-West, lag 7)

6/24는 별도로 AR(1) + 추세 + `Post` + `TimeAfter` + 요일을 포함한 Interrupted Time Series로 분석합니다.

### 배틀아이템
- 암흑 수류탄: 원가 + AR(1) + 요일 + 주요 이벤트 당일 더미
- 성스러운 계열: `log(P/Cost)`의 Treatment-Control 상대 프리미엄을 만들고
  5/20 전후 ±35일 ITS를 적용

## 2. 시간축 처리

SQLite의 `timestamp`는 KST 기준 수집시각으로 해석합니다.

- `life_materials.yday_avg_price`
- `crafted_items.yday_avg_price`

는 **전일 평균**이므로:

```text
price_date = KST 수집일 - 1일
```

로 보정합니다.

같은 KST 날짜의 `YDayAvgPrice`는 `price_date × item_name` 단위로 통합합니다.
0 이하 값은 먼저 결측 처리합니다. 양수 중복값이 서로 다르면 임의로 평균하지 않고
오류로 중단합니다. 원시 중복 현황은 audit에 기록합니다.

`gem_prices.top5_avg_price`는 시점 가격이므로 하루를 당기지 않고,
같은 날 여러 관측이 있으면 장기 benchmark용 일별 값은 마지막 관측을 사용합니다.

누락일은 보간하지 않습니다.
성스러운 계열 상대 프리미엄은 Treatment 2종과 Control 2종이 모두 관측된 날짜만
사용합니다. 시장 배경 차트의 7일 중앙값도 원래 관측이 없는 날짜는 결측으로 유지합니다.

## 3. 설치

```bash
python -m pip install -r analysis/requirements.txt
```

## 4. 실행

Python 3.10 이상, 저장소 루트에서 실행합니다. 기존 수집 workflow의
`requests` 설치와 별도로 분석에만 사용하는 의존성입니다.
운영 수집 환경과 분리하려면 가상환경을 만들어 설치하세요.

기본 DB는 스크립트 위치를 기준으로 저장소 루트의 `lostark_ts_data.db`입니다.
DB는 읽기 전용으로 열며 스키마와 데이터는 수정하지 않습니다.

```bash
python analysis/lostark_market_analysis.py \
  --db lostark_ts_data.db \
  --out analysis_outputs
```

Windows PowerShell:

```powershell
python .\analysis\lostark_market_analysis.py `
  --db .\lostark_ts_data.db `
  --out .\analysis_outputs
```

전달본의 회귀 결과를 재현할 때는 가격일 상한을 고정합니다.

```sh
python analysis/lostark_market_analysis.py --end-date 2026-09-24 --out analysis_outputs/reference
```

`--end-date`는 KST 가격일 기준이며 보석에는 수집일 기준으로 적용합니다.
이 옵션이 없으면 DB 전체 기간을 사용합니다. 감사 JSON에는 실제 분석 기간,
DB SHA-256, 실행 패키지 버전, 중복 및 결측 현황을 기록합니다.
보석 상한도 9/24로 제한하면 전달본의 9/25 보석 관측은 배경 차트에서 제외됩니다.
회귀 모형에는 보석을 사용하지 않습니다.

## 5. 주요 결과 파일

`analysis_outputs/` 아래에 다음이 생성됩니다.

- `data_audit.json`
- `model_diagnostics.json` (모형별 표본 수와 R², 아비도스 공동검정)
- `01_market_background.png`
- `02_abydos_dynamic_coefficients.png`
- `03_0624_structural_break.png`
- `04_dark_grenade_weekday.png`
- `05_holy_premium_0520.png`
- `model_abydos_dynamic_일반.csv`
- `model_abydos_dynamic_상급.csv`
- `model_0624_its_일반.csv`
- `model_0624_its_상급.csv`
- `model_dark_grenade_dynamic.csv`
- `model_holy_premium_0520_its.csv`

## 6. 최종 보고서와 비교할 때 주의

단순 전후 변화율과 통합 시계열 모형의 계수는 같은 값이 아닙니다.

예:
- 6/24 일반 아비도스
  - 전후 7일의 관측된 `가격/원가` 변화: 약 +23%
  - AR(1), 추세, 요일을 통제한 ITS의 즉각 수준점프: 약 +12%

- 5/20 성스러운 계열
  - 전후 7일 상대 프리미엄 차이: 약 +33%
  - 기존 상승추세와 AR(1)을 통제하면 당일의 독립적인 수준점프는 더 작고,
    출시 이후 추세 반전이 핵심 결과

따라서 분석 결과는 “가격이 몇 % 올랐다”와
“다른 시간구조를 통제한 뒤 남은 충격”을 구분해서 해석해야 합니다.

HAC(7)는 전달 코드의 statsmodels 관측행 기준 Newey-West 계산을 유지합니다.
변화율과 AR lag는 달력으로 계산하지만 HAC의 lag는 결측·이벤트 제외 후 남은
관측행 간 거리입니다. 전달 결과와 동일한 모형을 유지하기 위한 선택입니다.

## 7. 검증

```sh
python -m unittest discover -s analysis/tests -v
```

테스트는 KST 날짜 보정, 중복·0·보석 마지막 관측, 누락일 이후 lag,
이벤트 제외, Treatment/Control 구성, 감사 기간, DB 미생성을 확인합니다.
실제 DB의 계수와 그래프 확인은 위 분석 실행으로 수행합니다.

기본 생성 경로는 `.gitignore`에 포함되어 있습니다. 수집 코드와 수집 Actions는
변경하지 않으며 분석을 자동 실행하는 workflow도 추가하지 않습니다.

## 8. 결과를 해석할 때의 범위

이 모듈은 전달된 분석을 재현합니다. 계수 재현은 인과효과의 검증과 구분해야 합니다.

- 아비도스의 당일 원가 계수 약 0.70은 다른 모형 변수를 통제했을 때 원가가 1% 더
  상승한 날 가격도 약 0.70% 더 상승하는 관계입니다. 같은 날 원가와 가격이 함께
  결정되므로, 외생적인 원가 상승의 인과적 전가율로 단정할 수 없습니다.
- 암흑 수류탄의 원가 계수는 기준 기간 0.627, p=0.077입니다. 유의하지 않다는 이유만으로
  원가 영향이 없거나 수요 영향이 지배적이라고 결론내릴 수 없습니다. 생산비 중심과
  소비수요 중심이라는 구분은 경제적 가설이며 거래량·수요 자료 없이 메커니즘을 직접 식별하지는 않습니다.
- 암흑 수류탄의 이벤트 더미는 이벤트별 단 하루에만 1입니다. 해당 관측은 모형에서
  정확하게 적합되어 leverage가 1, 잔차가 사실상 0입니다. 출력된 HAC p값을 독립적인
  이벤트 효과의 강한 증거로 사용하지 마세요. 이벤트 계수는 해당 날짜의 조건부
  이탈 크기로 기술하고, 추론에는 사전 모형의 예측구간·위약 날짜·이벤트 구간 분석 등을 추가해야 합니다.
- 요일 계수는 월요일 대비 일간 로그수익률 차이입니다. 목요일 가격 수준이 3.28% 더
  높다는 뜻이 아닙니다. 여러 요일을 함께 검정한다는 점도 고려해야 합니다.
- 6/24 일반 Post의 약 +11.9%는 전일 종속변수·추세·요일을 통제한 즉각 효과입니다.
  누적·영구 효과와 같지 않으며 동시 업데이트의 영향을 구분하지 못합니다.
  일반의 유의성과 상급의 비유의성만으로 두 효과의 차이가 유의하다고 할 수도 없습니다.
- 5/20 성스러운 프리미엄은 Post p=0.091로 즉각 상승의 증거가 제한적입니다.
  `time_after`가 음수라는 것은 사전 대비 기울기가 감소했다는 뜻입니다.
  출시 후 기울기는 `time + time_after`로 별도 검정해야 합니다.
  기준 기간 추가 검정은 약 -0.002234, p=0.082여서 5% 기준 하락 추세 자체는 확정하기 어렵습니다.
  AR(1)이 포함되어 있으므로 추세 계수를 관측 프리미엄의 실제 일간 변화율로 그대로 읽지 마세요.
- 가격 lag는 달력일 기준이지만 현재 HAC는 관측행 기준입니다. 누락일·이벤트 구간을
  제거한 표본에서 정확한 7일 간격을 반영하지 못합니다. 달력 간격을 반영한 표준오차,
  잔차 진단, 이벤트 창·lag·HAC 폭 변경으로 유의성의 안정성을 추가 확인해야 합니다.

따라서 현재 결과는 **원가·요일·이벤트와 가격변화 사이의 조건부 시계열 관계**로 해석합니다.
거래량을 통한 수요 식별, 인과효과 확정, 표본 밖 예측력 검증은 별도 작업입니다.
