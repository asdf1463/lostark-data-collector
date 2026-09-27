# Lost Ark Data Collector

`자동가격.py`는 Lost Ark API의 생활재료, 제작품, 보석 가격을
`lostark_ts_data.db`에 저장합니다. 기존 GitHub Actions 수집 작업은
`requests`와 `LOSTARK_API_KEY` 환경변수를 사용합니다.

## 시장 분석

[analysis/README.md](analysis/README.md)의 별도 분석 모듈로 DB를 읽어
데이터 품질 감사, 회귀계수 CSV, PNG 차트를 생성할 수 있습니다.
분석은 수동 실행하며 API key가 필요하지 않습니다. DB는 읽기 전용으로 엽니다.

저장소 루트에서 Python 3.10 이상으로 실행합니다.

```sh
python -m pip install -r analysis/requirements.txt
python analysis/lostark_market_analysis.py --out analysis_outputs
```

전달 자료와 같은 가격일 범위를 재현하려면 `--end-date 2026-09-24`를 추가합니다.
분석 의존성은 기존 수집 workflow에서 설치하지 않습니다.

달력 기준 표준오차, 품목 간 효과 차이, 이벤트·요일 검정과 민감도 분석:

```sh
python analysis/lostark_market_robustness.py --end-date 2026-09-24 --out analysis_outputs/robustness
python -m unittest discover -s analysis/tests -v
```

실제 DB 검증의 기간·환경·핵심 결과는 [검증 기록](analysis/VALIDATION.md)에 정리했습니다.
