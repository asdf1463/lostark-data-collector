import requests
import sqlite3
import os
from datetime import datetime, timedelta, timezone

kst = timezone(timedelta(hours=9))
now_str = datetime.now(kst).strftime('%Y-%m-%d %H:%M:%S')

# API 키 및 URL 설정
API_KEY = os.getenv("LOSTARK_API_KEY")
MARKET_URL = "https://developer-lostark.game.onstove.com/markets/items"
AUCTION_URL = "https://developer-lostark.game.onstove.com/auctions/items"

headers = {
    "accept": "application/json",
    "authorization": f"Bearer {API_KEY}",
    "Content-Type": "application/json"
}

# 조회할 아이템 목록
items_to_search = [
    {"name": "오레하 융화 재료", "category": 50010},
    {"name": "상급 오레하 융화 재료", "category": 50010},
    {"name": "최상급 오레하 융화 재료", "category": 50010},
    {"name": "아비도스 융화 재료", "category": 50010},
    {"name": "상급 아비도스 융화 재료", "category": 50010},
    {"name": "[일품] 명인의 쫄깃한 꼬치구이", "category": 70000},
    {"name": "[일품] 명인의 허브 스테이크 정식", "category": 70000},
    {"name": "[일품] 거장의 채끝 스테이크 정식", "category": 70000},
    {"name": "고급 회복약", "category": 60000},
    {"name": "정령의 회복약", "category": 60000},
    {"name": "성스러운 부적", "category": 60000},
    {"name": "만능 물약", "category": 60000},
    {"name": "암흑 수류탄", "category": 60000},
    {"name": "성스러운 폭탄", "category": 60000},
    {"name": "빛나는 성스러운 부적", "category": 60000},
    {"name": "빛나는 성스러운 폭탄", "category": 60000},
    {"name": "빛나는 만능 물약", "category": 60000},
    {"name": "빛나는 정령의 회복약", "category": 60000},
    {"name": "도구 제작 부품", "category": 90000}
]

# 카테고리 코드 범위 설정 (90200 ~ 90700: 생활재료)
category_codes = range(90200, 90800, 100)

# 현재 시간 (시계열 데이터용)
now_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

base_path = os.path.dirname(os.path.abspath(__file__))
db_path = os.path.join(base_path, 'lostark_ts_data.db')
conn = sqlite3.connect(db_path)

cursor = conn.cursor()

# 1. 생활 재료용 테이블
cursor.execute('''
    CREATE TABLE IF NOT EXISTS life_materials (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        timestamp DATETIME,
        category_code INTEGER,
        grade TEXT,
        item_name TEXT,
        yday_avg_price REAL
    )
''')

# 2. 융화재료 및 배틀 아이템용 테이블
cursor.execute('''
    CREATE TABLE IF NOT EXISTS crafted_items (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        timestamp DATETIME,
        category_code INTEGER,
        item_name TEXT,
        yday_avg_price REAL
    )
''')

# 3. 기축통화(보석) 상위 5개 평균 시세용 테이블
cursor.execute('''
    CREATE TABLE IF NOT EXISTS gem_prices (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        timestamp DATETIME,
        tier INTEGER,
        gem_name TEXT,
        top5_avg_price REAL
    )
''')
conn.commit()

try:
    # 1. 생활 재료 시계열 데이터 수집 (항목별 예외 처리)
    print(f"=== 생활 재료 수집 시작 (기준시간: {now_str}) ===")
    for code in category_codes:
        try:
            response = requests.post(MARKET_URL, headers=headers, json={"CategoryCode": code})
            response.raise_for_status()
            items = response.json().get("Items", [])

            for item in items:
                try: # 항목별(등급별) 예외 처리 시작
                    name = item.get("Name", "")
                    grade = item.get("Grade", "")
                    yday_avg_price = item.get("YDayAvgPrice")

                    # 등급 분류 및 가격 가공 로직
                    custom_grade = None
                    if grade == "희귀":
                        if "아비도스" in name: 
                            custom_grade = "아비도스"
                        elif any(keyword in name for keyword in ["오레하", "화사한", "단단한", "튼튼한"]): 
                            custom_grade = "희귀"
                    elif grade in ["일반", "고급"]: 
                        custom_grade = grade
                    elif grade == "영웅":
                        custom_grade = "영웅"
                        if yday_avg_price is not None: 
                            yday_avg_price *= 10

                    # 데이터 적치
                    if custom_grade and yday_avg_price is not None:
                        cursor.execute('''
                            INSERT INTO life_materials (timestamp, category_code, grade, item_name, yday_avg_price)
                            VALUES (?, ?, ?, ?, ?)
                        ''', (now_str, code, custom_grade, name, yday_avg_price))
                        print(f"[저장됨] 생활: {name} ({custom_grade}) | 가격: {yday_avg_price}")
                        conn.commit() # 항목별로 즉시 반영
                except Exception as item_err:
                    print(f"  └ [항목 에러] {item.get('Name')} 처리 실패: {item_err}")
                    continue
        except Exception as cat_err:
            print(f"[카테고리 에러] 코드 {code} 호출 실패: {cat_err}")
            continue

    # 2. 배틀아이템 & 융화재료 데이터 수집 (항목별 예외 처리)
    print("\n=== 배틀아이템 & 융화재료 수집 시작 ===")
    for item in items_to_search:
        try: # 항목별 예외 처리 시작
            data = {"CategoryCode": item["category"], "ItemName": item["name"]}
            response = requests.post(MARKET_URL, headers=headers, json=data)
            response.raise_for_status()
            results = response.json().get("Items", [])

            yday_avg_price = None
            for result in results:
                if result["Name"] == item["name"]:
                    yday_avg_price = result.get("YDayAvgPrice")
                    break

            if yday_avg_price is not None:
                cursor.execute('''
                    INSERT INTO crafted_items (timestamp, category_code, item_name, yday_avg_price)
                    VALUES (?, ?, ?, ?)
                ''', (now_str, item["category"], item["name"], yday_avg_price))
                print(f"[저장됨] 제작: {item['name']} | 가격: {yday_avg_price}")
                conn.commit() # 저장 즉시 반영
            else:
                print(f"[누락] {item['name']}: 전일 평균가 정보 없음 (None)")
        except Exception as e:
            print(f"[에러] {item['name']} 수집 중 오류: {e}")
            continue

    # 3. 기축통화(보석) 시세 데이터 수집
    print("\n=== 보석 시세 수집 시작 ===")
    try:
        auction_payload = {
            "Sort": "BUY_PRICE", "SortCondition": "ASC", "CategoryCode": 210000, 
            "ItemTier": 4, "ItemName": "10레벨 겁화", "PageNo": 1
        }
        response = requests.post(AUCTION_URL, headers=headers, json=auction_payload)
        response.raise_for_status()
        gem_items = response.json().get("Items", [])

        if gem_items:
            buy_prices = [i.get("AuctionInfo", {}).get("BuyPrice") for i in gem_items if i.get("AuctionInfo", {}).get("BuyPrice", 0) > 0]
            if len(buy_prices) >= 10:
                filtered_prices = buy_prices[4:] 
                avg_buy_price = sum(filtered_prices) / len(filtered_prices)

                cursor.execute('''
                    INSERT INTO gem_prices (timestamp, tier, gem_name, top5_avg_price)
                    VALUES (?, ?, ?, ?)
                ''', (now_str, 4, "10레벨 겁화", avg_buy_price))
                print(f"[저장됨] 보석: 10레벨 겁화 평균가: {avg_buy_price:,.0f} 골드")
                conn.commit()
        else:
            print("[누락] 보석 매물을 찾을 수 없습니다.")
    except Exception as e:
        print(f"[에러] 보석 시세 수집 실패: {e}")

    print(f"\n성공적으로 모든 시계열 데이터가 '{db_path}'에 누적되었습니다!")

except Exception as global_e:
    print(f"오류 발생: {global_e}")

finally:
    conn.close()
    print("DB 연결을 종료합니다.")
