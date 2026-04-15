import requests
import sqlite3
import os
import time
from datetime import datetime, timedelta, timezone

# 환경 설정 및 시간 정의
kst = timezone(timedelta(hours=9))
now_str = datetime.now(kst).strftime('%Y-%m-%d %H:%M:%S')

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

category_codes = range(90200, 90800, 100)

# DB 연결 및 테이블 생성
base_path = os.path.dirname(os.path.abspath(__file__))
db_path = os.path.join(base_path, 'lostark_ts_data.db')
conn = sqlite3.connect(db_path)
cursor = conn.cursor()

cursor.execute('''CREATE TABLE IF NOT EXISTS life_materials (id INTEGER PRIMARY KEY AUTOINCREMENT, timestamp DATETIME, category_code INTEGER, grade TEXT, item_name TEXT, yday_avg_price REAL)''')
cursor.execute('''CREATE TABLE IF NOT EXISTS crafted_items (id INTEGER PRIMARY KEY AUTOINCREMENT, timestamp DATETIME, category_code INTEGER, item_name TEXT, yday_avg_price REAL)''')
cursor.execute('''CREATE TABLE IF NOT EXISTS gem_prices (id INTEGER PRIMARY KEY AUTOINCREMENT, timestamp DATETIME, tier INTEGER, gem_name TEXT, top5_avg_price REAL)''')
conn.commit()

# 데이터 수집
try:
    # 1. 생활 재료
    print(f"=== 생활 재료 수집 시작 (기준시간: {now_str}) ===")
    for code in category_codes:
        try:
            response = requests.post(MARKET_URL, headers=headers, json={"CategoryCode": code}, timeout=10)
            response.raise_for_status()
            items = response.json().get("Items", [])

            for item in items:
                try:
                    name, grade, yday_avg_price = item.get("Name", ""), item.get("Grade", ""), item.get("YDayAvgPrice")
                    custom_grade = None
                    if grade == "희귀":
                        if "아비도스" in name: custom_grade = "아비도스"
                        elif any(k in name for k in ["오레하", "화사한", "단단한", "튼튼한"]): custom_grade = "희귀"
                    elif grade in ["일반", "고급"]: custom_grade = grade
                    elif grade == "영웅":
                        custom_grade = "영웅"
                        if yday_avg_price: yday_avg_price *= 10

                    if custom_grade and yday_avg_price is not None:
                        cursor.execute('INSERT INTO life_materials (timestamp, category_code, grade, item_name, yday_avg_price) VALUES (?, ?, ?, ?, ?)', (now_str, code, custom_grade, name, yday_avg_price))
                        print(f"[저장됨] 생활: {name} | 가격: {yday_avg_price}")
                        conn.commit()
                except Exception: continue
        except Exception as e:
            print(f"[카테고리 에러] {code}: {e}")

    # 2. 배틀아이템 & 융화재료
    print("\n=== 배틀아이템 & 융화재료 수집 시작 ===")
    for item in items_to_search:
        try:
            response = requests.post(MARKET_URL, headers=headers, json={"CategoryCode": item["category"], "ItemName": item["name"]}, timeout=10)
            response.raise_for_status()
            results = response.json().get("Items", [])

            yday_avg_price = next((r.get("YDayAvgPrice") for r in results if r["Name"] == item["name"]), None)
            if yday_avg_price is not None:
                cursor.execute('INSERT INTO crafted_items (timestamp, category_code, item_name, yday_avg_price) VALUES (?, ?, ?, ?)', (now_str, item["category"], item["name"], yday_avg_price))
                print(f"[저장됨] 제작: {item['name']} | 가격: {yday_avg_price}")
                conn.commit()
        except Exception as e:
            print(f"[에러] {item['name']}: {e}")

    # 3. 보석 (3~6위 평균)
    print("\n=== 기축통화(보석) 시세 데이터 수집 시작 ===")
    buy_prices = []
    try:
        for page_no in [1, 2, 3]:
            payload = {"Sort": "BUY_PRICE", "SortCondition": "ASC", "CategoryCode": 210000, "ItemTier": 4, "ItemName": "10레벨 겁화", "PageNo": page_no}
            response = requests.post(AUCTION_URL, headers=headers, json=payload, timeout=10)
            response.raise_for_status()
            
            gem_items = response.json().get("Items")
            if gem_items and isinstance(gem_items, list):
                for i in gem_items:
                    price = i.get("AuctionInfo", {}).get("BuyPrice")
                    if price and price > 0: buy_prices.append(price)
            time.sleep(0.2)

        if len(buy_prices) >= 6:
            target_prices = buy_prices[2:6] # 3위~6위
            avg_buy_price = sum(target_prices) / len(target_prices)
            cursor.execute('INSERT INTO gem_prices (timestamp, tier, gem_name, top5_avg_price) VALUES (?, ?, ?, ?)', (now_str, 4, "10레벨 겁화", avg_buy_price))
            conn.commit()
            print(f"[저장됨] 보석 3~6위 평균: {avg_buy_price:,.0f} 골드 (총 {len(buy_prices)}개 확인)")
        else:
            print(f"[누락] 보석 매물 부족 ({len(buy_prices)}개)")
    except Exception as e:
        print(f"[에러] 보석 수집 실패: {e}")

    print(f"\n성공적으로 모든 데이터가 저장되었습니다!")

except Exception as global_e:
    print(f"치명적 오류 발생: {global_e}")
finally:
    conn.close()
    print("DB 연결을 종료합니다.")
