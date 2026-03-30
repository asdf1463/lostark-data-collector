import requests
import sqlite3
import os
from datetime import datetime

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
    print("=== 생활 재료 시계열 데이터 수집 시작 ===")
    for code in category_codes:
        response = requests.post(MARKET_URL, headers=headers, json={"CategoryCode": code})
        response.raise_for_status()
        items = response.json().get("Items", [])

        for item in items:
            name = item.get("Name", "")
            grade = item.get("Grade", "")
            yday_avg_price = item.get("YDayAvgPrice")

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

            if custom_grade and yday_avg_price is not None:
                cursor.execute('''
                    INSERT INTO life_materials (timestamp, category_code, grade, item_name, yday_avg_price)
                    VALUES (?, ?, ?, ?, ?)
                ''', (now_str, code, custom_grade, name, yday_avg_price))

                print(f"[저장됨] 분류: {custom_grade} | 아이템: {name} | 전일평균가: {yday_avg_price}")

    print("\n=== 배틀아이템 & 융화재료 데이터 수집 시작 ===")
    for item in items_to_search:
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

            print(f"[저장됨] 아이템명: {item['name']} | 전일평균가: {yday_avg_price}")

    print("\n=== 기축통화(보석) 시세 데이터 수집 시작 ===")

    auction_payload = {
        "Sort": "BUY_PRICE",  # 즉시 구매가 기준으로 정렬
        "SortCondition": "ASC",  # 오름차순 (가장 싼 것부터)
        "CategoryCode": 210000,  # 보석
        "ItemTier": 4,  # 4티어
        "ItemName": "10레벨 겁화",  # 이름
        "PageNo": 1
    }

    response = requests.post(AUCTION_URL, headers=headers, json=auction_payload)
    response.raise_for_status()
    auction_data = response.json()

    gem_items = auction_data.get("Items", [])

    if gem_items:
        buy_prices = []

        # 검색된 매물들을 순회하며 유효한 즉시구매가(0보다 큰 값) 수집
        for item in gem_items:
            price = item.get("AuctionInfo", {}).get("BuyPrice")
            if price and price > 0:
                buy_prices.append(price)

            # 5개 모이면 반복문 종료
            if len(buy_prices) >= 5:
                break

        if buy_prices:
            # 수집된 가격들의 평균 계산
            avg_buy_price = sum(buy_prices) / len(buy_prices)

            cursor.execute('''
                INSERT INTO gem_prices (timestamp, tier, gem_name, top5_avg_price)
                VALUES (?, ?, ?, ?)
            ''', (now_str, 4, "10레벨 겁화", avg_buy_price))

            print(f"[저장됨] 보석: 10레벨 겁화 (4티어)")
            print(f" -> 수집된 상위 {len(buy_prices)}개 가격: {buy_prices}")
            print(f" -> 산출된 평균가: {avg_buy_price:,.0f} 골드")
        else:
            print("즉시 구매가가 설정된 10레벨 겁화 보석 매물이 없습니다.")
    else:
        print("조건에 맞는 10레벨 겁화 보석 매물을 찾을 수 없습니다.")

    conn.commit()
    print(f"\n성공적으로 '{db_path}' 파일에 모든 시계열 데이터가 누적되었습니다!")

except requests.exceptions.RequestException as e:
    print("API 호출 실패:", e)
except Exception as e:
    print("에러 발생:", e)
finally:
    conn.close()