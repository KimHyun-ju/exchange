import sqlite3
import datetime
import requests
import time
import os
import sys

# API 키를 환경 변수에서 가져옵니다.
API_KEY = os.environ.get("EXCHANGE_RATE_API_KEY")
if not API_KEY:
    print("API 키가 설정되지 않았습니다. EXCHANGE_RATE_API_KEY 환경 변수를 설정해주세요.")
    sys.exit(1)

BASE_URL = "https://oapi.koreaexim.go.kr/site/program/financial/exchangeJSON"

def get_latest_exchange():
    """가장 최근 영업일의 환율 데이터를 가져옵니다."""
    today = datetime.date.today()
    for i in range(10):
        search_date = (today - datetime.timedelta(days=i)).strftime("%Y%m%d")
        url = f"{BASE_URL}?authkey={API_KEY}&searchdate={search_date}&data=AP01"
        try:
            response = requests.get(url).json()
            if response and isinstance(response, list) and not response[0].get('result', '') == '1':
                print(f"데이터를 찾았습니다: {search_date}")
                return search_date, response
            elif response and response[0].get('result', '') == '1':
                print(f"해당 날짜에 데이터가 없습니다: {search_date}")
        except requests.exceptions.RequestException as e:
            print(f"API 호출 중 오류 발생: {e}")
            continue
    print("환율 데이터를 가져올 수 없습니다.")
    return None, None

def update_current_rates():
    """현재 환율 데이터를 DB에 갱신합니다."""
    date_used, data = get_latest_exchange()
    if not data:
        return

    conn = sqlite3.connect('exchange_rate.db')
    cursor = conn.cursor()

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS current_rates (
            cur_unit TEXT PRIMARY KEY,
            deal_bas_r REAL,
            update_date TEXT,
            update_time TEXT
        )
    ''')

    current_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    for item in data:
        if item.get('cur_unit') and item.get('deal_bas_r') is not None:
            rate = item['deal_bas_r'].replace(',', '')
            try:
                cursor.execute('''
                    INSERT OR REPLACE INTO current_rates VALUES (?,?,?,?)
                ''', (item['cur_unit'], rate, date_used, current_time))
            except Exception as e:
                print(f"데이터 삽입 중 오류 발생: {e}")
                continue

    conn.commit()
    conn.close()
    print(f"최신 환율 정보가 {current_time}에 갱신되었습니다.")

def save_historical_data():
    """일별 환율 데이터를 DB에 기록합니다."""
    date_used, data = get_latest_exchange()
    if not data:
        return

    conn = sqlite3.connect('exchange_rate.db')
    cursor = conn.cursor()

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS historical_rates (
            cur_unit TEXT,
            deal_bas_r REAL,
            record_date TEXT,
            UNIQUE(cur_unit, record_date)
        )
    ''')

    for item in data:
        if item.get('cur_unit') and item.get('deal_bas_r') is not None:
            rate = item['deal_bas_r'].replace(',', '')
            try:
                cursor.execute('''
                    INSERT INTO historical_rates (cur_unit, deal_bas_r, record_date)
                    VALUES (?, ?, ?)
                ''', (item['cur_unit'], rate, date_used))
            except sqlite3.IntegrityError:
                continue

    # 2년(730일)치 데이터만 보관
    cursor.execute("SELECT record_date FROM historical_rates GROUP BY record_date ORDER BY record_date ASC")
    dates = cursor.fetchall()

    if len(dates) > 730:
        dates_to_delete = [date[0] for date in dates[:-730]]
        for d in dates_to_delete:
            cursor.execute("DELETE FROM historical_rates WHERE record_date = ?", (d,))
        print(f"오래된 {len(dates_to_delete)}일치 데이터가 삭제되었습니다.")

    conn.commit()
    conn.close()
    print(f"일별 종가 데이터가 {date_used} 기준으로 기록되었습니다.")

if __name__ == '__main__':
    if len(sys.argv) > 1 and sys.argv[1] == 'historical':
        save_historical_data()
    else:
        update_current_rates()
