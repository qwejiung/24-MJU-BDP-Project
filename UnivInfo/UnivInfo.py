from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup

# Selenium WebDriver 설정
options = webdriver.ChromeOptions()
options.add_argument("--headless")  # 브라우저 창을 띄우지 않음
options.add_argument("--no-sandbox")
options.add_argument("--disable-dev-shm-usage")

# ChromeDriver 실행
driver = webdriver.Chrome(options=options)

# URL 설정
url = "https://namu.wiki/w/%EC%BA%A0%ED%8D%BC%EC%8A%A4"
driver.get(url)

# WebDriverWait으로 특정 div가 로드될 때까지 대기
try:
    WebDriverWait(driver, 30).until(
        EC.presence_of_element_located((By.CLASS_NAME, "_1CbAe5Lw"))  # div 클래스명
    )
    print("특정 div가 로드되었습니다.")
except:
    print("특정 div 로드 실패.")
    driver.quit()
    exit()

# 페이지 소스 가져오기
html = driver.page_source
soup = BeautifulSoup(html, "html.parser")

# 해당 클래스명을 가진 모든 테이블 찾기
tables = soup.find_all("table", {"class": "xZl7LbPm _ba832eed82a14e6b3bc9304cd9b2a6d0"})

# 3번과 4번 테이블 데이터 가져오기
all_university_data = []
for index in [2, 3]:  # 3번, 4번 테이블은 인덱스 2, 3 (Python의 리스트는 0부터 시작)
    if index < len(tables):
        table = tables[index]
        rows = table.find("tbody").find_all("tr")
        for row in rows[1:]:  # 첫 번째 행은 헤더
            columns = row.find_all("td")
            if len(columns) >= 4:  # 데이터가 4열 이상인 경우
                # 대학명 (두 번째 열)
                name_div = columns[1].find("div", {"class": "W4-ZFyxd"})
                area_div = columns[3].find("div", {"class": "W4-ZFyxd"})
                if name_div and area_div:
                    name = name_div.get_text(strip=True)
                    area = area_div.get_text(strip=True)
                    
                    # 데이터 저장
                    all_university_data.append({
                        "대학명": name,
                        "면적": area
                    })

# WebDriver 종료
driver.quit()

# 결과 출력
if all_university_data:
    for item in all_university_data:
        print(f"{item['대학명']}: {item['면적']}")
else:
    print("데이터를 찾을 수 없습니다.")
    

import googlemaps
from dotenv import load_dotenv
import os

# .env 파일에서 Google API Key 로드
load_dotenv()
api_key = os.getenv('GOOGLE_API_KEY')
gmaps = googlemaps.Client(key=api_key)

# 대학교 이름과 면적 데이터를 기반으로 위도/경도 추가
university_locations = []
for data in all_university_data:
    try:
        # Geocoding API 호출
        geocode_result = gmaps.geocode(data['대학명'], language='ko')
        if geocode_result:
            location = geocode_result[0]['geometry']['location']
            latitude = location['lat']
            longitude = location['lng']
            university_locations.append({
                "대학명": data['대학명'],
                "면적": data['면적'],
                "위도": latitude,
                "경도": longitude
            })
            print(f"{data['대학명']}: 위도 {latitude}, 경도 {longitude}")
        else:
            print(f"{data['대학명']}: 위치 정보를 찾을 수 없습니다.")
    except Exception as e:
        print(f"{data['대학명']}: 오류 발생 - {e}")


import pandas as pd

# 데이터를 DataFrame으로 변환
df = pd.DataFrame(university_locations)

# CSV 파일로 저장
df.to_csv("university_locations.csv", index=False, encoding="utf-8-sig")

print("CSV 파일이 저장되었습니다.")
