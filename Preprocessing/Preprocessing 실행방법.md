# Preprocessing 실행 방법

- maria_dev 접속 후
    - BDP 디렉토리 생성, 접속
    
    ```jsx
    mkdir BDP
    cd BDP
    ```
    

- 소상공인시장진흥공단_상가(상권)정보_서울_202409.csv를 restaurant.csv로 이름 변경
- restaurant.csv 데이터의 용량이 커 Ambari에 직접 업로드 불가

- FileZilla 사용
    - BDP 디렉토리에 university_locations.csv 와 restaurant.csv 업로드

---

- hdfs에 csv파일 저장
    - BDP 디렉토리에서 코드 입력해야 함

```jsx
hdfs dfs -put university_locations.csv /user/maria_dev/
hdfs dfs -put restaurant.csv /user/maria_dev/
```

- 저장 후 확인을 위해 입력

```jsx
hdfs dfs -ls /user/maria_dev/
```

- pyspark에 접속하기 전 UTF-8 설정

```jsx
export LANG=en_US.UTF-8
export LC_ALL=en_US.UTF-8
```

---

- pyspark에 접속

```jsx
pyspark
```

### university_locations.csv 전처리

- 차례대로 코드 입력

```jsx
university_df = spark.read.csv('hdfs:///user/maria_dev/university_locations.csv',header=True,inferSchema=True)
university_df.show(n=50)
```

- 경희대학교 정보가 없어 직접 추가 시도
    - 글자 깨짐 현상 발생하여 제외하고 진행

- 면적과 대학명 정리

```jsx
from pyspark.sql import functions as F
university_df = university_df.withColumn("면적", F.regexp_replace(F.regexp_replace(F.regexp_replace(university_df["면적"], ",", ""), "㎡", ""), r"\[\d+\]", "")) \
                  .withColumn("대학명", F.regexp_replace(F.col("대학명"), r"\[.*?\]", ""))         
```

```jsx
university_df.show(n=50)
```

- 반경 column 추가
- 함수 입력 후 enter 두 번

```jsx
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType

def calculate_radius(area):
    if area <= 100000:
        return 500
    elif area > 1000000:
        return 2000
    else:
        additional_radius = ((area - 100000) // 100000 + 1) * 100
        return 500 + additional_radius
```

```jsx
calculate_radius_udf = F.udf(calculate_radius, IntegerType())
university_df = university_df.withColumn("반경", calculate_radius_udf(F.col("면적").cast(IntegerType())))
university_df.show(n=50)
```

---

## 소상공인시장진흥공단_상가(상권)정보_서울_202409.csv 전처리

```jsx
restaurant_df = spark.read.csv('hdfs:///user/maria_dev/restaurant.csv', header=True, inferSchema=True)
```

- 모든 column 출력
- 띄어쓰기 중요

```jsx
column_names = restaurant_df.columns
for name in column_names:
     print(name)

```

- 불필요한 column 삭제

```jsx
columns_to_keep = ['상호명', '상권업종대분류코드', '상권업종대분류명', 
                   '상권업종중분류코드', '상권업종중분류명', '상권업종소분류코드', 
                   '상권업종소분류명', '경도', '위도']
restaurant_filtered_df = restaurant_df.select(columns_to_keep)
restaurant_filtered_df.show()
```

- 상권업종대분류코드 ‘I2’를 제외한 나머지 삭제 (’I2’ = 음식점)

```jsx
restaurant_final_df = restaurant_filtered_df.filter(restaurant_filtered_df['상권업종대분류코드'] == 'I2')
restaurant_final_df.show()
```

- 함수 입력 후 enter 두 번
- 함수 설명
    - math 모듈에서 radians, sin, cos, sqrt, atan2 함수를 가져옵니다. 이 함수들은 수학적 계산을 위해 사용
    - haversine라는 이름의 함수를 정의. 이 함수는 두 점의 위도(lat1, lat2)와 경도(lon1, lon2)를 매개변수로 받음.
    - 지구의 평균 반경을 미터 단위로 설정(약 6371km)
    - Haversine 공식을 적용하여 두 점 사이의 거리 계산을 위한 중간 변수를 계산
    - a = 두 점 간의 거리 계산, c = 두 점 간의 중심각 계산

```jsx
from math import radians, sin, cos, sqrt, atan2
def haversine(lat1, lon1, lat2, lon2):
    R = 6371000
    lat1, lon1, lat2, lon2 = map(radians, [lat1, lon1, lat2, lon2])
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = sin(dlat / 2)**2 + cos(lat1) * cos(lat2) * sin(dlon / 2)**2
    c = 2 * atan2(sqrt(a), sqrt(1 - a))
    return R * c

```

```jsx
university_filtered_df = university_df.select('위도', '경도', '반경', '대학명')
broadcast_universities = spark.sparkContext.broadcast(university_filtered_df.collect())
```

- 함수 입력 후 enter 두 번
- 함수 설명
    - 레스토랑의 위도(lat)와 경도(lon)를 매개변수로 받는다.
    - for문을 통해 각 대학의 정보를 가져온다
    - Haversine 공식을 사용하여 레스토랑과 해당 대학 간의 거리를 계산
    - 계산된 거리가 대학의 반경(radius) 이내인지 확인 조건이 참이면 해당 대학의 이름을 list에 추가

```jsx
def find_universities_for_restaurant(lat, lon):
    universities_within_radius = []
    for row in broadcast_universities.value:
        uni_lat = row['위도']
        uni_lon = row['경도']
        radius = row['반경']
        distance = haversine(lat, lon, uni_lat, uni_lon)
        if distance <= radius:
            universities_within_radius.append(row['대학명'])
    return ', '.join(universities_within_radius)
```

- find_universities_for_restaurant 함수를 UDF(User-Defined Function)로 등록. 이 UDF는 레스토랑의 위도와 경도를 입력 받아, 해당 위치 주변의 대학 이름을 문자열로 반환

```jsx
from pyspark.sql.types import StringType
find_universities_udf = F.udf(find_universities_for_restaurant, StringType())
restaurant_final_df = restaurant_final_df.withColumn("대학교", find_universities_udf(F.col("위도"), F.col("경도")))
restaurant_final_df = restaurant_final_df.filter(F.col("대학교") != "")
restaurant_final_df.show()
```

- 특정 대학교만 보기 원하면

```jsx

myeongji_universities_df = restaurant_final_df.filter(F.col("대학교").contains("명지대학교"))
myeongji_universities_df.show()
```

- 'hdfs:///user/maria_dev/restaurant_with_universities.csv’ 저장

```jsx
output_path = 'hdfs:///user/maria_dev/restaurant_with_universities'
restaurant_final_df.coalesce(1).write.csv(output_path, header=True, mode='overwrite')
```
