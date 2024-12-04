import pandas as pd

# 데이터 불러오기
df = pd.read_csv('./Data/재적_학생_현황_(대학)_2024-12-04079849.csv')

# str -> int
df['총재학생수'] = df['총재학생수'].str.replace(',', '').astype(int)

# 중복 대학명 확인
duplicate_schools = df['학교'].str.replace(r' _제\d+캠퍼스$', '', regex=True).duplicated(keep=False)

# 중복되지 않은 대학만 캠퍼스 suffix 제거
df.loc[~duplicate_schools, '학교'] = df.loc[~duplicate_schools, '학교'].str.replace(r' _제\d+캠퍼스', '', regex=True)

result_df = df[['학교', '총재학생수']]

# 결과 저장
result_df.to_csv('./UnivInfo/enrollment/university_enrollment.csv', index=False)