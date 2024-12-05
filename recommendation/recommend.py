# -*- coding: utf-8 -*-

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, avg, concat, round, when, lit, abs
)
import sys

def createSparkSession():
    return SparkSession.builder \
        .appName("Recommend Categories") \
        .getOrCreate()

def univ_info_load(spark):
    info_file_path = "hdfs:///user/maria_dev/term_project/output/university_info.csv"
    info_df = spark.read.option("header", "true").csv(info_file_path)
    return info_df.withColumnRenamed("대학명", "대학교")

def univ_count_load(spark):
    count_file_path = "hdfs:///user/maria_dev/term_project/output/university_business_count.csv"
    return spark.read.option("header", "true").csv(count_file_path)

def join_on_univ(info_df, count_df):
    return count_df.join(
        info_df.select("대학교", "밀도(학생/km²)"),
        on="대학교",
        how="left"
    ).withColumn(
        "count_per_density", 
        col("count") / col("밀도(학생/km²)")
    ).drop("밀도(학생/km²)")

def group_by_codes(df):
    return df.groupBy(
        "상권업종중분류코드", "상권업종소분류코드"
    ).agg(
        avg("count_per_density").alias("avg_count_per_density")
    )

def recommend(univ_df, avg_df, target_univ):
    target_univ_df = univ_df.filter(col("대학교") == target_univ)
    
    compared_df = target_univ_df.join(
        avg_df,
        ["상권업종중분류코드", "상권업종소분류코드"]
    ).withColumn(
        "difference_from_avg",
        (col("count_per_density") - col("avg_count_per_density")) / col("avg_count_per_density")
    ).withColumn(
        "current_count",
        col("count")
    ).select(
        "상권업종중분류코드",
        "상권업종소분류코드",
        "difference_from_avg",
        "current_count"
    ).orderBy("difference_from_avg")

    return compared_df

def match_code_and_name(spark, recommended_df, criterion_value):
    classification_file_path = "hdfs:///user/maria_dev/term_project/output/classification_codes.csv"
    classification_df = spark.read.option("header", "true").csv(classification_file_path)

    return recommended_df.join(
        classification_df.select(
            "상권업종중분류코드", "상권업종중분류명",
            "상권업종소분류코드", "상권업종소분류명"
        ),
        ["상권업종중분류코드", "상권업종소분류코드"]
    ).withColumn(
        "percentage",
        col("difference_from_avg") * 100
    ).withColumn(
        "recommendation",
        when(col("difference_from_avg") < -criterion_value,
            concat(
                col("상권업종중분류명"), lit(" > "), col("상권업종소분류명"),
                lit("\n현재 "), col("current_count"), lit("개 운영 중"),
                lit("\n평균보다 "), round(abs(col("percentage")), 1), lit("% 적음\n")
            )
        ).when(col("difference_from_avg") > criterion_value,
            concat(
                col("상권업종중분류명"), lit(" > "), col("상권업종소분류명"),
                lit("\n현재 "), col("current_count"), lit("개 운영 중"),
                lit("\n평균보다 "), round(col("percentage"), 1), lit("% 많음\n")
            )
        )
    ).filter(col("recommendation").isNotNull()
    ).withColumn(
        "type",
        when(col("difference_from_avg") < -criterion_value, lit("recommended"))
        .otherwise(lit("excess"))
    ).select(
        "recommendation", "type"
    ).orderBy("type", col("percentage"))

def print_categorized_results(df):
    rows = df.collect()

    print("========== [추천 업종] ==========")
    recommended = [row for row in rows if row.type == "recommended"]
    if recommended:
        for row in recommended:
            print(row.recommendation)
    else:
        print("추천할 업종이 없습니다.")

    print("\n========== [과잉 업종] ==========")
    excess = [row for row in rows if row.type == "excess"]
    if excess:
        for row in excess:
            print(row.recommendation)
    else:
        print("과잉 업종이 없습니다.")

def print_description():
    description = """
    서울시 대학 근처 음식점 평균 개수를 기반으로 
        - 타겟 대학에서 창업을 추천할 만한 카테고리를 추천
        - 과잉 분포하고 있는 카테고리도 제공

    필요한 CSV 파일 및 위치:

        1. university_info.csv
           위치: hdfs:///user/maria_dev/term_project/output/
           내용: 대학교 정보 (대학명, 밀도(학생/km²) 등)

        2. university_business_count.csv
            위치: hdfs:///user/maria_dev/term_project/output/
            내용: 대학별 음식점 수 데이터

        3. classification_codes.csv
            위치: hdfs:///user/maria_dev/term_project/output/
            내용: 상권 업종 분류 코드 및 명칭
    """
    
    print(description)

def main():
    if len(sys.argv) != 2:
        print("Usage: spark-submit recommend.py <대학교명>")
        sys.exit(1)

    print_description()

    target_univ_name = sys.argv[1]
    criterion_value = 0.5
    print("추천/과잉 기준: {0}".format(criterion_value))

    spark = createSparkSession()
    
    info_df = univ_info_load(spark)
    count_df = univ_count_load(spark)
    joined_df = join_on_univ(info_df, count_df)
    grouped_df = group_by_codes(joined_df)

    recommended_df = recommend(joined_df, grouped_df, target_univ_name)
    result_df = match_code_and_name(spark, recommended_df, criterion_value)
    print_categorized_results(result_df)

    spark.stop()

if __name__=="__main__":
    main()