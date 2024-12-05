# -*- coding: utf-8 -*-

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, regexp_replace, lit, when, trim, min, max
)
from math import pi
import subprocess

def createSparkSession():
    return SparkSession.builder \
        .appName("Radius And Density Calculator") \
        .getOrCreate()

def rename_output_file(temp_path, final_name):
    try:
        find_cmd = "hdfs dfs -ls {0} | grep .csv".format(temp_path) 
        result = subprocess.check_output(find_cmd, shell=True).decode()
        csv_file = result.strip().split()[-1]
                                        
        mv_cmd = "hdfs dfs -mv {0} {1}".format(csv_file, final_name)
        subprocess.check_output(mv_cmd, shell=True)
                                                                                
        rm_cmd = "hdfs dfs -rm -r {0}".format(temp_path)
        subprocess.check_output(rm_cmd, shell=True)

    except subprocess.CalledProcessError as e:
        print("Error renaming file: {0}".format(e))

def students_load(spark):
    file_path = "hdfs:///user/maria_dev/term_project/input/university_enrollment.csv"

    return spark.read.option("header", "true").csv(file_path)

def student_processing(student_df, location_df):
    joined_df = location_df \
        .join(student_df, location_df['대학명'] == student_df['학교'], 'left') \
        .select("대학명", "반경(m)", "총재학생수") \
        .na.fill({
            "총재학생수": 0
        })

    return calculate_density(joined_df)


def locations_load(spark):
    file_path = "hdfs:///user/maria_dev/term_project/input/university_locations.csv"

    df = spark.read.option("header", "true").option("encoding", "UTF-8").csv(file_path)

    khu_exists = df.filter(df['대학명'] == '경희대학교').count() > 0
    if not khu_exists:
        khu_data = spark.createDataFrame([
            (u'경희대학교', '407,376㎡', 37.59685, 127.0518)], ['대학명', '면적', '위도', '경도'
        ])
        df = df.union(khu_data)

    return df

def location_processing(df):
    # Rename
    df = df.withColumnRenamed('면적', '면적(㎡)')
    
    df = df.withColumn('면적(㎡)', regexp_replace(col('면적(㎡)'), r'\[.*?\]', ''))
    df = df.withColumn('대학명', trim(regexp_replace(col('대학명'), r'\[.*?\]', '')))
    
    df = df.withColumn('면적(㎡)', 
        when(col('면적(㎡)').isNull(), lit(0))
        .otherwise(regexp_replace(regexp_replace(col('면적(㎡)'), ',', ''), '[^0-9]', ''))
    )

    df = df.withColumn('면적(㎡)', col('면적(㎡)').cast('int'))
    
    df = df.withColumn('반경(m)', calculate_radius(col('면적(㎡)')))
    df = df.orderBy(col('면적(㎡)').desc())
    return df

def calculate_radius(area):
    return (
        when(area <= 100000, lit(500))
        .when(area > 1000000, lit(2000))
        .otherwise(
           lit(500) + ((area - 100000) / 100000 + 1).cast('int') * 100
        )
    )
                              
def calculate_density(df): 
    df.show(truncate=False)

    return df.withColumn(
        "밀도(학생/km²)", 
        when(
            (col("총재학생수").isNull()) | (col("반경(m)").isNull()) | 
            (col("총재학생수") == 0) | (col("반경(m)") == 0), 
            lit(0.0)
        ).otherwise(
            (col("총재학생수") / ((col("반경(m)") / 1000.0) ** 2 * pi))
        )
    )

def minmax_scale(df):
    density_stats = df.agg(
        min("밀도(학생/km²)").alias("min_density"),
        max("밀도(학생/km²)").alias("max_density")
    ).collect()[0]

    min_density = density_stats["min_density"]
    max_density = density_stats["max_density"]

    if max_density == min_density:
        return df.withColumn("밀도(학생/km²)", lit(0.0))
    else:
        return df.withColumn(
            "밀도(학생/km²)",
            (col("밀도(학생/km²)") - lit(min_density)) / lit(max_density - min_density)
    )

    return df.withColumn(
        "밀도(학생/km²)",
        when(max_density == min_density, lit(0.0))
        .otherwise((col("밀도(학생/km²)") - min_density) / (max_density - min_density))
    )

def save_result(df):
    temp_path = "hdfs:///user/maria_dev/term_project/temp_save"
    final_name = "hdfs:///user/maria_dev/term_project/output/university_info.csv"

    df.coalesce(1).write.mode("overwrite").csv(temp_path, header=True)
    rename_output_file(temp_path, final_name)

def main():
    spark = createSparkSession()
    
    student_df = students_load(spark)
    
    location_df = locations_load(spark)
    processed_loc_df = location_processing(location_df)
    processed_loc_df.show(truncate=False)

    processed_stu_df = student_processing(student_df, processed_loc_df)
    processed_stu_df.show(truncate=False)

    result_df = minmax_scale(processed_stu_df)
    result_df.show(truncate=False)

    save_result(result_df)
    spark.stop()

if __name__=="__main__":
    main()

