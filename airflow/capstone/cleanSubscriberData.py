from pyspark.sql import SparkSession


def print_hi(name):
    # Use a breakpoint in the code line below to debug your script.
    print(f'Hi, {name}')  # Press ⌘F8 to toggle the breakpoint.


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    print_hi('PyCharm')
    spark: SparkSession = SparkSession.builder.getOrCreate()
    # See PyCharm help at https://www.jetbrains.com/help/pycharm/
    #   spark: SparkSession = SparkSession.builder.config("spark.jars.packages", "com.amazonaws:aws-java-sdk-pom:1.10.34,org.apache.hadoop:hadoop-aws:3.3.3").master("local[*]").appName(
    #       "SparkByExamples.com").getOrCreate()

    df_subscriber = spark.read.option("header", "true").csv("s3://takeo123/capstone/subscriber.csv")

    has_nulls = df_subscriber.dropna().count() < df_subscriber.count()
    if has_nulls:
        print("The dataset has null values.")
    else:
        print("The dataset does not have null values.")

    df_subscriber = df_subscriber.withColumnRenamed("sub _id", "sub_id")
    df_subscriber = df_subscriber.withColumnRenamed("Zip Code", "ZipCode")

    from pyspark.sql import SparkSession
    from pyspark.sql.functions import col

    null_sums = []

    for col_name in df_subscriber.columns:
        null_sum = df_subscriber.filter(col(col_name).isNull()).count()
        null_sums.append((col_name, null_sum))

    print("Sum of null values in each column:")
    for col_name, null_sum in null_sums:
        print(f'Column "{col_name}": {null_sum}')

    df_subscriber = df_subscriber.na.fill("NA", subset=["first_name"])
    df_subscriber = df_subscriber.na.fill("NA", subset=["Phone"])
    df_subscriber = df_subscriber.na.fill("NA", subset=["Subgrp_id"])
    df_subscriber = df_subscriber.na.fill("NA", subset=["Elig_ind"])

    df_subscriber.show()

    df_subscriber.write.format("redshift")\
        .option("url","jdbc:redshift://default-workgroup.309454309326.us-east-1.redshift-serverless.amazonaws.com:5439/dev")\
        .option("dbtable", "test.subscriber")\
        .option("driver", "com.amazon.redshift.jdbc42.Driver")\
        .option("user","admin").option("password", "Nepal123")\
        .option("tempdir", "s3a://takeo123/databrickstemp/projecttemp")\
        .option("aws_iam_role","arn:aws:iam::309454309326:role/redshiftAdmin").mode("overwrite").save()