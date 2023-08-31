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

    df_Patient_records = spark.read.option("header", "true").csv("s3://takeo123/capstone/Patient_records.csv")

    has_nulls = df_Patient_records.dropna().count() < df_Patient_records.count()
    if has_nulls:
        print("The dataset has null values.")
    else:
        print("The dataset does not have null values.")

    from pyspark.sql import SparkSession
    from pyspark.sql.functions import col

    null_sums = []

    for col_name in df_Patient_records.columns:
        null_sum = df_Patient_records.filter(col(col_name).isNull()).count()
        null_sums.append((col_name, null_sum))

    print("Sum of null values in each column:")
    for col_name, null_sum in null_sums:
        print(f'Column "{col_name}": {null_sum}')

    df_Patient_records = df_Patient_records.na.fill("NA", subset=["Patient_name"])
    df_Patient_records = df_Patient_records.na.fill("NA", subset=["patient_phone"])

    df_Patient_records.show()

    df_Patient_records.write.format("redshift")\
        .option("url","jdbc:redshift://default-workgroup.309454309326.us-east-1.redshift-serverless.amazonaws.com:5439/dev")\
        .option("dbtable", "test.Patient_records")\
        .option("driver", "com.amazon.redshift.jdbc42.Driver")\
        .option("user","admin").option("password", "Nepal123")\
        .option("tempdir", "s3a://takeo123/databrickstemp/projecttemp")\
        .option("aws_iam_role","arn:aws:iam::309454309326:role/redshiftAdmin").mode("overwrite").save()