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

    df_subgroup = spark.read.option("header", "true").csv("s3://takeo123/capstone/subgroup.csv")

    has_nulls = df_subgroup.dropna().count() < df_subgroup.count()
    if has_nulls:
        print("The dataset has null values.")
    else:
        print("The dataset does not have null values.")

    df = spark.read.format("redshift")\
        .option("url","jdbc:redshift://default-workgroup.309454309326.us-east-1.redshift-serverless.amazonaws.com:5439/dev")\
        .option("query", "select * from test.subgroup as s join test.group as g on s.gid = g.gid")\
        .option("driver", "com.amazon.redshift.jdbc42.Driver")\
        .option("user","admin").option("password", "Nepal123")\
        .option("tempdir", "s3a://takeo123/databrickstemp/projecttemp")\
        .option("aws_iam_role","arn:aws:iam::309454309326:role/redshiftAdmin").mode("overwrite").load()