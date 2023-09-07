#done
spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.access.key", "AKIAUQDHKA7HOJQ4MVRG")
spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.secret.key", "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")
df_claims=spark.read.option("header","true").json("s3://takeo123/capstone/claims.json")
df_claims.show()

has_nulls = df_claims.dropna().count() < df_claims.count()
if has_nulls:
    print("The dataset has null values.")
else:
    print("The dataset does not have null values.")

df_claims.write.format("redshift")\
    .option("url", "jdbc:redshift://default-workgroup.309454309326.us-east-1.redshift-serverless.amazonaws.com:5439/dev")\
    .option("dbtable", "test.claims")\
    .option("driver","com.amazon.redshift.jdbc42.Driver")\
    .option("user", "admin").option("password", "Nepal123")\
    .option("tempdir", "s3a://takeo123/databrickstemp/projecttemp")\
    .option("aws_iam_role", "arn:aws:iam::309454309326:role/redshiftAdmin").mode("overwrite").save()







spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.access.key", "AKIAUQDHKA7HOJQ4MVRG")
spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.secret.key", "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")
df_disease=spark.read.option("header","true").csv("s3://takeo123/capstone/disease.csv")
df_disease.show()

has_nulls = df_disease.dropna().count() < df_disease.count()
if has_nulls:
    print("The dataset has null values.")
else:
    print("The dataset does not have null values.")

df_disease = df_disease.withColumnRenamed(" Disease_ID", "Disease_ID")
df_disease.write.format("redshift")\
    .option("url", "jdbc:redshift://default-workgroup.309454309326.us-east-1.redshift-serverless.amazonaws.com:5439/dev")\
    .option("dbtable", "test.disease")\
    .option("driver","com.amazon.redshift.jdbc42.Driver")\
    .option("user", "admin").option("password", "Nepal123")\
    .option("tempdir", "s3a://takeo123/databrickstemp/projecttemp")\
    .option("aws_iam_role", "arn:aws:iam::309454309326:role/redshiftAdmin").mode("overwrite").save()





spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.access.key", "AKIAUQDHKA7HOJQ4MVRG")
spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.secret.key", "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")
df_group=spark.read.option("header","true").csv("s3://takeo123/capstone/group.csv")
df_group.show()

has_nulls = df_group.dropna().count() < df_group.count()
if has_nulls:
    print("The dataset has null values.")
else:
    print("The dataset does not have null values.")

df_group.write.format("redshift")\
    .option("url", "jdbc:redshift://default-workgroup.309454309326.us-east-1.redshift-serverless.amazonaws.com:5439/dev")\
    .option("dbtable", "test.group")\
    .option("driver","com.amazon.redshift.jdbc42.Driver")\
    .option("user", "admin").option("password", "Nepal123")\
    .option("tempdir", "s3a://takeo123/databrickstemp/projecttemp")\
    .option("aws_iam_role", "arn:aws:iam::309454309326:role/redshiftAdmin").mode("overwrite").save()


spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.access.key", "AKIAUQDHKA7HOJQ4MVRG")
spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.secret.key", "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")
df_grpsubgrp=spark.read.option("header","true").csv("s3://takeo123/capstone/grpsubgrp.csv")
df_grpsubgrp.show()

has_nulls = df_grpsubgrp.dropna().count() < df_grpsubgrp.count()
if has_nulls:
    print("The dataset has null values.")
else:
    print("The dataset does not have null values.")

df_grpsubgrp.write.format("redshift")\
    .option("url", "jdbc:redshift://default-workgroup.309454309326.us-east-1.redshift-serverless.amazonaws.com:5439/dev")\
    .option("dbtable", "test.grpsubgrp")\
    .option("driver","com.amazon.redshift.jdbc42.Driver")\
    .option("user", "admin").option("password", "Nepal123")\
    .option("tempdir", "s3a://takeo123/databrickstemp/projecttemp")\
    .option("aws_iam_role", "arn:aws:iam::309454309326:role/redshiftAdmin").mode("overwrite").save()


spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.access.key", "AKIAUQDHKA7HOJQ4MVRG")
spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.secret.key", "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")
df_hospital=spark.read.option("header","true").csv("s3://takeo123/capstone/hospital (2).csv")
df_hospital.show()

has_nulls = df_hospital.dropna().count() < df_hospital.count()
if has_nulls:
    print("The dataset has null values.")
else:
    print("The dataset does not have null values.")

df_hospital.write.format("redshift")\
    .option("url", "jdbc:redshift://default-workgroup.309454309326.us-east-1.redshift-serverless.amazonaws.com:5439/dev")\
    .option("dbtable", "test.hospital")\
    .option("driver","com.amazon.redshift.jdbc42.Driver")\
    .option("user", "admin").option("password", "Nepal123")\
    .option("tempdir", "s3a://takeo123/databrickstemp/projecttemp")\
    .option("aws_iam_role", "arn:aws:iam::309454309326:role/redshiftAdmin").mode("overwrite").save()


spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.access.key", "AKIAUQDHKA7HOJQ4MVRG")
spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.secret.key", "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")
df_Patient_records=spark.read.option("header","true").csv("s3://takeo123/capstone/Patient_records.csv")
df_Patient_records.show()

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
    .option("url", "jdbc:redshift://default-workgroup.309454309326.us-east-1.redshift-serverless.amazonaws.com:5439/dev")\
    .option("dbtable", "test.Patient_records")\
    .option("driver","com.amazon.redshift.jdbc42.Driver")\
    .option("user", "admin").option("password", "Nepal123")\
    .option("tempdir", "s3a://takeo123/databrickstemp/projecttemp")\
    .option("aws_iam_role", "arn:aws:iam::309454309326:role/redshiftAdmin").mode("overwrite").save()

#done
spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.access.key", "AKIAUQDHKA7HOJQ4MVRG")
spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.secret.key", "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")
df_subgroup=spark.read.option("header","true").csv("s3://takeo123/capstone/subgroup.csv")
df_subgroup.show()

has_nulls = df_subgroup.dropna().count() < df_subgroup.count()
if has_nulls:
    print("The dataset has null values.")
else:
    print("The dataset does not have null values.")

df_subgroup.write.format("redshift")\
    .option("url", "jdbc:redshift://default-workgroup.309454309326.us-east-1.redshift-serverless.amazonaws.com:5439/dev")\
    .option("dbtable", "test.subgroup")\
    .option("driver","com.amazon.redshift.jdbc42.Driver")\
    .option("user", "admin").option("password", "Nepal123")\
    .option("tempdir", "s3a://takeo123/databrickstemp/projecttemp")\
    .option("aws_iam_role", "arn:aws:iam::309454309326:role/redshiftAdmin").mode("overwrite").save()



#done
spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.access.key", "AKIAUQDHKA7HOJQ4MVRG")
spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.secret.key", "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")
df_subscriber=spark.read.option("header","true").csv("s3://takeo123/capstone/subscriber.csv")
df_subscriber.show()

has_nulls = df_subscriber.dropna().count() < df_subscriber.count()
if has_nulls:
    print("The dataset has null values.")
else:
    print("The dataset does not have null values.")

df_subscriber=df_subscriber.withColumnRenamed("sub _id", "sub_id")
df_subscriber=df_subscriber.withColumnRenamed("Zip Code", "ZipCode")

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
    .option("url", "jdbc:redshift://default-workgroup.309454309326.us-east-1.redshift-serverless.amazonaws.com:5439/dev")\
    .option("dbtable", "test.subscriber")\
    .option("driver","com.amazon.redshift.jdbc42.Driver")\
    .option("user", "admin").option("password", "Nepal123")\
    .option("tempdir", "s3a://takeo123/databrickstemp/projecttemp")\
    .option("aws_iam_role", "arn:aws:iam::309454309326:role/redshiftAdmin").mode("overwrite").save()





with diseasecte as (select disease_name,
ROW_NUMBER() OVER (PARTITION BY disease_name ORDER BY disease_name)cnt
from test.claims)
    , diseasecte2 as (select max(cnt) cnt from diseasecte)

    select b.disease_name, b.cnt from diseasecte2 a
    inner join diseasecte b
    on a.cnt = b.cnt




select birth_date, first_name, last_name from test.subscriber
where birth_date > DATEADD(year, -30, GETDATE())
and sub_id is not null




with sgcte as (select grp_id,ROW_NUMBER() OVER (PARTITION BY grp_id ORDER BY grp_id)cnt
from test.groupsubgroup) , sgcte1 as
    (select max(cnt) cnt from sgcte)

    select a.grp_id, a.cnt from sgcte a
    inner join sgcte1 b
    on a.cnt=b.cnt




with hospcte as (select h.hospital_name,
rank() OVER (PARTITION BY p.hospital_id
oRDER BY p.patient_id
)cnt
    from test.patient p
    join test.hospital h
on p.hospital_id=h.hospital_id), hospcte1 as
(select max(cnt) cnt from hospcte)

select a.hospital_name, a.cnt from hospcte a
inner join hospcte1 b
on a.cnt = b.cnt;




select count(*) from test.claims where claim_or_rejected='N'




with CLAIMS as (select city, ROW_NUMBER() OVER (PARTITION BY city ORDER BY claim_id)cnt
    from test.claims  c
    join test.patient p on c.patient_id=p.patient_id), CLAIMS1 as
        (select max(cnt) cnt
        from CLAIMS)
        select a.city, b.cnt
        from CLAIMS a
        inner join CLAIMS1 b
        on a.cnt = b.cnt




select grp_type,count(grp_id) from test.group group by grp_type




SELECT  sg.subgrp_id, Avg(monthly_premium)
FROM  test.subscriber s
INNER JOIN test.subgroup sg ON s.subgrp_id = sg.subgrp_id
GROUP BY sg.subgrp_id




with cte as (
select max(premium_written) maxprofit from test.group
)
select g.grp_name,premium_written from cte c
inner join test.group g
on g.premium_written=c.maxprofit




select * from test.patient
where disease_name like '%cancer%'
and patient_birth_date > DATEADD(year, -18, GETDATE())




select * from test.patient p
join test.claims c
on p.patient_id=c.patient_id
where c.claim_amount>=50000




select * from test.patient p
where
patient_gender = 'Female'
and patient_birth_date < DATEADD(year, -40, GETDATE())
and disease_name like '%surgery%'









