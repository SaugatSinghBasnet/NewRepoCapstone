from datetime import datetime

from airflow import DAG
from airflow.providers.amazon.aws.operators.emr import (
    EmrServerlessCreateApplicationOperator,
    EmrServerlessStartJobOperator,
    EmrServerlessDeleteApplicationOperator,
)

# Replace these with your correct values
JOB_ROLE_ARN = "arn:aws:iam::309454309326:role/emrAdmin"
S3_LOGS_BUCKET = "capstoneproject12345"

DEFAULT_MONITORING_CONFIG = {
    "monitoringConfiguration": {
        "s3MonitoringConfiguration": {"logUri": f"s3://capstoneproject12345/logs/"}
    },
}


# def spark_params():
#     params_dict = {
#         # just some params to tweak your execution
#         'spark.jars.packages', 'com.databricks:spark-redshift_2.11:3.0.0-preview1'
#     }
#     # in my test run I provided both (spark.submit.pyFiles)
#     # pyfiles = f"--py-files s3://<UTILS-AS-ZIP>.zip"
#     configs = ' '.join([f"--conf {key}={params_dict[key]}" for key in params_dict])
#     return f"{configs}"


with DAG(
    dag_id="emr_redshift_dag",
    schedule_interval=None,
    start_date=datetime(2023, 1, 1),
    tags=["example"],
    catchup=False,
) as dag:


    application_id = "00fcrh3uek9uch09"

    job1 = EmrServerlessStartJobOperator(
        task_id="start_job_1",
        application_id=application_id,
        execution_role_arn=JOB_ROLE_ARN,
        job_driver={
            "sparkSubmit": {
                "entryPoint": "s3://capstoneproject12345/jobs/csvToparquet.py",
            }
        },
        configuration_overrides=DEFAULT_MONITORING_CONFIG,
    )

    job2 = EmrServerlessStartJobOperator(
        task_id="start_job_2",
        application_id=application_id,
        execution_role_arn=JOB_ROLE_ARN,
        job_driver={
            "sparkSubmit": {
                "entryPoint": "s3://capstoneproject12345/jobs/parquetToorc.py",
                "entryPointArguments": ["1000"]
            }
        },
        configuration_overrides=DEFAULT_MONITORING_CONFIG,
    )

    job3 = EmrServerlessStartJobOperator(
        task_id="write_to_redshift_job_3",
        application_id=application_id,
        execution_role_arn=JOB_ROLE_ARN,
        job_driver={
            "sparkSubmit": {
                "entryPoint": "s3://capstoneproject12345/jobs/orcTodisplay.py",
                "entryPointArguments": ["1000"]
            }
        },
        configuration_overrides=DEFAULT_MONITORING_CONFIG,
    )



    # delete_app = EmrServerlessDeleteApplicationOperator(
    #     task_id="delete_app",
    #     application_id=application_id,
    #     trigger_rule="all_done",
    # )

    #(create_app >> [job1, job2, job3] >> delete_app)
    ( job1 >> job2 >> job3 )