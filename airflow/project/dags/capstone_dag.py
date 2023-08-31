from datetime import datetime

from airflow import DAG
from airflow.providers.amazon.aws.operators.emr import (
    EmrServerlessCreateApplicationOperator,
    EmrServerlessStartJobOperator,
    EmrServerlessDeleteApplicationOperator,
)

# Replace these with your correct values
JOB_ROLE_ARN = "arn:aws:iam::309454309326:role/aws-service-role/ops.emr-serverless.amazonaws.com/AWSServiceRoleForAmazonEMRServerless"
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
    dag_id="capstone_dag",
    schedule_interval=None,
    start_date=datetime(2023, 1, 1),
    tags=["example"],
    catchup=False,
) as dag:
    create_app = EmrServerlessCreateApplicationOperator(
        task_id="create_spark_app",
        job_type="SPARK",
        release_label="emr-6.7.0",
        config={"name": "airflow-test"},
    )

    application_id = create_app.output

    job1 = EmrServerlessStartJobOperator(
        task_id="cleanClaimsData",
        application_id=application_id,
        execution_role_arn=JOB_ROLE_ARN,
        job_driver={
            "sparkSubmit": {
                "entryPoint": "s3://capstoneproject12345/jobs/cleanClaimsData.py",
            }
        },
        configuration_overrides=DEFAULT_MONITORING_CONFIG,
    )

    job2 = EmrServerlessStartJobOperator(
        task_id="cleanDiseaseData",
        application_id=application_id,
        execution_role_arn=JOB_ROLE_ARN,
        job_driver={
            "sparkSubmit": {
                "entryPoint": "s3://capstoneproject12345/jobs/cleanDiseaseData.py",
                "entryPointArguments": ["1000"]
            }
        },
        configuration_overrides=DEFAULT_MONITORING_CONFIG,
    )

    job3 = EmrServerlessStartJobOperator(
        task_id="cleanGroupData",
        application_id=application_id,
        execution_role_arn=JOB_ROLE_ARN,
        job_driver={
            "sparkSubmit": {
                "entryPoint": "s3://capstoneproject12345/jobs/cleanGroupData.py",
                "entryPointArguments": ["1000"]
            }
        },
        configuration_overrides=DEFAULT_MONITORING_CONFIG,
    )

    job4 = EmrServerlessStartJobOperator(
        task_id="cleanGrpsubgrpData",
        application_id=application_id,
        execution_role_arn=JOB_ROLE_ARN,
        job_driver={
            "sparkSubmit": {
                "entryPoint": "s3://capstoneproject12345/jobs/cleanGrpsubgrpData.py",
                "entryPointArguments": ["1000"]
            }
        },
        configuration_overrides=DEFAULT_MONITORING_CONFIG,
    )

    job5 = EmrServerlessStartJobOperator(
        task_id="cleanHospitalData",
        application_id=application_id,
        execution_role_arn=JOB_ROLE_ARN,
        job_driver={
            "sparkSubmit": {
                "entryPoint": "s3://capstoneproject12345/jobs/cleanHospitalData.py",
                "entryPointArguments": ["1000"]
            }
        },
        configuration_overrides=DEFAULT_MONITORING_CONFIG,
    )

    job6 = EmrServerlessStartJobOperator(
        task_id="cleanPatientrecordsData",
        application_id=application_id,
        execution_role_arn=JOB_ROLE_ARN,
        job_driver={
            "sparkSubmit": {
                "entryPoint": "s3://capstoneproject12345/jobs/cleanPatientrecordsData.py",
                "entryPointArguments": ["1000"]
            }
        },
        configuration_overrides=DEFAULT_MONITORING_CONFIG,
    )

    job7 = EmrServerlessStartJobOperator(
        task_id="cleanSubgroupData",
        application_id=application_id,
        execution_role_arn=JOB_ROLE_ARN,
        job_driver={
            "sparkSubmit": {
                "entryPoint": "s3://capstoneproject12345/jobs/cleanSubgroupData.py",
                "entryPointArguments": ["1000"]
            }
        },
        configuration_overrides=DEFAULT_MONITORING_CONFIG,
    )

    job8 = EmrServerlessStartJobOperator(
        task_id="cleanSubscriberData",
        application_id=application_id,
        execution_role_arn=JOB_ROLE_ARN,
        job_driver={
            "sparkSubmit": {
                "entryPoint": "s3://capstoneproject12345/jobs/cleanSubscriberData.py",
                "entryPointArguments": ["1000"]
            }
        },
        configuration_overrides=DEFAULT_MONITORING_CONFIG,
    )

    job9 = EmrServerlessStartJobOperator(
        task_id="resultCreationData",
        application_id=application_id,
        execution_role_arn=JOB_ROLE_ARN,
        job_driver={
            "sparkSubmit": {
                "entryPoint": "s3://capstoneproject12345/jobs/resultCreationData.py",
                "entryPointArguments": ["1000"]
            }
        },
        configuration_overrides=DEFAULT_MONITORING_CONFIG,
    )

    job10 = EmrServerlessStartJobOperator(
        task_id="subgroupanalysis.",
        application_id=application_id,
        execution_role_arn=JOB_ROLE_ARN,
        job_driver={
            "sparkSubmit": {
                "entryPoint": "s3://capstoneproject12345/jobs/subgroupanalysis.py",
                "entryPointArguments": ["1000"]
            }
        },
        configuration_overrides=DEFAULT_MONITORING_CONFIG,
    )


    delete_app = EmrServerlessDeleteApplicationOperator(
        task_id="delete_app",
        application_id=application_id,
        trigger_rule="all_done",
    )

    #(create_app >> [job1, job2, job3] >> delete_app)
    #(create_app >> job1 >> job2 >> job3 >> delete_app)

    (create_app >> job7 >> job9 >> delete_app)
    #(create_app >> [job7, job3] >> job9 >> delete_app)
    #(create_app >> [job7, job3] >> [job9, job10] >> delete_app)




