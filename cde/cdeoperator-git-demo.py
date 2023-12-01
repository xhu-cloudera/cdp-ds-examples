from dateutil import parser
from datetime import timedelta
from dateutil import parser
from airflow.utils import timezone
from airflow import DAG
from cloudera.cdp.airflow.operators.cde_operator import CDEJobRunOperator


default_args = {
    'retry_delay': timedelta(seconds=5),
    'depends_on_past': False,
}

git_demo_dag = DAG(
    'cdeoperator-git-demo',
    default_args=default_args,
    start_date=parser.isoparse("2022-05-11T20:20:04.268Z").replace(tzinfo=timezone.utc),
    # end_date=datetime.combine(datetime.today() + timedelta(1), datetime.min.time()),
    schedule_interval='@once',
    catchup=False,
    is_paused_upon_creation=False
)

ingest_step1 = CDEJobRunOperator(
    connection_id='cde_runtime_api',
    task_id='ingest',
    retries=3,
    dag=git_demo_dag,
    job_name='example-scala-pi'
)

prep_step2 = CDEJobRunOperator(
    task_id='data_prep',
    dag=git_demo_dag,
    job_name='example-scala-pi'
)

proc_step3 = CDEJobRunOperator(
    task_id='data_proc-1',
    dag=git_demo_dag,
    job_name='example-scala-pi'
)

proc_step4 = CDEJobRunOperator(
    task_id='data_proc-2',
    dag=git_demo_dag,
    job_name='example-scala-pi'
)

pres_step5 = CDEJobRunOperator(
    task_id='data_pres',
    dag=git_demo_dag,
    job_name='example-scala-pi'
)

proc_step6 = CDEJobRunOperator(
    task_id='data_proc-3',
    dag=git_demo_dag,
    job_name='example-scala-pi'
)

proc_step7 = CDEJobRunOperator(
    task_id='data_proc-4',
    dag=git_demo_dag,
    job_name='example-scala-pi'
)

# ingest_step1 >> prep_step2
ingest_step1 >> prep_step2 >> [proc_step3, proc_step4] >> pres_step5 >> [proc_step6, proc_step7]
