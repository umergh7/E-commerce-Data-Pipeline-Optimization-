import airflow
from airflow import DAG
from datetime import timedelta
from airflow.operators.python_operator import PythonOperator
from airflow.providers.amazon.aws.operators.emr import EmrAddStepsOperator
from airflow.providers.amazon.aws.operators.emr import EmrCreateJobFlowOperator
from airflow.providers.amazon.aws.operators.emr import EmrTerminateJobFlowOperator
from airflow.providers.amazon.aws.sensors.emr import EmrStepSensor
from airflow.providers.amazon.aws.sensors.emr import EmrJobFlowSensor



# this is just an example of how to use SPARK_STEPS, you need to define your own steps
SPARK_STEPS = [
    {
        'Name': 'wcd_data_engineer',
        'ActionOnFailure': "CONTINUE",
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': [
                'spark-submit',
                '--master', 'yarn',
                '--deploy-mode', 'cluster',
                's3://midterm-artifacts-ug/run.py', #s3 pyscript location
                '--spark_name', 'midterm',
                '--output_file_url', 's3://midterm-output-ug/transformation_output',  ## the S3 folder to send transformation
                '--output_file_url_dim', 's3://midterm-output-ug/dim_tables',
                '--sales', "{{ task_instance.xcom_pull('parse_request', key='sales') }}",
                '--calendar', "{{ task_instance.xcom_pull('parse_request', key='calendar') }}",
                '--inventory', "{{ task_instance.xcom_pull('parse_request', key='inventory') }}",
                '--product', "{{ task_instance.xcom_pull('parse_request', key='product') }}",
                '--store', "{{ task_instance.xcom_pull('parse_request', key='store') }}",
                

            ]
        }
    }

]


JOB_FLOW_OVERRIDES = {
    "Name": "midtermcluster",
    "ReleaseLabel": "emr-6.11.0",
    "Applications": [{"Name": "Spark"}],
    "Instances": {
        "InstanceGroups": [
            {
                "Name": "Primary node",
                "Market": "ON_DEMAND",
                "InstanceRole": "MASTER",
                "InstanceType": "m5.xlarge",
                "InstanceCount": 1,
            },
            {
                "Name": "Worker node",
                "Market": "ON_DEMAND",
                "InstanceRole": "CORE",
                "InstanceType": "m5.xlarge",
                "InstanceCount": 1,
            },
        ],
        "KeepJobFlowAliveWhenNoSteps": False,
        "TerminationProtected": False,
        "Ec2SubnetId": "subnet-0433e4e4a978ce175"
    },
    # "Steps": SPARK_STEPS,
    "JobFlowRole": "EMR_EC2_DefaultRole",
    "ServiceRole": "EMR_DefaultRole"
}




DEFAULT_ARGS = {
    'owner': 'wcd_data_engineer',
    'depends_on_past': False,
    'start_date': airflow.utils.dates.days_ago(0),
    'email': ['airflow_data_eng@wcd.com'],
    'email_on_failure': False,
    'email_on_retry': False
}

def retrieve_s3_files(**kwargs):    
    calendar = kwargs['dag_run'].conf['calendar']
    inventory = kwargs['dag_run'].conf['inventory']
    product = kwargs['dag_run'].conf['product']
    sales = kwargs['dag_run'].conf['sales']
    store = kwargs['dag_run'].conf['store']
    kwargs['ti'].xcom_push(key = 'calendar', value = calendar)
    kwargs['ti'].xcom_push(key = 'inventory', value = inventory)
    kwargs['ti'].xcom_push(key = 'product', value = product)
    kwargs['ti'].xcom_push(key = 'sales', value = sales)
    kwargs['ti'].xcom_push(key = 'store', value = store)



dag = DAG(
    'midterm_dag',
    default_args = DEFAULT_ARGS,
    dagrun_timeout = timedelta(hours=2),
    schedule_interval = None
)

parse_request = PythonOperator(task_id = 'parse_request',
                                provide_context = True, # Airflow will pass a set of keyword arguments that can be used in your function
                                python_callable = retrieve_s3_files,
                                dag = dag
                                ) 

create_job_flow = EmrCreateJobFlowOperator(
    task_id="create_job_flow",
    job_flow_overrides=JOB_FLOW_OVERRIDES,
    aws_conn_id = "aws_default",
    emr_conn_id = "emr_default",
    region_name = "ca-central-1",
    dag = dag
)

step_adder = EmrAddStepsOperator(
    task_id = 'add_steps',
    job_flow_id = create_job_flow.output,
    aws_conn_id = "aws_default",
    steps = SPARK_STEPS,
    dag = dag
)


step_checker = EmrStepSensor(
    task_id = 'watch_step',
    job_flow_id = create_job_flow.output,
    step_id = "{{ task_instance.xcom_pull('add_steps', key='return_value')[0] }}",
    dag = dag
)

create_job_flow.set_upstream(parse_request)
step_adder.set_upstream(create_job_flow)
step_checker.set_upstream(step_adder)

