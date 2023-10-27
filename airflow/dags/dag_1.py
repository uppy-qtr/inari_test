import functools
import logging
import typing
import boto3
from datetime import timedelta, datetime

from airflow.decorators import dag,task
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.models import Variable
from airflow import AirflowException
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from kubernetes.client import models as k8s
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

DAG_NAME = "dev_ypopov_test"
DEFAULT_OP_ARGS = {
    'email': ['alert@monitoring.system'],   # add apropriate email for alerts
}
DEFAULT_PARAMETERS = {'s3-bucket-name': 'uppy-qtr-dev-bucket', 'output-s3-bucket-name': 'uppy-qtr-dev-output/output-raw/', 'sra-file': 'DRR147569', 'memory-request': '4G', 'memory-limit': '8G', 'cpu-request': 2, 'cpu-limit': 4, 'node-selector': 'spice-comp-general-purpose'}
PARAMETERS = {
    'output-s3-bucket-name',
    's3-bucket-name',
    'sra-file',
    'memory-request',
    'memory-limit',
    'cpu-request',
    'cpu-limit',
    'node-selector',
}
assert PARAMETERS.issubset(set(DEFAULT_PARAMETERS.keys()))      #default parameters should be imported from param file .json?

KUBE_CONFIG_CLUSTER_CONTEXT = 'aws'
KUBE_CONFIG_FILE_PATH = '/usr/local/airflow/dags/kube_config.yaml'  # already exists on AWS
PROJECT = 'pillar-sandbox'


ECR = '381094636682.dkr.ecr.us-east-1.amazonaws.com'        # use inari AWS account?
REPOSITORY_NAME = 'dev-ilgar-airflow-python-task'
RELEASE_VERSION = 'latest'
COMMON_KUBERNETES_KWARGS = dict(
    in_cluster=False,
    config_file=KUBE_CONFIG_FILE_PATH,
    cluster_context=KUBE_CONFIG_CLUSTER_CONTEXT,
)


@functools.cache
def var(name: str, **kwargs) -> typing.Any:             # check 
    airflow_var_name = f'{DAG_NAME}__{name}'
    try:
        val = Variable.get(airflow_var_name, **kwargs)
        logging.info(f'Using Airflow overriden value for variable: {name} ')
        return val
    except KeyError:
        return DEFAULT_PARAMETERS[name]


@dag(dag_id=DAG_NAME,
     description=__doc__,
     catchup=False,
     dagrun_timeout=timedelta(hours=1),
     default_args=DEFAULT_OP_ARGS,
     doc_md=__doc__,
     max_active_runs=1,
     schedule=None,
     start_date=datetime(2022, 1, 1),
     )
def Dag():
    namespace = PROJECT
    image = f'{ECR}/{REPOSITORY_NAME}:{RELEASE_VERSION}'
    task_name = 'fasterq-dump'


    @task
    def list_s3_files():
        s3_bucket_name = DEFAULT_PARAMETERS['s3-bucket-name']
        hook = S3Hook('s3_conn')
        try:
            paths = hook.list_keys(bucket_name=s3_bucket_name, prefix='input_raw')           # использовать s3 hook вместо boto3
            return paths
        except Exception as e:
            raise AirflowException(f"Failed to list S3 files: {str(e)}")
        

    @task
    def compare_list_of_files_with_parameters(list_of_files: list, default_params: dict) -> list:
        compared_list = []

        for file in list_of_files:
            if file in default_params['sra-file']:
                compared_list.append(file)

        return compared_list


    @task
    def download_from_s3(key: str, bucket_name: str, local_path: str) -> str:
        hook = S3Hook('s3_conn')
        file_name = hook.download_file(key=key, bucket_name=bucket_name, prefix='input_raw', local_path=local_path)
        return file_name



    run_this = BashOperator(
    task_id="run_after_loop",
    bash_command=f"echo {name}")



    @task
    def upload_to_output_s3_bucket (filename: str, key: str, bucket_name: str) -> None:
        hook = S3Hook('s3_conn')
        hook.load_file(filename=filename, key=key, bucket_name=bucket_name)


    @task
    def start_k8_pod_and_transform(key):    # поменять временно, s3 operator чтобы записать в другой бакет
        
        task_name = key.replace("/", "_")  

        fasterq_dump = KubernetesPodOperator(
            task_id=task_name,
            image=image,   # читать из переменных airflow, аналогично функции var (line 49)  / jinja2 expression, which reads airflow vars
            image_pull_policy='Always',
            namespace=namespace,
            name=f'{DAG_NAME}-{task_name}',
            random_name_suffix=True,
            labels={'env': 'env'},
            startup_timeout_seconds=720,
            reattach_on_restart=True,
            is_delete_operator_pod=True,
            get_logs=True,
            log_events_on_failure=True,
            do_xcom_push=True,
            env_vars=[
                k8s.V1EnvVar(
                    name='SRA_FILE',
                    value=var('sra-file'),  # эти ключи брать из xcom, которые идут из параметр файла
                ),
                k8s.V1EnvVar(
                    name='S3_BUCKET_NAME',
                    value=var('s3-bucket-name'),    # эти ключи брать из xcom, которые идут из параметр файла
                ),
                k8s.V1EnvVar(
                    name='OUTPUT_S3_BUCKET_NAME',
                    value=var('output-s3-bucket-name'), # эти ключи брать из xcom, которые идут из параметр файла
                )
            ],
            # volumes=[
                
                
            # ],
            # volume_mounts=[
                
            # ],
            
            
            container_resources=k8s.V1ResourceRequirements(
                limits={
                    'cpu': var('cpu-limit'),    # эти ключи брать из xcom, которые идут из параметр файла
                    'memory': var('memory-limit'),  # эти ключи брать из xcom, которые идут из параметр файла
                },
                requests={
                    'cpu': var('cpu-request'),  # эти ключи брать из xcom, которые идут из параметр файла
                    'memory': var('memory-request'),    # эти ключи брать из xcom, которые идут из параметр файла
                },
            ),
            retries=0,
            **COMMON_KUBERNETES_KWARGS,
        )


    list_of_files_input = list_s3_files() #отфильтровать по времени. со времени предыдущего запуска. Взять тайм интервал (airflow context)

    # list_checked = compare_list_of_files_with_parameters(
    #     list_of_files=list_of_files_input,
    #     default_params=DEFAULT_PARAMETERS
    # )

    files = download_from_s3.partial(
        bucket_name=DEFAULT_PARAMETERS['s3-bucket-name'], 
        local_path='/home/airflow/data').expand(
        key=list_of_files_input                    # скачивание и извлечение ключей из параметр файла (и вернуть ключи)
        )
    
    transformation = dummy_transformation.expand(filename=files)    # посмотреть другие операторы, отличные от питона (bash)
    # результаты чтения из 

    upload_to_output_s3_bucket(
        filename='/home/airflow/data/' + str(transformation), 
        bucket_name=DEFAULT_PARAMETERS['output-s3-bucket-name'],
        key=transformation
        )


dag = Dag()





####

# def download_from_s3(key: str, bucket_name: str, local_path: str) -> str:
#     hook = S3Hook('s3_conn')
#     file_name = hook.download_file(key=key, bucket_name=bucket_name, local_path=local_path)
#     return file_name

# def get_filenames() -> list:
    
#     session = boto3.Session( aws_access_key_id='<your_access_key_id>', aws_secret_access_key='<your_secret_access_key>')
#     s3 = session.resource('s3')

#     my_bucket = s3.Bucket(DEFAULT_PARAMETERS['s3-bucket-name'])
#     filenames = []

#     for my_bucket_object in my_bucket.objects.all():
#         filenames.append(my_bucket_object.key)

#     return filenames

# with DAG(
#     dag_id='file_processing_test',
#     schedule_interval='@daily',
#     start_date=datetime(2022, 3, 1),
#     catchup=False
# ) as dag:
#     task_get_list_of_files_on_s3 = PythonOperator(
#         task_id='get_list_of_files',
#         python_callable=get_filenames
#     )

    
#     task_download_from_s3 = PythonOperator(
#         task_id='download_from_s3',
#         python_callable=download_from_s3,
#         op_kwargs={
#             'key': DEFAULT_PARAMETERS['sra-file'],
#             'bucket_name': DEFAULT_PARAMETERS['s3-bucket-name'],
#             'local_path': '/home/airflow/data'
#         }
#     )
