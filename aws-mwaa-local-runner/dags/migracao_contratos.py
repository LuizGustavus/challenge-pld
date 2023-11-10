from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from datetime import datetime, timedelta
from kubernetes.client import models as k8s
from airflow.kubernetes.secret import Secret

# Define default arguments
default_args = {
    'owner': 'luiz',
    'depends_on_past': False,
    'start_date': datetime(2023, 9, 1, 8, 0, 0),  # Start date and time
    'retries': 3,  # Retry each task three times
    'retry_delay': timedelta(minutes=1),  # Delay between retries
}

# Create the DAG
dag = DAG(
    'migracao_contratos',
    default_args=default_args,
    schedule_interval=timedelta(days=1),  # Daily schedule
    catchup=False,
    tags=['pld', 'contratos_inteligentes'],
)

# Define a task to extract data
extract_data_task = KubernetesPodOperator(
    task_id='extract_data',
    name='extract-data',
    image='python:3.10',
    namespace='default',
    cmds=['sh', '-c'],
    arguments=[
        'pip install -r /contratos-inteligentes/requirements/requirements.txt && python /contratos-inteligentes/scripts/extract.py'
    ],
    volumes=[
        k8s.V1Volume(
            name='contratos-inteligentes',
            host_path=k8s.V1HostPathVolumeSource(
                path='/contratos-inteligentes',
                type='Directory'
            )
        )
    ],
    volume_mounts=[
        k8s.V1VolumeMount(
            name='contratos-inteligentes',
            mount_path='/contratos-inteligentes'
        )
    ],
    get_logs=True,
    dag=dag,
)

# Define a task to transform data
transform_data_task = KubernetesPodOperator(
    task_id='transform_data',
    name='transform-data',
    image='python:3.10',
    namespace='default',
    cmds=['sh', '-c'],
    arguments=[
        'pip install -r /contratos-inteligentes/requirements/requirements.txt && python /contratos-inteligentes/scripts/transform.py'
    ],
    volumes=[
        k8s.V1Volume(
            name='contratos-inteligentes',
            host_path=k8s.V1HostPathVolumeSource(
                path='/contratos-inteligentes',
                type='Directory'
            )
        )
    ],
    volume_mounts=[
        k8s.V1VolumeMount(
            name='contratos-inteligentes',
            mount_path='/contratos-inteligentes'
        )
    ],
    get_logs=True,
    dag=dag,
)

# Define a task to load data
load_data_task = KubernetesPodOperator(
    task_id='load_data',
    name='load-data',
    image='python:3.10',
    namespace='default',
    cmds=['sh', '-c'],
    arguments=[
        'pip install -r /contratos-inteligentes/requirements/requirements.txt && python /contratos-inteligentes/scripts/load.py'
    ],
    secrets=[
        Secret("env", "POSTGRES_USER", "postgres-credentials", "POSTGRES_USER"),
        Secret("env", "POSTGRES_PASSWORD", "postgres-credentials", "POSTGRES_PASSWORD"),
        Secret("env", "POSTGRES_DB", "postgres-credentials", "POSTGRES_DB"),
        Secret("env", "POSTGRES_HOST", "postgres-credentials", "POSTGRES_HOST"),
        Secret("env", "POSTGRES_PORT", "postgres-credentials", "POSTGRES_PORT")
    ],
    volumes=[
        k8s.V1Volume(
            name='contratos-inteligentes',
            host_path=k8s.V1HostPathVolumeSource(
                path='/contratos-inteligentes',
                type='Directory'
            )
        )
    ],
    volume_mounts=[
        k8s.V1VolumeMount(
            name='contratos-inteligentes',
            mount_path='/contratos-inteligentes'
        )
    ],
    get_logs=True,
    dag=dag,
)

# Set task dependencies
extract_data_task >> transform_data_task >> load_data_task
