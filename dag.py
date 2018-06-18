import datetime as dt

from airflow import DAG
from airflow.operators import PythonOperator

from utils import scrape_web, scrape_pdf, add_to_dataset


# replace user with your username
base_dir = '/home/user/idsp_pipeline'
default_args = {
    'owner': 'user',
    'depends_on_past': False,
    'start_date': dt.datetime.strptime('2017-04-17T00:00:00', '%Y-%m-%dT%H:%M:%S'),
    'provide_context': True
}
dag = DAG('idsp_v1', default_args=default_args, schedule_interval='0 0 * * 2', max_active_runs=1)

web_scrape_task = PythonOperator(task_id='scrape_web', python_callable=scrape_web,
                                 op_kwargs={'base_dir': base_dir}, dag=dag)
pdf_scrape_task = PythonOperator(task_id='scrape_pdf', python_callable=scrape_pdf,
                                 op_kwargs={'base_dir': base_dir}, dag=dag)
add_to_dataset_task = PythonOperator(task_id='add_to_dataset', python_callable=add_to_dataset,
                                     op_kwargs={'base_dir': base_dir}, dag=dag)

# define the relationship between the tasks using set_downstream
web_scrape_task.set_downstream(pdf_scrape_task)
pdf_scrape_task.set_downstream(add_to_dataset_task)
