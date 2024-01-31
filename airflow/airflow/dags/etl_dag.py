from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago

def run_python_file(file_path):
    """
    Function to run a Python file.
    """
    try:
        exec(open(file_path).read(), globals())
        print(f'Successfully executed file: {file_path}')
    except Exception as e:
        print(f'Error executing file {file_path}: {str(e)}')

# Define the DAG
dag = DAG(
    'execute_python_files_dag',
    default_args={'start_date': days_ago(1)},
    schedule_interval='0 23 * * *',
    catchup=False
)

# List of Python files to execute
python_files_to_run = [
    r'C:\Users\havet\Desktop\workflow_1\extract.py',
    r'C:\Users\havet\Desktop\workflow_1\stagingArea.py',
    r'C:\Users\havet\Desktop\workflow_1\transform.py',
    r'C:\Users\havet\Desktop\workflow_1\division.py',
    r'C:\Users\havet\Desktop\workflow_1\load.py',
]

# Create a PythonOperator for each file
tasks = []
for i, file_path in enumerate(python_files_to_run):
    filename_without_extension = file_path.split('\\')[-1].split('.')[0]
    task_id = f'execute_python_file_{filename_without_extension}'
    python_task = PythonOperator(
        task_id=task_id,
        python_callable=run_python_file,
        op_kwargs={'file_path': file_path},
        dag=dag
    )
    tasks.append(python_task)

# Set the dependencies between the tasks
for i in range(len(tasks) - 1):
    tasks[i] >> tasks[i+1]
