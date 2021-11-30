from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from pendulum import timezone

default_args = {
    "owner": "premier_league_docker",
    "depends_on_past": False,
    "email": ["k7ragav@gmail.com"],
    "email_on_failure": True,
    "email_on_success": True,
    "retries": 1,
    "retry_delay": timedelta(minutes=15),
    "catchup": True,
}
intervals = {
    "daily_at_8am": "0 8 */1 * *",
    "daily_at_7am": "0 7 */1 * *",
    "every_3_days": "0 0 */3 * *",
}
bash_command = "docker exec premier_league_docker python {{ task.task_id }}.py "

with DAG(
        "premier_league_table",
        description="testing_airflow",
        default_args=default_args,
        schedule_interval=intervals["every_3_days"],
        start_date=datetime(2021, 11, 27, tzinfo=timezone("Europe/Amsterdam")),
) as premier_league_table_dag:
    premier_league_table_task = BashOperator(
        task_id="premier_league_table",
        bash_command=bash_command,
    )
