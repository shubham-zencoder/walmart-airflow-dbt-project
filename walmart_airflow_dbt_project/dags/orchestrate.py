from airflow.sdk import dag, task
from airflow.operators.bash import BashOperator
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.jobs import RunLifeCycleState, RunResultState
import pendulum
import time

@dag(
        dag_id = "orchestrate",
        schedule = "0 11 * * *",
        catchup = False,
        start_date = pendulum.datetime(year=2026, month=9, day=21, tz="Asia/Kolkata")
)
def orchestrate():

    @task
    def ingest_cdc():
        ws = WorkspaceClient(
            host = "****",
            token = "****"
        )

        job_trigger = ws.jobs.run_now(
            job_id = "120341995657698"
        )

        while True:
            job_status = ws.jobs.get_run(job_trigger.run_id)

            if job_status.state.life_cycle_state in [RunLifeCycleState.TERMINATED, RunLifeCycleState.SKIPPED, RunLifeCycleState.INTERNAL_ERROR]:
                if job_status.state.result_state == RunResultState.SUCCESS:
                    print("Job completed successfully")
                    break

                else: 
                    raise Exception(f"Job failed with state: {job_status.state.result_state}")
                
            time.sleep(5)

        return "CDC ingestion completed successfully"

    @task.bash
    def clear_target():
        return "rm -rf /opt/airflow/walmart_project/target && rm -rf /opt/airflow/walmart_project/logs"

    @task.bash
    def source_freshness():
        # Manually set the working directory using the 'cd' command before 'running'

        return "rm -rf /opt/airflow/walmart_project/target && cd /opt/airflow/walmart_project && dbt source freshness"

    silver_technical = BashOperator(
        task_id = "silver_technical",
        bash_command = "cd /opt/airflow/walmart_project && dbt run --select silver_t"
    )

    silver_technical_test = BashOperator(
        task_id = "silver_technical_test",
        cwd = "/opt/airflow/walmart_project",
        bash_command = "dbt test --select silver_t"
    )

    silver_business = BashOperator(
        task_id = "silver_business",
        cwd = "/opt/airflow/walmart_project",
        bash_command = "dbt run --select silver_b"
    )

    silver_business_test = BashOperator(
        task_id = "silver_business_test",
        cwd = "/opt/airflow/walmart_project",
        bash_command = "dbt test --select silver_b"
    )

    gold_ephemeral = BashOperator(
        task_id = "gold_ephemeral",
        cwd = "/opt/airflow/walmart_project",
        bash_command = "dbt run --select gold/ephemeral"
    ) 

    gold_dimension = BashOperator(
        task_id = "gold_dimension",
        cwd = "/opt/airflow/walmart_project",
        bash_command = "dbt snapshot"
    )

    gold_fact = BashOperator(
        task_id = "gold_fact",
        cwd = "/opt/airflow/walmart_project",
        bash_command = "dbt test --select gold/fact"
    )

    ingest_cdc() >> clear_target() >> source_freshness() >> silver_technical >> silver_technical_test >> silver_business >> silver_business_test >> gold_ephemeral >> gold_dimension >> gold_fact


orchestrate()