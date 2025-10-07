# tutorial_dag.py
import textwrap
from datetime import datetime, timedelta
from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator

# Step 2: Define the DAG
with DAG(
    "tutorial",
    default_args={
        "owner": "airflow",
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    description="A simple tutorial DAG",
    schedule=timedelta(days=1),
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["example"],
) as dag:

    # Task 1: Print the date
    t1 = BashOperator(
        task_id="print_date",
        bash_command="date",
    )

    # Task 2: Sleep for 5 seconds
    t2 = BashOperator(
        task_id="sleep",
        depends_on_past=False,
        bash_command="sleep 5",
        retries=3,
    )

    # Task 3: Templated command using Jinja
    templated_command = textwrap.dedent(
        """
        {% for i in range(5) %}
            echo "Loop {{ i + 1 }}: Execution date is {{ ds }}"
            echo "       Date +7 days: {{ macros.ds_add(ds, 7) }}"
        {% endfor %}
        """
    )

    t3 = BashOperator(
        task_id="templated",
        depends_on_past=False,
        bash_command=templated_command,
    )

    # Step 4: Set Task Dependencies
    # t1 runs first, then t2 and t3 run in parallel
    t1 >> [t2, t3]

    # Optional: Add Markdown documentation
    t1.doc_md = textwrap.dedent("""
        #### Task Documentation
        This task prints the current system date using the `date` command.
    """)

    t2.doc_md = textwrap.dedent("""
        #### Task Documentation
        This task sleeps for 5 seconds to simulate work.
        It will retry up to 3 times if interrupted.
    """)

    t3.doc_md = textwrap.dedent("""
        #### Task Documentation
        This task uses **Jinja templating** to:
        - Loop 5 times
        - Print the **execution date** (`{{ ds }}`)
        - Print the date **7 days in the future** using `macros.ds_add(ds, 7)`
    """)

    # Optional: Add DAG-level documentation
    dag.doc_md = textwrap.dedent("""
        ### Tutorial DAG Documentation
        This DAG demonstrates:
        - A simple sequential task (`t1`)
        - Two parallel tasks (`t2`, `t3`) triggered after `t1`
        - Jinja templating in BashOperator
        - Markdown documentation for tasks and DAG
    """)
    