import pytz
import yaml
from datetime import datetime
from airflow.models.param import Param
from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from resources.scripts.assignment.extract_assignment import extract_table
from resources.scripts.assignment.load_assignment import load_table

with open("dags/resources/config/assignment.yaml", "r") as f:
   config = yaml.safe_load(f)

@dag(
   schedule_interval = "*/120 09-21 * * FRI#2,FRI#4",
   start_date        = datetime(2024, 12, 1, tzinfo=pytz.timezone("Asia/Jakarta")),
   catchup           = False,
   params            = {
       table: Param("full", description="incremental / full", enum=["full", "incremental"]
)
       for table in config["ingestion"]
   }
)
def assignmentd15():
   start_task          = EmptyOperator(task_id="start_task")
   end_task            = EmptyOperator(task_id="end_task")
   wait_el_task        = EmptyOperator(task_id="wait_el_task")


   # Membuat task EL (Extract & Load) secara dinamis berdasarkan konfigurasi
   for table in config.get("ingestion", []):
       extract = task(extract_table, task_id=f"extract.{table}")
       load    = task(load_table, task_id=f"load.{table}")

       start_task >> extract(table) >> load(table) >> wait_el_task

   # Membuat task transformasi secara dinamis berdasarkan konfigurasi
   for filepath in config.get("transformation", []):
       transform = SQLExecuteQueryOperator(
           task_id = f"transform.{filepath.split('/')[-1]}",
           conn_id = "postgres_dibimbing",
           sql     = filepath,
       )

       wait_el_task >> transform >> end_task



assignmentd15()






