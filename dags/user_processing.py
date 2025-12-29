from airflow.sdk import dag, task
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.sdk.bases.sensor import PokeReturnValue
from airflow.providers.standard.operators.python import PythonOperator

def _extract_user(ti):
    fake_user = ti.xcom_pull(task_ids = "is_api_available")
    return {
        "id" : fake_user["id"],
        "firstname" : fake_user["personalInfo"]["firstName"],
        "lastname" :fake_user["personalInfo"]["lastName"],
        "email" : fake_user["personalInfo"]["email"]
    }

# # testing _extract_user
# def _extract_user(ti):
#     import requests
#     response = requests.get("https://raw.githubusercontent.com/marclamberti/datasets/fcad9e032e8d0c54f8dd40830efd7858d73e92d2/fakeuser.json")
#     # print(response.status_code)
#     fake_user = response.json()
#     # print(fake_user)
#     return {
#         "id" : fake_user["id"],
#         "firstname" : fake_user["personalInfo"]["firstName"],
#         "lastname" :fake_user["personalInfo"]["lastName"],
#         "email" : fake_user["personalInfo"]["email"]
#     }
    
# # _extract_user()

@dag
def user_processing():
    create_table = SQLExecuteQueryOperator(
        task_id = "create_table",
        conn_id = "postgres",
        sql = """
                CREATE TABLE IF NOT EXISTS users(
                    id INT PRIMARY KEY,
                    firstname VARCHAR(255),
                    lastname VARCHAR(255),
                    email VARCHAR(255),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
        """
    )

    @task.sensor(poke_interval=30, timeout=300)  #two important parameters
    def is_api_available() -> PokeReturnValue:
        import requests
        response = requests.get("https://raw.githubusercontent.com/marclamberti/datasets/fcad9e032e8d0c54f8dd40830efd7858d73e92d2/fakeuser.json")
        print(response.status_code)
        if response.status_code == 200:
            condition = True
            fake_user = response.json()
        else:
            condition = False
            fake_user = None
        
        return PokeReturnValue(is_done = condition, xcom_value = fake_user)
    
    extract_user = PythonOperator(
        task_id = "extract_user",
        python_callable = _extract_user
    )
    
    is_api_available()

user_processing()






