from airflow.sdk import dag, task
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.sdk.bases.sensor import PokeReturnValue
from airflow.providers.postgres.hooks.postgres import PostgresHook

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
    
    # using task - task decorator is python operator
    @task
    def extract_user(fake_user):
        return {
            "id" : fake_user["id"],
            "firstname" : fake_user["personalInfo"]["firstName"],
            "lastname" :fake_user["personalInfo"]["lastName"],
            "email" : fake_user["personalInfo"]["email"]
        }
    
    @task
    def process_user(user_info):

        # # can use csv module too
        # with open("user_info.csv", "a", newline = "", encoding = "utf-8") as ui:
            
        #     ui.write()

        # using pandas
        import pandas as pd
        from pathlib import Path

        # # test data
        # user_info = {
        #     "id" : "123",
        #     "firstname" : "John",
        #     "lastname" :"Doe",
        #     "email" : "johndoe@gmail.com"
        # }

        # test store_user()
        from datetime import datetime
        user_info["created_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        file_path = Path("/tmp/user_info.csv")
        df = pd.DataFrame([user_info])
        exists = file_path.exists()
        df.to_csv("/tmp/user_info.csv", mode = "a", index = False, header = not exists)

    @task
    def store_user():
        hook = PostgresHook(postgres_conn_id = "postgres")
        hook.copy_expert(
            sql="COPY users FROM STDIN WITH CSV HEADER",
            filename="/tmp/user_info.csv"
        )

    # process_user(extract_user(create_table >> is_api_available())) >> store_user()
    fake_user = is_api_available()
    user_info = extract_user(fake_user)
    processed = process_user(user_info)

    create_table >> fake_user
    processed >> store_user()

user_processing()

#test up git



















# #using python operator
# from airflow.providers.standard.operators.python import PythonOperator

# def _extract_user(ti):
#     fake_user = ti.xcom_pull(task_ids = "is_api_available")
#     return {
#         "id" : fake_user["id"],
#         "firstname" : fake_user["personalInfo"]["firstName"],
#         "lastname" :fake_user["personalInfo"]["lastName"],
#         "email" : fake_user["personalInfo"]["email"]
#     }

# # # testing _extract_user
# # def _extract_user(ti):
# #     import requests
# #     response = requests.get("https://raw.githubusercontent.com/marclamberti/datasets/fcad9e032e8d0c54f8dd40830efd7858d73e92d2/fakeuser.json")
# #     # print(response.status_code)
# #     fake_user = response.json()
# #     # print(fake_user)
# #     return {
# #         "id" : fake_user["id"],
# #         "firstname" : fake_user["personalInfo"]["firstName"],
# #         "lastname" :fake_user["personalInfo"]["lastName"],
# #         "email" : fake_user["personalInfo"]["email"]
# #     }
    
# # # _extract_user()


# extract_user = PythonOperator(
#         task_id = "extract_user",
#         python_callable = _extract_user
#     )


