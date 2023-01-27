try:
    from datetime import timedelta
    from airflow import DAG
    from airflow .operators.python_operator import PythonOperator
    from datetime import datetime

    print(" all modules ok bro")

except Exception as e:
    print(" Error  {}.format(e)") 

def hi_func(*args, **kwargs):
    variable= kwargs.get("yti", " 1st didn't get the key")

    print(" hi func executing ")
    print("hi personal  {}".format(variable))
    kwargs['ti'].xcom_push(key='mykey', value=" inter5nal ")
    # return "hell personal " + variable

def bye_func(*args, **kwargs):
    variable= kwargs.get("name", " 2nd didn't get the key")

    print(" bye func executing ")
    print("bye personal  {}".format(variable))
    instance=kwargs.get('ti').xcom_pull(key='mykey')
    print(" i am bye {} this value from hi func ".format(instance))
    # return "heaven personal " + variable

#cron expression
# */2 * * *  * execute every two minutes

with DAG(

    dag_id="hi_dag",
    schedule_interval="@daily",
    default_args={
        "owner": "HASAN",
        "retries": 3,
        "retry_delay": timedelta(minutes=5),
        "start_date": datetime(2023,1,12),

    },
    catchup=False

    ) as f:

    hi_func = PythonOperator(
        task_id="hi_func_executing",
        python_callable=hi_func,
        provide_context=True,
        op_kwargs={"name":"air_condition","yti":"extern500al"}  
        #,"ti":"intern5al"
    )

    bye_func = PythonOperator(
        task_id="bye_func_executing",
        python_callable=bye_func,
        provide_context=True,
        op_kwargs={"name":"_plasma_"}
    )


hi_func >> bye_func




