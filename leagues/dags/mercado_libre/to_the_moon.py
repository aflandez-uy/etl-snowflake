from pendulum import datetime
import requests
from bs4 import BeautifulSoup

from airflow.decorators import (
    dag,
    task,
)

import pandas as pd

URLS_BASE = {
    'sin_espacio': 'https://listado.mercadolibre.com.uy/gamer#D[A:$]',
    'con_espacio': 'https://listado.mercadolibre.com.uy/pc-gamer#D[A:pc%20gamer]'
}

KEYS = ['Pc gamer', 'Laptop gamer', 'Rtx', 'Rx ', 'I5 ', 'I7', 'I9', 'Ryzen']


@dag(
    # This defines how often your DAG will run, or the schedule by which your DAG runs. In this case, this DAG
    # will run daily
    schedule=None,
    # This DAG is set to run for the first time on January 1, 2023. Best practice is to use a static
    # start_date. Subsequent DAG runs are instantiated based on the schedule
    start_date=datetime(2023, 1, 1),
    # When catchup=False, your DAG will only run the latest run that would have been scheduled. In this case, this means
    # that tasks will not be run between January 1, 2023 and 30 mins ago. When turned on, this DAG's first
    # run will be for the next 30 mins, per the its schedule
    catchup=False,
    default_args={
        "retries": 2,  # If a task fails, it will retry 2 times.
        "owner": 'jypmami'
    },
    tags=["mercado_libre"],
)  # If set, this tag is shown in the DAG view of the Airflow UI
def to_the_moon():

    @task()
    def get_keys():
        for key in KEYS:
            print(key)

    @task()
    def read_html():
        r = requests.get('https://listado.mercadolibre.com.uy/pc-gamer#D[A:pc%20gamer]')
        content = r.content

        soup = BeautifulSoup(content, 'html.parser')
        urls = soup.find_all('a', {'class': 'ui-search-item__group__element'})

        products_array = []

        for url in urls:
            data = {}
            data['id'] = url['href'][37:50]
            data['nombre'] = url.find('h2', {'class': 'ui-search-item__title'}).text
            data['link'] = url['href']
            products_array.append(data)

        print(products_array)

    get_keys()
    read_html()



to_the_moon()
