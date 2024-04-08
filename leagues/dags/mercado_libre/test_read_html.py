import requests
from bs4 import BeautifulSoup
import time
import random
import pandas as pd

URL_PC_GAMER_USADA = 'https://listado.mercadolibre.com.uy/computacion/pc-escritorio/computadoras-servidores/usado/pc-gamer_NoIndex_True#applied_filter_id%3DITEM_CONDITION%26applied_filter_name%3DCondici%C3%B3n%26applied_filter_order%3D5%26applied_value_id%3D2230581%26applied_value_name%3DUsado%26applied_value_order%3D3%26applied_value_results%3D52%26is_custom%3Dfalse'
URL_LAPTOP_GAMER_USADA = 'https://listado.mercadolibre.com.uy/computacion/laptops-accesorios/notebooks/usado/laptop-gamer_NoIndex_True#applied_filter_id%3DITEM_CONDITION%26applied_filter_name%3DCondici%C3%B3n%26applied_filter_order%3D5%26applied_value_id%3D2230581%26applied_value_name%3DUsado%26applied_value_order%3D3%26applied_value_results%3D30%26is_custom%3Dfalse'
URL_RTX_USADO = 'https://listado.mercadolibre.com.uy/computacion/componentes-pc/placas/tarjetas-video/usado/rtx_NoIndex_True#applied_filter_id%3DITEM_CONDITION%26applied_filter_name%3DCondici%C3%B3n%26applied_filter_order%3D5%26applied_value_id%3D2230581%26applied_value_name%3DUsado%26applied_value_order%3D3%26applied_value_results%3D18%26is_custom%3Dfalse'
URL_RX_USADO = 'https://listado.mercadolibre.com.uy/rx_ITEM*CONDITION_2230581_NoIndex_True#applied_filter_id%3DITEM_CONDITION%26applied_filter_name%3DCondici%C3%B3n%26applied_filter_order%3D7%26applied_value_id%3D2230581%26applied_value_name%3DUsado%26applied_value_order%3D3%26applied_value_results%3D110%26is_custom%3Dfalse'
URL_I5_USADO = 'https://listado.mercadolibre.com.uy/computacion/usado/i5_NoIndex_True#applied_filter_id%3DITEM_CONDITION%26applied_filter_name%3DCondici%C3%B3n%26applied_filter_order%3D4%26applied_value_id%3D2230581%26applied_value_name%3DUsado%26applied_value_order%3D3%26applied_value_results%3D161%26is_custom%3Dfalse'
URL_I7_USADO = 'https://listado.mercadolibre.com.uy/computacion/usado/i7_NoIndex_True#applied_filter_id%3DITEM_CONDITION%26applied_filter_name%3DCondici%C3%B3n%26applied_filter_order%3D4%26applied_value_id%3D2230581%26applied_value_name%3DUsado%26applied_value_order%3D3%26applied_value_results%3D96%26is_custom%3Dfalse'
URL_I9_USADO = 'https://listado.mercadolibre.com.uy/computacion/usado/i9_NoIndex_True#applied_filter_id%3DITEM_CONDITION%26applied_filter_name%3DCondici%C3%B3n%26applied_filter_order%3D3%26applied_value_id%3D2230581%26applied_value_name%3DUsado%26applied_value_order%3D3%26applied_value_results%3D4%26is_custom%3Dfalse'
URL_RYZEN_USADO = 'https://listado.mercadolibre.com.uy/computacion/usado/ryzen_NoIndex_True#applied_filter_id%3DITEM_CONDITION%26applied_filter_name%3DCondici%C3%B3n%26applied_filter_order%3D4%26applied_value_id%3D2230581%26applied_value_name%3DUsado%26applied_value_order%3D3%26applied_value_results%3D34%26is_custom%3Dfalse'

URLS_BUSQUEDA = [URL_PC_GAMER_USADA, URL_LAPTOP_GAMER_USADA, URL_RTX_USADO, URL_RX_USADO,
                 URL_I5_USADO, URL_I7_USADO, URL_I9_USADO, URL_RYZEN_USADO]

# URLS_BUSQUEDA = [URL_PC_GAMER_USADA]

EXCEL_NAME = 'datos-articulos-meli.xlsx'


def process_data(url):
    data = {}
    index_code = url['href'].find('MLU')
    data['id'] = url['href'][index_code:index_code + 13]
    data['nombre'] = url.find('h2', {'class': 'ui-search-item__title'}).text
    data['link'] = url['href']
    data['es_nuevo'] = True

    return data


def read_html():
    products_array = []

    # Obtengo la primer pagina de la busqueda
    for url_busqueda in URLS_BUSQUEDA:
        r = requests.get(url_busqueda)
        content = r.content

        soup = BeautifulSoup(content, 'html.parser')
        urls = soup.find_all('a', {'class': 'ui-search-item__group__element'})

        # Genero los datos para incluir al array de busqueda
        for url in urls:
            products_array.append(process_data(url))

        # Comienzo a iterar sobre al boton siguiente
        next_link = ''
        count = 0
        while next_link is not None or count < 50:
            count += 1
            print('Leyendo pagina ' + str(count))
            tiempo = [1, 3, 2]
            time.sleep(random.choice(tiempo))

            next_button = soup.find_all('li', {'class': 'andes-pagination__button--next'})
            if next_button is None or len(next_button) < 1:
                break

            next_link = soup.find_all('li', {'class': 'andes-pagination__button--next'})[0]. \
                find('a', {'class': 'andes-pagination__link'})['href']

            if next_link is None or len(next_link) < 1:
                break

            new_page = requests.get(next_link)

            soup = BeautifulSoup(new_page.content, 'html.parser')
            urls = soup.find_all('a', {'class': 'ui-search-item__group__element'})

            for url in urls:
                products_array.append(process_data(url))

    print('Cantidad de articulos: ', len(products_array))

    df_nuevo = pd.DataFrame(products_array)

    try:
        df_actual = pd.read_excel(EXCEL_NAME)

        for i in range(0, len(df_actual)):
            df_actual.loc[i, 'es_nuevo'] = False

        art_codes = df_actual['id'].values.tolist()

        for i in range(0, len(df_nuevo)):
            try:
                art_codes.index(df_nuevo.loc[i, 'id'])

            except:
                print('Art nuevo: ', df_nuevo.loc[i, 'id'])
                df_actual.loc[len(df_actual)] = df_nuevo.loc[i]

        df_actual.to_excel(EXCEL_NAME, index=False)

    except:
        print('Es la primer carga o se borro el archivo')
        df_nuevo.to_excel(EXCEL_NAME, index=False)


read_html()
