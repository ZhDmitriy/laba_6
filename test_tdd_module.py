from requests import RequestException
import time
import pandas as pd
import requests
import json
from loguru import logger
import os
import wget
import pytest

REPORT_MARKETING_PRICE_OZON = r"C:\Users\janev\OneDrive\Desktop\tableau_ozon\main\report_marketing_price.xlsx"
REPORT_OZON_PRODUCT_ID = r"C:/Users/janev/OneDrive/Desktop/tableau_ozon/main/report_goods.csv"
URL_BASE = "api-seller.ozon.ru"

# Модуль для дипломного проекта
HEADERS_SIMFER = {
    "Client-Id": "8404",
    "Api-Key": "b43e53ea-43bc-4814-aab3-9fcf3567e51f"
}

# headers for mbt account ozon
HEADERS_MBT = {
    "Client-Id": "194018",
    "Api-Key": "e3ca6e87-6a05-4f5c-88f4-02e4c80682a1"
}

# headers for delonghi account ozon
HEADERS_DELONGHI = {
    "Client-Id": "125803",
    "Api-Key": "71a22aeb-f82b-44cb-b979-e93c7cb05538"
}

# headers for gressel account ozon
HEADERS_GREESEL = {
    "Client-Id": "21429",
    "Api-Key": "e093c3e8-8b66-4005-8b26-f2bc95354fd2"
}

# headers for kitchengoods account ozon
HEADERS_KITCHENGOODS = {
    "Client-Id": "52527",
    "Api-Key": "9916c3ba-86a6-43c5-9da9-1cbb1e160f0e"
}


# https://drive.google.com/file/d/1TpMgjohvUjIKbst2QKDWlw1_R7TN8F1_/view?usp=sharing - TDD

# class ETLprocess:
#     def extract_transform(self):
#         pass
#     def extract_load(self):
#         pass
#     def extract(self):
#         pass
#     def transform(self):
#         pass
#     def load(self):
#         pass


def retry(func):
    def wrapper_exception_requests(*args, **kwargs):
        retries = [5, 15, 30, 45, 60, 90, 120]
        for second in retries:
            try:
                return func(*args, **kwargs)
            except RequestException:
                print(f"Failed requests on API OZON on {second} seconds in method {extract_transform}")
                time.sleep(second)
        return func(*args, **kwargs)

    return wrapper_exception_requests

@retry
def extract_transform(headers_http) -> pd.DataFrame:
    """ Функция получает маркетинговые цены канала Ozon """

    def get_report_goods(headers_http: dict, url_base: str) -> dict:
        """ Отправляем запрос для получения отчета по товарам """
        response_answer_api_ozon = requests.post(f'https://{url_base}/v1/report/products/create',
                                                 headers=headers_http)
        body_for_next_requests = response_answer_api_ozon.json()['result']
        return body_for_next_requests

    def get_link_report_goods(headers_http: dict, body: dict) -> dict:
        """ Отправляем запрос на получение ссылки для скачивания отчета по товарам """
        dict_result_report_goods = {}
        response_answer_api_ozon = requests.post(f"https://{URL_BASE}/v1/report/info", headers=headers_http,
                                                 data=json.dumps(body))
        answer = response_answer_api_ozon.json()['result']
        dict_result_report_goods['file'] = answer['file']
        dict_result_report_goods['created_at'] = answer['created_at']
        return dict_result_report_goods

    url = get_link_report_goods(headers_http, get_report_goods(headers_http, URL_BASE))['file']
    while url == "":
        url = get_link_report_goods(headers_http, get_report_goods(headers_http, URL_BASE))['file']
    if os.path.isfile(REPORT_OZON_PRODUCT_ID):
        os.remove(REPORT_OZON_PRODUCT_ID)
        print('success')
        logger.info("Скачали отчет по товарам с Ozon product ID {main_get_report_actual_marketing_price}")
    else:
        print("Error! File doesn't exists!")
        logger.error(
            "Отчет по товарам Ozon product ID не скачен или не найден {main_get_report_actual_marketing_price}")

    wget.download(url, REPORT_OZON_PRODUCT_ID)
    data_result_report_product = pd.read_csv(REPORT_OZON_PRODUCT_ID, sep=";")
    article_price = {}
    print(f"get_actual_marketing_price {headers_http}: ")
    count = 0
    for i_row in range(len(data_result_report_product)):
        count += 1
        if count == 20:
            break
        body = {
            "filter": {
                    "product_id": [
                        int(data_result_report_product.iloc[i_row]['Ozon Product ID'])
                    ],
                    "visibility": "ALL"
                },
                "last_id": "",
                "limit": 1000
            }
        # отправили запрос на API Ozon, чтобы получить маркетинговые цены Ozon
        result_post_marketing_prices = requests.post("https://api-seller.ozon.ru/v4/product/info/prices",
                                                         headers=headers_http, data=json.dumps(body)).json()
        marketing_price_ozon = result_post_marketing_prices['result']['items'][0]['price']['marketing_seller_price']
        logger.info("Получили маркетинговые цены без учета соинвеста {main_get_report_actual_marketing_price}")

        data_result = pd.DataFrame({'Артикул': article_price.keys(), 'Цена': article_price.values()})

    return [list(data_result.columns), all(list(data_result['Артикул'])), all(list(data_result['Цена']))]


print(extract_transform(HEADERS_SIMFER))


@pytest.mark.parametrize("exc_result", [([['Артикул', 'Цена'], True, True])])
def test_tdd_module(exc_result):
    assert extract_transform(HEADERS_SIMFER) == exc_result

