from requests import RequestException
import time
import pandas as pd
import requests
import json
from loguru import logger
import os
import wget
import doctest

#
# result_words = """She sells sea shells on the sea shore; The shells that she sells are sea shells I'm sure. So if she sells sea shells on the sea shore, I'm sure that the shells are sea shore shells."""
# signs_not_in_words = [";", ".", "!", "?", ","]

# """She sells sea"""
# """She sells sea shells on the"""
# """The shells that she sells are"""
#
# def clean_word_list(result_words: str, signs_not_in_words: list) -> list:
#     """
#     Функция очищает список от ненужных знаков препинания
#     """
#     result_words_list = result_words.split(" ")
#     clean_result_words = []
#     for word in result_words_list:
#         for sign in signs_not_in_words:
#             if sign in word:
#                 word = word.replace(sign, "")
#         clean_result_words.append(word)
#     return clean_result_words
#
#
# def count_word_history(result_words: list) -> list:
#     """
#     Функция считает сколько раз встретилось слово в истории перебора
#     >>> count_word_history(clean_word_list('''She sells sea''', [";", ".", "!", "?", ","]))
#     [0, 0, 0]
#     >>> count_word_history(clean_word_list('''She sells She shells on the''', [";", ".", "!", "?", ","]))
#     [0, 0, 1, 0, 0, 0]
#     >>> count_word_history(clean_word_list('''The shells that she sells are''', [";", ".", "!", "?", ","]))
#     [0, 0, 0, 0, 0, 0]
#     """
#     dict_words = {}
#     answer_counter_word = []
#
#     for word in result_words:
#         if word not in dict_words:
#             dict_words[word] = 0
#         else:
#             dict_words[word] += 1
#         answer_counter_word.append(dict_words[word])
#
#     return answer_counter_word



# # Задание 2 Doctest
# result_words = """She sells sea shells on the sea shore; The shells that she sells are sea shells I'm sure. So if she sells sea shells on the sea shore, I'm sure that the shells are sea shore shells."""
# signs_not_in_words = [";", ".", "!", "?", ","]
#
#
# def clean_word_list(result_words: str, signs_not_in_words: list) -> list:
#     result_words_list = result_words.split(" ")
#     clean_result_words = []
#     for word in result_words_list:
#         for sign in signs_not_in_words:
#             if sign in word:
#                 word = word.replace(sign, "")
#         clean_result_words.append(word)
#     return clean_result_words
#
#
# def count_word_history(result_words: list) -> list:
#     dict_words = {}
#     answer_counter_word = []
#
#     for word in result_words:
#         if word not in dict_words:
#             dict_words[word] = 0
#         else:
#             dict_words[word] += 1
#         answer_counter_word.append(dict_words[word])
#
#     return answer_counter_word
#
#
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

REPORT_MARKETING_PRICE_OZON = r"C:\Users\janev\OneDrive\Desktop\tableau_ozon\main\report_marketing_price.xlsx"
REPORT_OZON_PRODUCT_ID = r"C:/Users/janev/OneDrive/Desktop/tableau_ozon/main/report_goods.csv"
URL_BASE = "api-seller.ozon.ru"


class ETLprocess:
    def extract_transform(self):
        pass

    def extract_load(self):
        pass

    def extract(self):
        pass

    def transform(self):
        pass

    def load(self):
        pass


class OzonMarketingPrice(ETLprocess):

    def retry(func):
        def wrapper_exception_requests(*args, **kwargs):
            retries = [5, 15, 30, 45, 60, 90, 120]
            for second in retries:
                try:
                    return func(*args, **kwargs)
                except RequestException:
                    print(f"Failed requests on API OZON on {second} seconds in method {OzonMarketingPrice}")
                    time.sleep(second)
            return func(*args, **kwargs)

        return wrapper_exception_requests

    @retry
    def extract_transform(self, headers_http) -> pd.DataFrame:

        self.headers_http = headers_http

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
        for i_row in range(len(data_result_report_product)):
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
            print(marketing_price_ozon)
        logger.info("Получили маркетинговые цены без учета соинвеста {main_get_report_actual_marketing_price}")

        return pd.DataFrame({'Артикул': article_price.keys(), 'Цена': article_price.values()}).to_excel(REPORT_MARKETING_PRICE_OZON, index=False)


marketing_price = OzonMarketingPrice()
marketing_price.extract_transform(HEADERS_SIMFER)
#
#


def extract_data_price() -> pd.DataFrame:
    """
    Функция получает цены в сопоставлении с sku из ЛК Яндекс.Маркет
    >>> extract_data_price()
    (['sku', 'price', 'date'], True)
    """
    sku_list = []
    price_list = []
    date_list = []

    headers = {
        'Authorization': 'Bearer y0_AgAAAAAwy8GyAAqafQAAAADuVES0dzOy_1rARlGU3LSBRVT0QoDJ9FA'
    }

    body_result = {"offerIds": ["121787"]}

    result = requests.get('https://api.partner.market.yandex.ru/campaigns/23415831/offer-prices?page_token=&limit=940',
                          headers=headers, data=body_result)

    for item in result.json()['result']['offers']:
        sku_list.append(item['id'])  # sku товара
        price_list.append(item['price']['value'])  # цена товара
        date_list.append(item['updatedAt'])  # цена на дату

    data_result = {'sku': sku_list, 'price': price_list, 'date': date_list}
    data_result = pd.DataFrame(data_result)

    return list(data_result.columns), all(list(data_result['sku']))

