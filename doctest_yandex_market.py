import pandas as pd
import requests
import doctest


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


doctest.testmod()