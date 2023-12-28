import pytest
from requests import RequestException
import time
import pandas as pd
import requests


URL_BASE = "api-seller.ozon.ru"

HEADERS_SIMFER = {
    "Client-Id": '8404',
    "Api-Key": 'b43e53ea-43bc-4814-aab3-9fcf3567e51f'
}


def retry(func):
    def wrapper_exception_requests(*args, **kwargs):
        retries = [5, 15, 30, 45, 60, 90, 120]
        for second in retries:
            try:
                return func(*args, **kwargs)
            except RequestException:
                print(f"Failed requests on API OZON on {second} seconds in method")
                time.sleep(second)
        return func(*args, **kwargs)

    return wrapper_exception_requests


@retry
def extract_ozon_chats():

    def get_list_customer_chats() -> pd.DataFrame:
        response = requests.post(f'https://{URL_BASE}/v2/chat/list', headers=HEADERS_SIMFER)
        customer_chat_list = response.json()['chats']
        chat_type_list = []  # тип чата
        chat_created_at_list = []  # когда был создан чат
        chat_unread_count_list = []  # количество непрочитанных сообщений в чате

        for item_chat in customer_chat_list:
            if item_chat['chat_status'] == 'Opened':
                chat_type_list.append(item_chat['chat_type'])
                chat_created_at_list.append(item_chat['created_at'])
                chat_unread_count_list.append(item_chat['unread_count'])

        data = {'type_chat': chat_type_list, 'created_at': chat_created_at_list,
                'count_unread_message': chat_unread_count_list}

        return pd.DataFrame(data)

    result_cc = get_list_customer_chats()

    print(list(result_cc['count_unread_message']))

    return [all(list(result_cc['type_chat'])), all(list(result_cc['created_at'])), all(list(result_cc['count_unread_message']))]

# Если есть пустые значения или 0, то False
# Если нет пустых значений, то True


@pytest.mark.parametrize("exc_result", [([True, True, False])])
def test_extract_ozon_chats(exc_result):
    assert extract_ozon_chats() == exc_result

print(extract_ozon_chats())