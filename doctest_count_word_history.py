import doctest

result_words = """She sells sea shells on the sea shore; The shells that she sells are sea shells I'm sure. So if she sells sea shells on the sea shore, I'm sure that the shells are sea shore shells."""
signs_not_in_words = [";", ".", "!", "?", ","]


def clean_word_list(result_words: str, signs_not_in_words: list) -> list:
    """
    Функция очищает список от ненужных знаков препинания
    """
    result_words_list = result_words.split(" ")
    clean_result_words = []
    for word in result_words_list:
        for sign in signs_not_in_words:
            if sign in word:
                word = word.replace(sign, "")
        clean_result_words.append(word)
    return clean_result_words


def count_word_history(result_words: list) -> list:
    """
    Функция считает сколько раз встретилось слово в истории перебора
    >>> count_word_history(clean_word_list('''She sells sea''', [";", ".", "!", "?", ","]))
    [0, 0, 0]
    >>> count_word_history(clean_word_list('''She sells She shells on the''', [";", ".", "!", "?", ","]))
    [0, 0, 1, 0, 0, 0]
    >>> count_word_history(clean_word_list('''The shells that she sells are''', [";", ".", "!", "?", ","]))
    [0, 0, 0, 0, 0, 0]
    """
    dict_words = {}
    answer_counter_word = []

    for word in result_words:
        if word not in dict_words:
            dict_words[word] = 0
        else:
            dict_words[word] += 1
        answer_counter_word.append(dict_words[word])

    return answer_counter_word

doctest.testmod()