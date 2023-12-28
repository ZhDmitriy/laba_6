import doctest


def count_unique_number(numbers: list) -> int:
    """
    Функция считает количество уникальных элементов в списке
    >>> count_unique_number([1, 1, 2, 2])
    2
    >>> count_unique_number([1, 1, 2, 2, 3, 4])
    4
    >>> count_unique_number([1, 1, 2, 2, 3, 4, 5, 5])
    5
    """
    return len(list(set(numbers)))


doctest.testmod()