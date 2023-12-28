import pytest
import math


def quadratic_equation(a: int, b: int, c: int) -> (int, int):
    if a == 0:
        return "Failed: a = 0"
    discr = b ** 2 - 4 * a * c
    if discr > 0:
        x1 = (-b + math.sqrt(discr)) / (2 * a)
        x2 = (-b - math.sqrt(discr)) / (2 * a)
        return x1, x2
    elif discr == 0:
        x = -b / (2 * a)
        return x
    else:
        return "Корней нет"


@pytest.mark.parametrize("a, b, c, exc_result", [(4, 4, 1, -0.5),
                                                 (2, 1, 1, "Корней нет"),
                                                 (-1, 2, 8, (-2, 4))])
def test_quadratic_equation(a, b, c, exc_result):
    assert quadratic_equation(a, b, c) == exc_result
