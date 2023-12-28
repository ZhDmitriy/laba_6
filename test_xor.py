import pytest


def xor(a: int, b: int) -> int:
    if a == 0 and b == 1:
        return 1
    elif a == 1 and b == 0:
        return 1
    else:
        return 0


@pytest.mark.parametrize("a, b, exc_result", [(1, 1, 0),
                                              (0, 1, 1),
                                              (1, 0, 1),
                                              (0, 0, 0)])
def test_xor(a, b, exc_result):
    assert xor(a, b) == exc_result