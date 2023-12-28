import pytest


def voting(x: int, y: int, z: int) -> int:
    if (x + y + z) >= 2:
        return 1
    else:
        return 0


@pytest.mark.parametrize("a, b, c, exc_result", [(1, 1, 0, 1),
                                                 (1, 0, 0, 0),
                                                 (0, 0, 0, 0),
                                                 (1, 1, 1, 1)])
def test_voting(a, b, c, exc_result):
    assert voting(a, b, c) == exc_result