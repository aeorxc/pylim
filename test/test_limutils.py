import pytest

from pylim import limutils


@pytest.mark.parametrize(
    "symbol, result",
    [
        ("FB", False),
        ("AAGXJ00", True),
        ("PGACR00", True),
        ("PA0005643.6.0", True),
    ]
)
def test_pra_symbol(symbol: str, result: bool):
    assert limutils.check_pra_symbol(symbol) == result
