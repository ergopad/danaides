# bootstrap
import pytest

# imports
from main import ping


def test_always_passes():
    return True


@pytest.mark.asyncio
async def test_ping():
    res = await ping()
    assert res["hello"] == "world"
