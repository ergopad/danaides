# bootstrap
import pytest

# imports
# can't call this since database is in different repo
# from api import ping

@pytest.mark.asyncio
async def test_true():    
    assert True == True
