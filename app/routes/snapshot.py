from fastapi import APIRouter, Depends, HTTPException, status
from utils.logger import logger, myself, Timer, LEIF
from utils.db import eng
from pydantic import BaseModel

# get by token id

snapshot_router = r = APIRouter()

class Token(BaseModel):
    id: str
    name: str = ''
    decimals: int = 0
    amount: int = 0

@r.post("/byTokenId/")
async def burn(token: Token):
    return {}

