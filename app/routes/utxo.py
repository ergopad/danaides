from fastapi import APIRouter, Depends, HTTPException, status
from utils.logger import logger, myself, Timer, LEIF
from utils.db import eng
from pydantic import BaseModel
from typing import List
from sqlalchemy import text
from decimal import Decimal

# mint
# burn
# get by id

utxo_router = r = APIRouter()

class ErgoTreeHex(BaseModel):
    ergoTree: str

@r.get("/{box_id}")
async def get_utxo_by_id(box_id: str):
    sql = text(f'''
        select id, box_id, ergo_tree, address, nergs, hstore_to_json_loose(registers) as registers, hstore_to_json_loose(assets) as assets, transaction_id, index, creation_height, height
        from utxos 
        where box_id = :box_id
    ''')
    with eng.begin() as con:
        res = con.execute(sql, {'box_id': box_id}).fetchone()

    assets = []
    for k in res["assets"]:
        assets.append({"tokenId": k, "amount": res["assets"][k]})

    return   {
        "boxId": res["box_id"],
        "value": res["nergs"],
        "ergoTree": res["ergo_tree"],
        "creationHeight": res["creation_height"],
        "assets": assets,
        "additionalRegisters": res["registers"],
        "transactionId": res["transaction_id"],
        "index": res["index"]
    }

@r.post("/byErgoTree")
async def get_utxo_by_ergotree(ergoTree: ErgoTreeHex,  offset: int = 0, limit: int = 100):
    sql = text(f'''
        select id, box_id, ergo_tree, address, nergs, hstore_to_json_loose(registers) as registers, hstore_to_json_loose(assets) as assets, transaction_id, index, creation_height, height
        from utxos 
        where ergo_tree = :ergo_tree
        order by id asc
        offset :offset limit :limit
    ''')
    with eng.begin() as con:
        res = con.execute(sql, {'ergo_tree': ergoTree.ergoTree, 'offset': offset, 'limit': limit}).fetchall()

    result = []

    for row in res:
        assets = []
        for k in row["assets"]:
            assets.append({"tokenId": k, "amount": row["assets"][k]})
        result.append({
                "boxId": row["box_id"],
                "value": row["nergs"],
                "ergoTree": row["ergo_tree"],
                "creationHeight": row["creation_height"],
                "assets": assets,
                "additionalRegisters": row["registers"],
                "transactionId": row["transaction_id"],
                "index": row["index"]
            })

    return result