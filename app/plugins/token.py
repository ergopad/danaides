import asyncio
import pandas as pd
import argparse

from utils.db import eng
from utils.logger import logger
from ergo_python_appkit.appkit import ErgoAppKit, ErgoValue

# region INIT
PRETTYPRINT = False
VERBOSE = False  # args.verbose
NERGS2ERGS = 10**9
BOXES = 'boxes'
TOKENS = 'tokens'
# endregion INIT


# upsert current chunk
async def checkpoint(height, tokens, is_plugin: bool = False, args=None):
    try:
        # handle globals when process called from as plugin
        if is_plugin and (args != None):
            if args.prettyprint:
                PRETTYPRINT = True

        # suffix = f'''TOKENS: Found {len(tokens)} tokens in current block range (starting at {height})...                      '''
        # if PRETTYPRINT: printProgressBar(height, height, prefix='[TOKENS]', suffix=suffix, length=50)
        # else: logger.info(suffix)
        if VERBOSE:
            logger.debug(tokens)
        df = pd.DataFrame.from_dict({
            'token_id': list(tokens.keys()),
            'height': [n['height'] for n in tokens.values()],
            'amount': [n['amount'] for n in tokens.values()],
            'token_name': [n['token_name'] for n in tokens.values()],
            'token_description': [n['token_description'] for n in tokens.values()],
            'decimals': [n['decimals'] for n in tokens.values()],
        })
        if VERBOSE:
            logger.warning(df)
        df.to_sql(f'{TOKENS}', eng, schema='checkpoint', if_exists='replace')
        if VERBOSE:
            logger.debug('saved to checkpoint.tokens')

        # execute as transaction
        with eng.begin() as con:
            # add unspent
            sql = f'''
                insert into {TOKENS} (token_id, height, amount, token_name, token_description, decimals)
                    select c.token_id, c.height, c.amount, c.token_name, c.token_description, c.decimals
                    from checkpoint.{TOKENS} c
                        left join {TOKENS} t on t.token_id = c.token_id
                    -- unique constraint; but want all others
                    where t.token_id is null
                    ;
            '''
            if VERBOSE:
                logger.debug(sql)
            con.execute(sql)

            sql = f'''
                insert into audit_log (height, service, notes)
                values ({int(height)-1}, '{TOKENS}', '{len(tokens)} found')
            '''
            if VERBOSE:
                logger.debug(sql)
            con.execute(sql)

    except Exception as e:
        logger.error(f'ERR: checkpointing {e}')


# extract tokens from the bolckchain
async def process(transactions: dict, tokens: dict, height: int, is_plugin: bool = False, args=None):
    try:
        # handle globals when process called from as plugin
        if is_plugin and (args != None):
            if args.prettyprint:
                PRETTYPRINT = True

        # find all input box ids
        new = tokens
        for tx in transactions:
            input_boxes = [i['boxId'] for i in tx['inputs']]
            for o in tx['outputs']:
                for a in o['assets']:
                    # TODO: refactor this section
                    try:
                        if a['tokenId'] in input_boxes:
                            token_id = a['tokenId']  # this is also the box_id
                            # if already exists, don't redo work
                            if token_id not in new:
                                try:
                                    token_name = ''.join([chr(r) for r in ErgoAppKit.deserializeLongArray(
                                        o['additionalRegisters']['R4'])]
                                    )
                                except:
                                    token_name = ''
                                try:
                                    token_description = ''.join(
                                        [chr(r) for r in ErgoAppKit.deserializeLongArray(
                                            o['additionalRegisters']['R5'])]
                                    )
                                except:
                                    token_description = ''
                                if 'R6' in o['additionalRegisters']:
                                    try:
                                        decimals = ''.join([chr(r) for r in ErgoAppKit.deserializeLongArray(
                                            o['additionalRegisters']['R6'])]
                                        )
                                    except:
                                        decimals = ErgoValue.fromHex(
                                            o['additionalRegisters']['R6']
                                        ).getValue()
                                else:
                                    decimals = 0
                                try:
                                    decimals = int(decimals)
                                except:
                                    decimals = 0
                                try:
                                    amount = int(a['amount'])
                                except:
                                    pass
                                # some funky deserialization issues
                                if type(amount) == int:
                                    if VERBOSE:
                                        logger.debug(
                                            f'''token found: {token_name}/{token_description}/{decimals}/{token_id}/{amount}''')
                                    new[token_id] = {
                                        'height': height,
                                        'token_name': token_name,
                                        'token_description': token_description,
                                        'decimals': decimals,
                                        'amount': amount
                                    }
                    except Exception as e:
                        # BLIPS.append({'asset': a, 'height': height, 'msg': f'invalid asset while looking for tokens'})
                        logger.warning(
                            f'invalid asset, {a} at height {height} while fetching tokens {e}')
        return new

    except Exception as e:
        logger.error(f'ERR: find tokens {e}')
        return {}


def cli():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-J", "--juxtapose", help="Alternative table name", default='boxes'
    )
    parser.add_argument(
        "-H", "--height", help="Begin at this height", type=int, default=-1
    )
    parser.add_argument(
        "-P", "--prettyprint", help="Progress bar vs. scrolling", action='store_true'
    )
    parser.add_argument(
        "-O", "--once", help="When complete, finish", action='store_true'
    )
    parser.add_argument(
        "-V", "--verbose", help="Be wordy", action='store_true'
    )

    args = parser.parse_args()
    if args.juxtapose != 'boxes':
        logger.warning(f'Using alt boxes table: {args.juxtapose}...')
        # only-alpha tablename
        BOXES = ''.join([i for i in args.juxtapose if i.isalpha()])
    if args.height != -1:
        logger.warning(f'Starting at height: {args.height}...')
    if args.prettyprint:
        logger.warning(f'Pretty print...')
    if args.once:
        logger.warning(f'Processing once, then exit...')
    if args.verbose:
        logger.warning(f'Verbose...')

    PRETTYPRINT = args.prettyprint
    VERBOSE = args.verbose
    FETCH_INTERVAL = args.fetchinterval

    return args


# region MAIN
async def main(args):
    res = await process()
    if res['tokens'] == None:
        logger.error(f'''ERR: processing tokens {res['message']}''')
    else:
        logger.debug(res['tokens'])


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("-J", "--juxtapose",
                        help="Alternative table name", default='staking')
    parser.add_argument("-P", "--prettyprint",
                        help="Begin at this height", action='store_true')
    args = parser.parse_args()

    PRETTYPRINT = args.prettyprint
    JUXTAPOSE = args.juxtapose

    res = asyncio.run(main(args))
# endregion MAIN
