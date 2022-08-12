create unique index if not exists uq_utxos_boxid on utxos (box_id);
create unique index if not exists uq_assets_address_tokenid on assets (address, token_id);
create unique index if not exists uq_balances_address on balances (address);
create unique index if not exists uq_tokens on tokens (token_id) include (decimals, amount, token_name, token_price);
