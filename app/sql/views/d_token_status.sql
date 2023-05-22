-- find these values in github/ergo-paid/paideia-contracts/contract/staking/__init__.py

create materialized view token_status as
	with 
	-- paideia
	  paideia_st as (select registers->'R4' as r4 from utxos where assets ? 'b682ad9e8c56c5a0ba7fe2d3d9b2fbd40af989e8870628f4a03ae1022d36f091' limit 1)
	, paideia_pl as (select registers->'R4' as r4 from utxos where assets ? '93cda90b4fe24f075d7961fa0d1d662fdc7e1349d313059b9618eecb16c5eade' limit 1)
	-- ergopad
	, ergopad_st as (select registers->'R4' as r4 from utxos where assets ? '05cde13424a7972fbcd0b43fccbb5e501b1f75302175178fc86d8f243f3f3125' limit 1)
	, ergopad_pl as (select registers->'R4' as r4 from utxos where assets ? '0d01f2f0b3254b4a1731e1b61ad77641fe54de2bd68d1c6fc1ae4e7e9ddfb212' limit 1)
	-- egio
	, egio_st as (select registers->'R4' as r4 from utxos where assets ? 'f419099a27aaa5f6f7d109d8773b1862e8d1857b44aa7d86395940d41eb53806' limit 1)
	, egio_pl as (select registers->'R4' as r4 from utxos where assets ? '07a8648d0de0f7c87aad41a1fbc6d393a6ad95584d38c47c88125bef101c29e9' limit 1)
	-- egiov2
	, egiov2_st as (select registers->'R4' as r4 from utxos where assets ? '097fd281c99588269d672e1b686bf6bcdce04102e183b2242f6634d93869fc0a' limit 1)
	, egiov2_pl as (select registers->'R4' as r4 from utxos where assets ? 'dc22db903e5ac54da5d4b6f33dd13b2330b79a9ae473f9dd3ea0d796c1179443' limit 1)
	-- neta
	, neta_st as (select registers->'R4' as r4 from utxos where assets ? '9e5e5e0a3abbaeaf0e54e1e2ca4a6a96c9b7151dd6d7ddaf738be2f99a54dc2b' limit 1)
	, neta_pl as (select registers->'R4' as r4 from utxos where assets ? '8a4f19b27efeaa328e9ba03f0752253713e6b477cdb9ea7d5e8857e671c48e60' limit 1)
	-- aht
	, aht_st as (select registers->'R4' as r4 from utxos where assets ? '4517aed531db665693495fd3544faf75717816509cb0987513d24939977abbb6' limit 1)
	, aht_pl as (select registers->'R4' as r4 from utxos where assets ? '9d90e7567d066263104d1afd13bc23a1a259d733507bf1f33cd02155e7ff0df1' limit 1)
	-- status
		  select 'paideia' as token_name, paideia_st.r4 as str4, paideia_pl.r4 as plr4, 4 as dcml from paideia_st, paideia_pl
	union select 'ergopad' as token_name, ergopad_st.r4 as str4, ergopad_pl.r4 as plr4, 2 from ergopad_st, ergopad_pl
	union select 'egio' as token_name, egio_st.r4 as str4, egio_pl.r4 as plr4, 4 from egio_st, egio_pl
	union select 'egiov2' as token_name, egiov2_st.r4 as str4, egiov2_pl.r4 as plr4, 4 from egiov2_st, egiov2_pl
	union select 'neta' as token_name, neta_st.r4 as str4, neta_pl.r4 as plr4, 6 from neta_st, neta_pl
	union select 'aht' as token_name, aht_st.r4 as str4, aht_pl.r4 as plr4, 4 from aht_st, aht_pl

    with no data;

create unique index uq_token_status on token_status (token_name);
