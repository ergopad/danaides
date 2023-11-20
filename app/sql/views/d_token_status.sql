CREATE MATERIALIZED VIEW public.token_status
AS
 WITH paideia_st AS (
         SELECT utxos.registers -> 'R4'::text AS r4
           FROM utxos
          WHERE utxos.assets ? 'b682ad9e8c56c5a0ba7fe2d3d9b2fbd40af989e8870628f4a03ae1022d36f091'::text
         LIMIT 1
        ), paideia_pl AS (
         SELECT utxos.registers -> 'R4'::text AS r4
           FROM utxos
          WHERE utxos.assets ? '93cda90b4fe24f075d7961fa0d1d662fdc7e1349d313059b9618eecb16c5eade'::text
         LIMIT 1
        ), ergopad_st AS (
         SELECT utxos.registers -> 'R4'::text AS r4
           FROM utxos
          WHERE utxos.assets ? '05cde13424a7972fbcd0b43fccbb5e501b1f75302175178fc86d8f243f3f3125'::text
         LIMIT 1
        ), ergopad_pl AS (
         SELECT utxos.registers -> 'R4'::text AS r4
           FROM utxos
          WHERE utxos.assets ? '0d01f2f0b3254b4a1731e1b61ad77641fe54de2bd68d1c6fc1ae4e7e9ddfb212'::text
         LIMIT 1
        ), egio_st AS (
         SELECT utxos.registers -> 'R4'::text AS r4
           FROM utxos
          WHERE utxos.assets ? 'f419099a27aaa5f6f7d109d8773b1862e8d1857b44aa7d86395940d41eb53806'::text
         LIMIT 1
        ), egio_pl AS (
         SELECT utxos.registers -> 'R4'::text AS r4
           FROM utxos
          WHERE utxos.assets ? '07a8648d0de0f7c87aad41a1fbc6d393a6ad95584d38c47c88125bef101c29e9'::text
         LIMIT 1
        ), egiov2_st AS (
         SELECT utxos.registers -> 'R4'::text AS r4
           FROM utxos
          WHERE utxos.assets ? '097fd281c99588269d672e1b686bf6bcdce04102e183b2242f6634d93869fc0a'::text
         LIMIT 1
        ), egiov2_pl AS (
         SELECT utxos.registers -> 'R4'::text AS r4
           FROM utxos
          WHERE utxos.assets ? 'dc22db903e5ac54da5d4b6f33dd13b2330b79a9ae473f9dd3ea0d796c1179443'::text
         LIMIT 1
        ), neta_st AS (
         SELECT utxos.registers -> 'R4'::text AS r4
           FROM utxos
          WHERE utxos.assets ? '9e5e5e0a3abbaeaf0e54e1e2ca4a6a96c9b7151dd6d7ddaf738be2f99a54dc2b'::text
         LIMIT 1
        ), neta_pl AS (
         SELECT utxos.registers -> 'R4'::text AS r4
           FROM utxos
          WHERE utxos.assets ? '8a4f19b27efeaa328e9ba03f0752253713e6b477cdb9ea7d5e8857e671c48e60'::text
         LIMIT 1
        ), aht_st AS (
         SELECT utxos.registers -> 'R4'::text AS r4
           FROM utxos
          WHERE utxos.assets ? '4517aed531db665693495fd3544faf75717816509cb0987513d24939977abbb6'::text
         LIMIT 1
        ), aht_pl AS (
         SELECT utxos.registers -> 'R4'::text AS r4
           FROM utxos
          WHERE utxos.assets ? '9d90e7567d066263104d1afd13bc23a1a259d733507bf1f33cd02155e7ff0df1'::text
         LIMIT 1
        ), crux_st AS (
         SELECT utxos.registers -> 'R4'::text AS r4
           FROM utxos
          WHERE utxos.assets ? 'd3c7ce0fc494b4d91ae1925aafc24f3c9202ec55d620e030df3fdae3dcbd3ae9'::text
         LIMIT 1
        ), crux_pl AS (
         SELECT utxos.registers -> 'R4'::text AS r4
           FROM utxos
          WHERE utxos.assets ? '22029bcd60b23e4b0a6926cbfd53a1c6100403df0e6d1b3fac810ed1da51aa5e'::text
         LIMIT 1
        )
 SELECT 'paideia'::text AS token_name,
    paideia_st.r4 AS str4,
    paideia_pl.r4 AS plr4,
    4 AS dcml
   FROM paideia_st,
    paideia_pl
UNION
 SELECT 'ergopad'::text AS token_name,
    ergopad_st.r4 AS str4,
    ergopad_pl.r4 AS plr4,
    2 AS dcml
   FROM ergopad_st,
    ergopad_pl
UNION
 SELECT 'egio'::text AS token_name,
    egio_st.r4 AS str4,
    egio_pl.r4 AS plr4,
    4 AS dcml
   FROM egio_st,
    egio_pl
UNION
 SELECT 'egiov2'::text AS token_name,
    egiov2_st.r4 AS str4,
    egiov2_pl.r4 AS plr4,
    4 AS dcml
   FROM egiov2_st,
    egiov2_pl
UNION
 SELECT 'neta'::text AS token_name,
    neta_st.r4 AS str4,
    neta_pl.r4 AS plr4,
    6 AS dcml
   FROM neta_st,
    neta_pl
UNION
 SELECT 'aht'::text AS token_name,
    aht_st.r4 AS str4,
    aht_pl.r4 AS plr4,
    4 AS dcml
   FROM aht_st,
    aht_pl
UNION
 SELECT 'crux'::text AS token_name,
    crux_st.r4 AS str4,
    crux_pl.r4 AS plr4,
    0 AS dcml
   FROM crux_st,
    crux_pl
WITH NO DATA;

CREATE UNIQUE INDEX uq_token_status
    ON public.token_status USING btree
    (token_name);