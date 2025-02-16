-- part of a query repo
-- query name: Memecoins part of DeFi
-- query link: https://dune.com/queries/4719735


WITH memecoins AS (
    SELECT DISTINCT raw_address
    FROM dune.ton_foundation.result_ton_meme
),
categorized_trades AS (
    SELECT
        cast(dt.block_time as date) as trade_date,
        dt.volume_usd,
        CASE
            WHEN m1.raw_address IS NOT NULL OR m2.raw_address IS NOT NULL THEN 'Memecoin'
            ELSE 'Non-memecoin'
        END AS category
    FROM ton.dex_trades dt
    LEFT JOIN memecoins m1 ON dt.token_sold_address = m1.raw_address
    LEFT JOIN memecoins m2 ON dt.token_bought_address = m2.raw_address
    WHERE dt.block_time >= CURRENT_DATE - INTERVAL '120' DAY
)
SELECT
    trade_date,
    category,
    SUM(volume_usd) AS volume_usd
FROM categorized_trades
GROUP BY trade_date, category
ORDER BY trade_date, category