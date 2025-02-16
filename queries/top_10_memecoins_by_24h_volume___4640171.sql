-- part of a query repo
-- query name: Top 10 Memecoins by 24h Volume [d]
-- query link: https://dune.com/queries/4640171


WITH
    memecoins AS (
        -- Coingecko listed memecoins
        SELECT DISTINCT
            memecoin_jetton_master as token_address
        FROM
            dune.barsik_labs.dataset_ton_meme_coingecko
        UNION
        -- Memepad launched tokens
        SELECT DISTINCT
            token_bought_address
        FROM
            ton.dex_trades
        WHERE
            (
                project = 'ton.fun'
                OR project = 'gaspump'
                OR project_type = 'launchpad'
            )
        UNION
        SELECT DISTINCT
            dex_token
        FROM
            query_4632219
    ),
    memecoin_trades AS (
        SELECT
            COALESCE(jm.symbol, 'Unknown') as token_symbol,
            COALESCE(jm.name, m.token_address) as token_name,
            m.token_address,
            SUM(
                CASE
                    WHEN dt.token_bought_address = m.token_address THEN dt.volume_usd
                    WHEN dt.token_sold_address = m.token_address THEN dt.volume_usd
                    ELSE 0
                END
            ) as volume_24h,
            COUNT(DISTINCT dt.tx_hash) as trades_count,
            COUNT(DISTINCT dt.trader_address) as unique_traders,
            SUM(
                CASE
                    WHEN dt.token_bought_address = m.token_address THEN dt.volume_usd
                    ELSE 0
                END
            ) as buy_volume,
            SUM(
                CASE
                    WHEN dt.token_sold_address = m.token_address THEN dt.volume_usd
                    ELSE 0
                END
            ) as sell_volume
        FROM
            memecoins m
            JOIN ton.dex_trades dt ON (
                dt.token_bought_address = m.token_address
                OR dt.token_sold_address = m.token_address
            )
            LEFT JOIN ton.jetton_metadata jm ON m.token_address = jm.address
        WHERE
            dt.block_time >= NOW() - INTERVAL '24' HOUR
            -- Exclude TON address
            AND dt.token_bought_address != '0:0000000000000000000000000000000000000000000000000000000000000000'
            AND dt.token_sold_address != '0:0000000000000000000000000000000000000000000000000000000000000000'
        GROUP BY
            1,
            2,
            3
        HAVING
            COUNT(DISTINCT dt.tx_hash) >= 5 -- Filter out potential outliers
    )
SELECT
    token_symbol,
    token_name,
    volume_24h,
    trades_count,
    unique_traders,
    buy_volume,
    sell_volume,
    buy_volume - sell_volume as volume_delta,
    ROUND(
        (buy_volume - sell_volume) / NULLIF(volume_24h, 0),
        2
    ) as volume_delta_percentage
FROM
    memecoin_trades
ORDER BY
    volume_24h DESC
LIMIT
    10