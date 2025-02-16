-- part of a query repo
-- query name: memecoins_mcap_memepads_100k_1mil_10mil_100mil [d]
-- query link: https://dune.com/queries/4640977


WITH memecoins AS (
    SELECT DISTINCT raw_address as token_address
    FROM dune.ton_foundation.result_ton_meme
),
token_metadata AS (
    SELECT address, decimals
    FROM dune.ton_foundation.result_ton_jettons_metadata_latest_values
),
token_first_trade AS (
    SELECT
        CASE
            WHEN token_sold_address IN (SELECT token_address FROM memecoins) THEN token_sold_address
            ELSE token_bought_address
        END as token,
        MIN(block_time) as first_trade_time
    FROM ton.dex_trades dt
    WHERE (token_bought_address IN (SELECT token_address FROM memecoins)
       OR token_sold_address IN (SELECT token_address FROM memecoins))
      AND block_time >= timestamp '2024-09-01'  --  start date
    GROUP BY 1
),
daily_trades_and_tvl AS (
    SELECT
        DATE_TRUNC('day', dt.block_time) as day,
        CASE
            WHEN token_sold_address IN (SELECT token_address FROM memecoins) THEN token_sold_address
            ELSE token_bought_address
        END as token,
        SUM(CASE
            WHEN token_sold_address IN (SELECT token_address FROM memecoins) THEN amount_sold_raw
            ELSE amount_bought_raw
        END) as token_amount,
        SUM(volume_usd) as volume_usd,
        COUNT(DISTINCT tx_hash) as trades_count,
        MAX(dp.tvl_usd) as pool_tvl  -- Use MAX for daily TVL
    FROM ton.dex_trades dt
    LEFT JOIN ton.dex_pools dp ON dt.pool_address = dp.pool
        AND DATE_TRUNC('day', dt.block_time) = DATE_TRUNC('day', dp.block_time)
    WHERE (token_bought_address IN (SELECT token_address FROM memecoins)
       OR token_sold_address IN (SELECT token_address FROM memecoins))
       AND dt.block_time >= timestamp '2024-09-01'   --  start date
    GROUP BY 1, 2
),
daily_marketcap AS (
    SELECT
        d.day,
        d.token,
        d.volume_usd,
        d.trades_count,
        d.pool_tvl,
        COALESCE(tm.decimals, 9) as decimals, -- Default to 9 if decimals are missing.
        CASE
            WHEN d.token_amount = 0 OR d.pool_tvl IS NULL THEN 0  -- Handle cases with no trades or missing TVL
            ELSE (d.volume_usd / (d.token_amount / power(10, COALESCE(tm.decimals, 9)))) * 1e9 -- Use token decimals
        END as market_cap_usd_raw,
          CASE
            WHEN d.token_amount = 0 OR d.pool_tvl IS NULL THEN 0  -- Handle cases with no trades or missing TVL
            ELSE (d.volume_usd / (d.token_amount / power(10, COALESCE(tm.decimals, 9)))) * 1e9 -- Use token decimals
        END as market_cap_usd,
        t.first_trade_time
    FROM daily_trades_and_tvl d
    JOIN token_first_trade t ON d.token = t.token
    LEFT JOIN token_metadata tm ON d.token = tm.address -- join for decimals
),
peak_marketcap_per_token AS (
    SELECT
        d.token,  -- Use d.token to disambiguate
        MAX(d.market_cap_usd) as peak_marketcap
    FROM daily_marketcap d  -- Use table alias here
    GROUP BY 1
)
SELECT
    DATE_TRUNC('month', first_trade_time) AS launch_month,
     COUNT(DISTINCT CASE WHEN peak_marketcap >= 100000000 THEN p.token END) AS tokens_100m_plus,  -- Use alias p here
     COUNT(DISTINCT CASE WHEN peak_marketcap >= 10000000 THEN p.token END) AS tokens_10m_plus,
     COUNT(DISTINCT CASE WHEN peak_marketcap >= 1000000 THEN p.token END) AS tokens_1m_plus,
     COUNT(DISTINCT CASE WHEN peak_marketcap >= 100000 THEN p.token END) AS tokens_100k_plus
FROM peak_marketcap_per_token p  -- Use table alias here
JOIN token_first_trade t ON p.token = t.token
GROUP BY 1
ORDER BY launch_month;