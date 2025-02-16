-- part of a query repo
-- query name: Graduated Tokens (24h pump.fun)
-- query link: https://dune.com/queries/4727726


WITH withdraws AS (
    SELECT DISTINCT
        COALESCE(account_arguments[3], account_arguments[2]) AS token_address
    FROM
        solana.instruction_calls
    WHERE
        executing_account = '6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P'
        AND bytearray_substring(data, 1, 8) = 0xb712469c946da122
        AND tx_success = true
        AND block_time >= NOW() - INTERVAL '24' HOUR
),
token_prices AS (
    SELECT 
        CASE
            WHEN token_bought_mint_address = w.token_address THEN token_bought_mint_address
            WHEN token_sold_mint_address = w.token_address THEN token_sold_mint_address
            ELSE NULL
        END AS token_address,
        CASE
            WHEN token_bought_mint_address = w.token_address THEN token_bought_symbol
            WHEN token_sold_mint_address = w.token_address THEN token_sold_symbol
            ELSE 'Unknown'
        END AS asset,
        amount_usd / NULLIF(
            CASE 
                WHEN token_bought_mint_address = w.token_address THEN token_bought_amount
                WHEN token_sold_mint_address = w.token_address THEN token_sold_amount
                ELSE 0
            END, 0
        ) AS token_price,
        CASE 
            WHEN token_bought_mint_address = w.token_address THEN token_bought_amount
            WHEN token_sold_mint_address = w.token_address THEN token_sold_amount
            ELSE 0
        END AS token_amount,
        block_time,
        ROW_NUMBER() OVER (PARTITION BY COALESCE(token_bought_mint_address, token_sold_mint_address) ORDER BY block_time DESC) AS rn
    FROM dex_solana.trades t
    JOIN withdraws w
    ON t.token_bought_mint_address = w.token_address
    OR t.token_sold_mint_address = w.token_address
    WHERE amount_usd >= 1
    AND block_time <= NOW() - INTERVAL '1' HOUR
),
ranked_prices AS (
    SELECT
        token_address, 
        asset,
        token_price,
        token_amount
    FROM 
        token_prices
    WHERE 
        rn <= 7
    AND asset != 'Unknown'
),
trade_counts AS (
    SELECT
        COALESCE(token_bought_mint_address, token_sold_mint_address) AS token_address,
        COUNT(*) AS trade_count
    FROM dex_solana.trades
    WHERE 
        (token_bought_mint_address IN (SELECT token_address FROM withdraws)
         OR token_sold_mint_address IN (SELECT token_address FROM withdraws))
        AND amount_usd >= 1
    AND block_time >= NOW() - INTERVAL '24' HOUR
    GROUP BY COALESCE(token_bought_mint_address, token_sold_mint_address)
)

SELECT
    r.token_address,
    r.asset,
    SUM(r.token_price * r.token_amount) / SUM(r.token_amount) AS vwap_token_price,
    SUM(r.token_price * r.token_amount) / SUM(r.token_amount) * 1000000000 AS market_cap,
    CONCAT(
        '<a href="https://dexscreener.com/solana/',
        r.token_address,
        '" target=_blank">Chart</a>'
    ) AS chart,
    tc.trade_count
FROM
    ranked_prices r
JOIN
    trade_counts tc
ON
    r.token_address = tc.token_address
WHERE
    tc.trade_count >= 100
GROUP BY
    r.token_address, r.asset, tc.trade_count
ORDER BY
    market_cap DESC;
