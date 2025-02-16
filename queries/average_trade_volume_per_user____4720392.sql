-- part of a query repo
-- query name: average trade volume per user for memecoins and non-memecoins
-- query link: https://dune.com/queries/4720392


WITH
    memecoins AS (
        SELECT DISTINCT
            raw_address
        FROM
            dune.ton_foundation.result_ton_meme
        WHERE
            raw_address NOT IN (
                '0:2F956143C461769579BAEF2E32CC2D7BC18283F40D20BB03E432CD603AC33FFC',
                '0:AFC49CB8786F21C87045B19EDE78FC6B46C51048513F8E9A6D44060199C1BF0C'
            ) -- Exclude noisy memecoins
    ),
    excluded_non_memecoins AS (
        SELECT
            '0:09F2E59DEC406AB26A5259A45D7FF23EF11F3E5C7C21DE0B0D2A1CBE52B76B3D' AS address -- Humster Kombat
        UNION ALL
        SELECT
            '0:78CD9BAC1EC6D4DAF5533EA8E19689083A8899844742313EF4DC2584CE14CEA3' AS address -- Empire X
    ),
    categorized_trades AS (
        SELECT
            DATE_TRUNC('{{date_granularity}}', dt.block_time) AS block_date,
            dt.volume_usd,
            dt.trader_address,
            CASE
                WHEN m1.raw_address IS NOT NULL
                OR m2.raw_address IS NOT NULL THEN 'Memecoin'
                ELSE 'Non-memecoin'
            END AS category
        FROM
            ton.dex_trades dt
            LEFT JOIN memecoins m1 ON dt.token_sold_address = m1.raw_address
            LEFT JOIN memecoins m2 ON dt.token_bought_address = m2.raw_address
            LEFT JOIN excluded_non_memecoins ex1 ON dt.token_sold_address = ex1.address
            LEFT JOIN excluded_non_memecoins ex2 ON dt.token_bought_address = ex2.address
        WHERE
            dt.block_time >= timestamp '{{since_date}}'
            AND ex1.address IS NULL -- Exclude trades involving excluded tokens
            AND ex2.address IS NULL
    ),
    avg_volume_by_month AS (
        SELECT
            block_date,
            category,
            SUM(volume_usd) AS total_volume_usd,
            COUNT(DISTINCT trader_address) AS total_traders,
            CASE
                WHEN COUNT(DISTINCT trader_address) > 0 THEN SUM(volume_usd) / COUNT(DISTINCT trader_address)
                ELSE 0
            END AS avg_volume_per_user_usd
        FROM
            categorized_trades
        GROUP BY
            1,
            2
    )
SELECT
    block_date,
    category,
    avg_volume_per_user_usd,
    AVG(avg_volume_per_user_usd) OVER (
        PARTITION BY
            category
        ORDER BY
            block_date
    ) AS cumulative_avg_volume
FROM
    avg_volume_by_month
ORDER BY
    1,
    2;