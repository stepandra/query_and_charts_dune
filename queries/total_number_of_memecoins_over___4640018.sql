-- part of a query repo
-- query name: total number of memecoins over time d
-- query link: https://dune.com/queries/4640018


WITH
    monthly_memecoin_counts AS (
        -- Count from Coingecko dataset
        SELECT
            DATE_TRUNC('month', block_time) as month,
            COUNT(DISTINCT memecoin_jetton_master) as coingecko_coins
        FROM
            ton.dex_trades dt
            JOIN dune.barsik_labs.dataset_ton_meme_coingecko cg ON dt.token_bought_address = cg.memecoin_jetton_master
        WHERE
            block_time >= timestamp '2024-05-01'
        GROUP BY
            1
    ),
    memepad_counts AS (
        -- Count from memepads
        SELECT
            DATE_TRUNC('month', first_trade_time) as month,
            COUNT(DISTINCT token_bought_address) as memepad_coins
        FROM
            (
                SELECT
                    token_bought_address,
                    MIN(block_time) as first_trade_time
                FROM
                    ton.dex_trades
                WHERE
                    (
                        project = 'ton.fun'
                        OR project = 'gaspump'
                        OR project_type = 'launchpad'
                    )
                    AND block_time >= timestamp '2024-05-01'
                GROUP BY
                    1
            ) first_appearances
        GROUP BY
            1
    ),
    monthly_totals AS (
        SELECT
            COALESCE(mc.month, mp.month) as month,
            COALESCE(coingecko_coins, 0) as coingecko_coins,
            COALESCE(memepad_coins, 0) as memepad_coins,
            COALESCE(coingecko_coins, 0) + COALESCE(memepad_coins, 0) as monthly_new_coins
        FROM
            monthly_memecoin_counts mc
            FULL OUTER JOIN memepad_counts mp ON mc.month = mp.month
    )
SELECT
    month,
    coingecko_coins,
    memepad_coins,
    monthly_new_coins,
    SUM(monthly_new_coins) OVER (
        ORDER BY
            month
    ) as cumulative_total_coins
FROM
    monthly_totals
ORDER BY
    month