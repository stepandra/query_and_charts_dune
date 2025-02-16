-- part of a query repo
-- query name: Memecoins Monthly Trading Volume Overview [d]
-- query link: https://dune.com/queries/4640538


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
    dex_volumes AS (
        SELECT
            DATE_TRUNC('month', block_time) as month,
            SUM(volume_usd) as volume_usd,
            'DEX' as source
        FROM
            ton.dex_trades dt
        WHERE
            (
                token_bought_address IN (
                    SELECT
                        token_address
                    FROM
                        memecoins
                )
                OR token_sold_address IN (
                    SELECT
                        token_address
                    FROM
                        memecoins
                )
            )
            AND block_time >= timestamp '2024-07-01'
            AND token_bought_address != '0:0000000000000000000000000000000000000000000000000000000000000000'
            AND token_sold_address != '0:0000000000000000000000000000000000000000000000000000000000000000'
        GROUP BY
            1
    ),
    memepad_volumes AS (
        SELECT
            DATE_TRUNC('month', block_time) as month,
            SUM(volume_usd) as volume_usd,
            'Memepad' as source
        FROM
            ton.dex_trades
        WHERE
            (
                project = 'ton.fun'
                OR project = 'gaspump'
                OR project_type = 'launchpad'
            )
            AND block_time >= timestamp '2024-07-01'
        GROUP BY
            1
    ),
    combined_volumes AS (
        SELECT
            *
        FROM
            dex_volumes
        UNION ALL
        SELECT
            *
        FROM
            memepad_volumes
    ),
    monthly_totals AS (
        SELECT
            month,
            source,
            volume_usd,
            SUM(volume_usd) OVER (
                ORDER BY
                    month
            ) as cumulative_volume
        FROM
            combined_volumes
    )
SELECT
    month,
    source,
    volume_usd,
    SUM(volume_usd) OVER (
        ORDER BY
            month
    ) as total_cumulative_volume
FROM
    monthly_totals
ORDER BY
    month,
    source