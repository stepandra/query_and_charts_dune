-- part of a query repo
-- query name: arbitrage_opportunities_ton_memecoins
-- query link: https://dune.com/queries/4641990


WITH 
    memecoins AS (
        SELECT DISTINCT 
            raw_address as token_address
        FROM 
            dune.ton_foundation.result_ton_meme
    ),
    latest_metadata AS (
        SELECT 
            address,
            symbol,
            decimals
        FROM 
            dune.ton_foundation.result_ton_jettons_metadata_latest_values
    ),
    parameters AS (
        SELECT
            COALESCE(CAST({{minimum_tvl_usd}} AS DECIMAL), 5000) as min_tvl_usd,
            COALESCE(CAST({{minimum_spread_percentage}} AS DECIMAL), 2.5) / 100 as min_spread
    ),
    ton_addresses AS (
        SELECT * FROM (VALUES
            ('0:0000000000000000000000000000000000000000000000000000000000000000', 'TON', 9), -- Native TON
            ('0:671963027F7F85659AB55B821671688601CDCF1EE674FC7FBBB1A776A18D34A3', 'TON', 9)  -- pTON
        ) as t(address, symbol, decimals)
    ),
    filtered_pools AS (
        SELECT
            block_time,
            pool,
            project,
            jetton_left,
            jetton_right,
            -- Handle both native TON and pTON (always 9 decimals)
            CASE 
                WHEN jetton_left IN (SELECT address FROM ton_addresses) THEN CAST(reserves_left AS DECIMAL(38,0)) / POWER(10, 9)
                ELSE CAST(reserves_left AS DECIMAL(38,0)) / POWER(10, COALESCE(l.decimals, 9))
            END as reserves_left_adjusted,
            CASE 
                WHEN jetton_right IN (SELECT address FROM ton_addresses) THEN CAST(reserves_right AS DECIMAL(38,0)) / POWER(10, 9)
                ELSE CAST(reserves_right AS DECIMAL(38,0)) / POWER(10, COALESCE(r.decimals, 9))
            END as reserves_right_adjusted,
            tvl_usd,
            is_liquid,
            -- Improved token symbol handling
            COALESCE(
                (SELECT symbol FROM ton_addresses WHERE address = jetton_left),
                l.symbol,
                SUBSTRING(jetton_left, 1, 6)
            ) as left_symbol,
            COALESCE(
                (SELECT symbol FROM ton_addresses WHERE address = jetton_right),
                r.symbol,
                SUBSTRING(jetton_right, 1, 6)
            ) as right_symbol
        FROM 
            ton.dex_pools p
            LEFT JOIN latest_metadata l ON p.jetton_left = l.address
            LEFT JOIN latest_metadata r ON p.jetton_right = r.address
            CROSS JOIN parameters
        WHERE 
            block_date >= CURRENT_DATE - INTERVAL '7' DAY
            AND tvl_usd >= (SELECT min_tvl_usd FROM parameters)
            AND is_liquid = true
            AND reserves_left > 0 
            AND reserves_right > 0
    ),
    arbitrage_opportunities AS (
        SELECT
            a.block_time,
            a.pool as pool_a,
            b.pool as pool_b,
            a.project as project_a,
            b.project as project_b,
            a.jetton_left as token_a_left,
            a.jetton_right as token_a_right,
            b.jetton_left as token_b_left,
            b.jetton_right as token_b_right,
            a.left_symbol as token_a_left_symbol,
            a.right_symbol as token_a_right_symbol,
            b.left_symbol as token_b_left_symbol,
            b.right_symbol as token_b_right_symbol,
            -- Properly handle reciprocal rates for consistency
            CASE 
                WHEN a.reserves_left_adjusted > 0 THEN 
                    CASE 
                        WHEN a.reserves_right_adjusted / a.reserves_left_adjusted > 1 
                        THEN a.reserves_right_adjusted / a.reserves_left_adjusted
                        ELSE a.reserves_left_adjusted / a.reserves_right_adjusted
                    END
                ELSE 0 
            END as rate_a,
            CASE 
                WHEN b.reserves_left_adjusted > 0 THEN 
                    CASE 
                        WHEN b.reserves_right_adjusted / b.reserves_left_adjusted > 1
                        THEN b.reserves_right_adjusted / b.reserves_left_adjusted
                        ELSE b.reserves_left_adjusted / b.reserves_right_adjusted
                    END
                ELSE 0 
            END as rate_b,
            -- Improved spread calculation to avoid extreme values
            CASE 
                WHEN a.reserves_left_adjusted > 0 AND b.reserves_left_adjusted > 0 THEN
                    LEAST(
                        ABS(1 - (
                            CASE 
                                WHEN a.reserves_right_adjusted / a.reserves_left_adjusted > 1 
                                THEN a.reserves_right_adjusted / a.reserves_left_adjusted
                                ELSE a.reserves_left_adjusted / a.reserves_right_adjusted
                            END / 
                            CASE 
                                WHEN b.reserves_right_adjusted / b.reserves_left_adjusted > 1
                                THEN b.reserves_right_adjusted / b.reserves_left_adjusted
                                ELSE b.reserves_left_adjusted / b.reserves_right_adjusted
                            END
                        )),
                        0.5  -- Cap at 50% spread
                    )
                ELSE 0
            END as spread,
            a.tvl_usd as pool_a_tvl,
            b.tvl_usd as pool_b_tvl
        FROM 
            filtered_pools a
            JOIN filtered_pools b ON (
                (
                    (
                        a.jetton_left = b.jetton_right 
                        OR (
                            a.jetton_left IN (SELECT address FROM ton_addresses)
                            AND b.jetton_right IN (SELECT address FROM ton_addresses)
                        )
                    )
                    AND 
                    (
                        a.jetton_right = b.jetton_left
                        OR (
                            a.jetton_right IN (SELECT address FROM ton_addresses)
                            AND b.jetton_left IN (SELECT address FROM ton_addresses)
                        )
                    )
                )
                AND a.block_time = b.block_time
                AND a.pool != b.pool
            )
        WHERE 
            (a.jetton_left IN (SELECT token_address FROM memecoins)
            OR a.jetton_right IN (SELECT token_address FROM memecoins))
    )
SELECT 
    block_time,
    pool_a,
    pool_b,
    project_a,
    project_b,
    CONCAT(token_a_left_symbol, '/', token_a_right_symbol) as pool_a_pair,
    CONCAT(token_b_left_symbol, '/', token_b_right_symbol) as pool_b_pair,
    ROUND(rate_a, 6) as rate_a,
    ROUND(rate_b, 6) as rate_b,
    ROUND(spread * 100, 2) as spread_percentage,
    ROUND(pool_a_tvl, 2) as pool_a_tvl_usd,
    ROUND(pool_b_tvl, 2) as pool_b_tvl_usd,
    get_href(CONCAT('https://tonviewer.com/', pool_a), CONCAT(project_a, ' Pool')) as pool_a_link,
    get_href(CONCAT('https://tonviewer.com/', pool_b), CONCAT(project_b, ' Pool')) as pool_b_link
FROM 
    arbitrage_opportunities
    CROSS JOIN parameters
WHERE 
    spread >= (SELECT min_spread FROM parameters)
    AND spread < 0.5  -- Cap at 50% spread
    AND pool_a_tvl >= (SELECT min_tvl_usd FROM parameters)
    AND pool_b_tvl >= (SELECT min_tvl_usd FROM parameters)
    AND rate_a > 0
    AND rate_b > 0
ORDER BY 
    spread_percentage DESC,
    (pool_a_tvl + pool_b_tvl) DESC
LIMIT 1000;