-- part of a query repo
-- query name: Migrated Tokens Leaderboard
-- query link: https://dune.com/queries/4681784


WITH
    trades AS (
        SELECT
            block_time,
            block_date,
            volume_usd AS amount_usd,
            trades.version,
            trader_address AS tx_from,
            pool_address AS project_contract_address,
            amount_bought_raw,
            amount_sold_raw,
            token_sold_address,
            token_bought_address,
            tx_hash,
            case
                when ( -- on Blum
                    project_type = 'launchpad'
                    AND referral_address = '0:C2705CA692BEEFA522895CC0522C3CA88C95D32298E427583E66319C211090EA'
                ) then 'Blum'
                when ( -- Migrated
                    referral_address = '0:37BD8AC8CF61D228F0FDBAE7877F1348701D76B85B2D671BE43E4872603A1BE7'
                ) then 'BlumMigratedToStonfi'
                else 'other'
            end as label
        FROM
            ton.dex_trades AS trades
        WHERE
            -- ( -- on Blum
            --     project_type = 'launchpad'
            --     AND referral_address = '0:C2705CA692BEEFA522895CC0522C3CA88C95D32298E427583E66319C211090EA'
            -- )
            -- OR 
            ( -- Migrated
                referral_address = '0:37BD8AC8CF61D228F0FDBAE7877F1348701D76B85B2D671BE43E4872603A1BE7'
            )
    ),
    liquidity AS (
        SELECT
            *
        FROM
            query_4681803 -- Ston Pool Liquidity
    ),
    pairLeaderboard AS (
        SELECT
            RANK() OVER (
                ORDER BY
                    SUM(amount_usd) DESC
            ) AS pairRank,
            SUM(amount_usd) AS totalVolumeUSD,
            COUNT(DISTINCT (tx_hash)) AS numberOfTrades,
            COUNT(DISTINCT (tx_from)) AS numberOfUsers,
            -- MIN(trades.block_time) AS firstSwapTimestamp,
            -- MAX(trades.block_time) AS latestSwapTimestamp,
            -- version,
            MAX(
                1e0 * amount_usd / (
                    case
                        when token_sold_address = '0:671963027F7F85659AB55B821671688601CDCF1EE674FC7FBBB1A776A18D34A3' then amount_bought_raw
                        else amount_sold_raw
                    end
                ) * 1e18
            ) as max_cap,
            max_by(
                1e0 * amount_usd / (
                    case
                        when token_sold_address = '0:671963027F7F85659AB55B821671688601CDCF1EE674FC7FBBB1A776A18D34A3' then amount_bought_raw
                        else amount_sold_raw
                    end
                ) * 1e18,
                block_time
            ) as cur_cap,
            label,
            project_contract_address
        FROM
            trades
        GROUP BY
            -- version,
            label,
            project_contract_address
    )
SELECT
    pairRank,
    COALESCE(
        token_pair,
        SUBSTRING(CAST(project_contract_address AS VARCHAR), 1, 6)
    ) AS token_pair,
    get_href (
        'https://tonviewer.com/' || case
            when jetton_left_symbol = 'pTON' then jetton_right_address
            Else jetton_left_address
        end,
        case
            when jetton_left_symbol = 'pTON' then jetton_right_symbol
            Else jetton_left_symbol
        end
    ) as token,
    CONCAT(
        '<a href="https://tonviewer.com/',
        CAST(project_contract_address AS VARCHAR),
        '" target=_blank">',
        COALESCE(
            token_pair,
            SUBSTRING(CAST(project_contract_address AS VARCHAR), 1, 6)
        ),
        '</a>'
    ) AS token_pair_url,
    totalVolumeUSD,
    tvl_usd,
    cur_cap,
    max_cap,
    numberOfTrades,
    numberOfUsers,
    get_href (
        'https://tonviewer.com/' || jetton_left_address,
        jetton_left_symbol
    ) as jetton_left_symbol,
    reserves_left,
    jetton_left_address,
    get_href (
        'https://tonviewer.com/' || jetton_right_address,
        jetton_right_symbol
    ) as jetton_right_symbol,
    reserves_right,
    jetton_right_address,
    lp_fee,
    protocol_fee,
    label,
    project_contract_address,
    CONCAT(
        '<a href="https://tonviewer.com/',
        CAST(project_contract_address AS VARCHAR),
        '" target=_blank">',
        CAST(project_contract_address AS VARCHAR),
        '</a>'
    ) AS project_contract_address_url
FROM
    pairLeaderboard
    LEFT JOIN liquidity ON project_contract_address = pool_address
ORDER BY
    totalVolumeUSD DESC