-- part of a query repo
-- query name: Meme Launchpads Metrics
-- query link: https://dune.com/queries/4418048


SELECT 
    DATE_TRUNC('{{date_granularity}}', block_time) AS block_date
    , CASE
        WHEN project = 'gaspump' THEN 'GasPump'
        WHEN referral_address IS NULL THEN 'ton.fun'
        WHEN referral_address = '0:4AD7249B18ED2BCD96EFE6F8E3D0DEDCDF3D17678F8FAAEE2E5C305EF3618564' THEN 'Wagmi'
        WHEN referral_address = '0:3DDBD4759309D89CA5E5D3B5CFF3071C70C2F49CD27EA96D01B5F0094264AE95' THEN 'BigPump'
        WHEN referral_address = '0:71AE4A9BF6C55518156A349CC95BD94370AC2186079A9A404936DD678E0A3FB5' THEN 'Hot'
        WHEN referral_address = '0:C2705CA692BEEFA522895CC0522C3CA88C95D32298E427583E66319C211090EA' THEN 'Blum'
        WHEN referral_address = '0:316265466B4853B41630D23F574A978E866FFDDD8F8B8530A3612E9647A5663E' THEN 'TonTradingBot'
        WHEN referral_address = '0:067E020EC5A115B3F6753567C668BD5D7FBB0EDC3326EB433A68ECAF8ED99128' THEN 'dabtrade.ton'
        WHEN referral_address = '0:13C3E4D18903F945F20F0E815D9910DB8A24BC9F5A2B9AD6F907C860529969A3' THEN 'GraFun'
        ELSE 'other' -- uncomment to label morereferral_address
    END source

    , SUM(volume_usd) AS volume_usd
    , SUM(volume_ton) AS volume_ton
    , COUNT(*) trades
    , COUNT(DISTINCT trader_address) traders
    , COUNT(DISTINCT token_bought_address) tokens_traded
FROM ton.dex_trades 
WHERE 1=1
    AND block_date >= CAST('{{since_date}}' AS TIMESTAMP)
    AND project_type = 'launchpad'
GROUP BY 1, 2
ORDER BY 1, 2, 3 DESC
