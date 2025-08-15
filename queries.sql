/*
THIS FILE IS FOR USERS ONLY (not AI) 
SQL code can be pasted here and run via Snowflake Connection.

If SQL code is valuable, save is to its own named file separately.

 */

 -- summary statistics
 WITH all_fees AS (
  SELECT 
    symbol as asset,
    blockchain as source_chain,
    SUM(COALESCE(amount_adj, 0)) as total_token_amount,
    SUM(COALESCE(amount_usd, 0)) as total_usd_amount,
    COUNT(*) as total_transactions,
    MIN(block_timestamp) as first_fee,
    MAX(block_timestamp) as last_fee
  FROM NEAR.DEFI.EZ_INTENTS 
  WHERE new_owner_id = 'app-fee.near'
  GROUP BY 1, 2
),
top_assets AS (
  SELECT 
    asset,
    SUM(total_usd_amount) as total_usd,
    SUM(total_token_amount) as total_tokens,
    SUM(total_transactions) as total_txs,
    COUNT(DISTINCT source_chain) as num_chains
  FROM all_fees
  WHERE asset IS NOT NULL
  GROUP BY 1
  ORDER BY total_usd DESC
  LIMIT 20
)
SELECT * FROM top_assets`



-- daily and cumulative fees
WITH daily_fees AS (
  SELECT 
    DATE_TRUNC('day', block_timestamp) as date,
    COALESCE(symbol, contract_address) as asset,
    blockchain as source_chain,
    SUM(COALESCE(amount_adj, 0)) as daily_token_amount,
    SUM(COALESCE(amount_usd, 0)) as daily_usd_amount,
    COUNT(*) as transaction_count
  FROM NEAR.DEFI.EZ_INTENTS 
  WHERE new_owner_id = 'app-fee.near'
    AND block_timestamp >= '2025-01-01'
  GROUP BY 1, 2, 3
),
cumulative_fees AS (
  SELECT 
    date,
    asset,
    source_chain,
    daily_token_amount,
    daily_usd_amount,
    transaction_count,
    SUM(daily_token_amount) OVER (PARTITION BY asset ORDER BY date) as cumulative_token_amount,
    SUM(daily_usd_amount) OVER (PARTITION BY asset ORDER BY date) as cumulative_usd_amount
  FROM daily_fees
)
SELECT 
  date,
  asset,
  source_chain,
  ROUND(daily_token_amount, 6) as daily_token_amount,
  ROUND(daily_usd_amount, 2) as daily_usd_amount,
  ROUND(cumulative_token_amount, 6) as cumulative_token_amount,
  ROUND(cumulative_usd_amount, 2) as cumulative_usd_amount,
  transaction_count
FROM cumulative_fees
WHERE daily_usd_amount > 0 OR cumulative_usd_amount > 0
ORDER BY date DESC, daily_usd_amount DESC
LIMIT 1000