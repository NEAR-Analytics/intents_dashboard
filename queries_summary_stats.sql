-- Summary statistics for KPI cards
WITH all_fees AS (
  SELECT 
    symbol as asset,
    blockchain as source_chain,
    COALESCE(amount_usd, 0) as usd_amount,
    block_timestamp,
    DATE_TRUNC('day', block_timestamp) as date
  FROM NEAR.DEFI.EZ_INTENTS 
  WHERE new_owner_id = 'app-fee.near'
),
summary_stats AS (
  SELECT 
    -- Total metrics
    SUM(usd_amount) as total_usd,
    COUNT(*) as total_transactions,
    COUNT(DISTINCT asset) as unique_assets,
    COUNT(DISTINCT source_chain) as unique_chains,
    
    -- Today's metrics (based on most recent date in data)
    SUM(CASE WHEN date = (SELECT MAX(date) FROM all_fees) THEN usd_amount ELSE 0 END) as today_usd,
    COUNT(CASE WHEN date = (SELECT MAX(date) FROM all_fees) THEN 1 ELSE NULL END) as today_transactions,
    
    -- Date range
    MIN(block_timestamp) as first_transaction,
    MAX(block_timestamp) as last_transaction,
    MAX(date) as latest_date
  FROM all_fees
),
top_asset AS (
  SELECT 
    asset,
    SUM(usd_amount) as asset_total_usd
  FROM all_fees
  WHERE asset IS NOT NULL
  GROUP BY asset
  ORDER BY asset_total_usd DESC
  LIMIT 1
)
SELECT 
  s.total_usd,
  s.total_transactions,
  s.unique_assets,
  s.unique_chains,
  s.today_usd,
  s.today_transactions,
  s.first_transaction,
  s.last_transaction,
  s.latest_date,
  t.asset as top_asset,
  t.asset_total_usd as top_asset_usd,
  DATEDIFF('day', s.first_transaction, s.last_transaction) + 1 as days_active,
  s.total_usd / NULLIF(DATEDIFF('day', s.first_transaction, s.last_transaction) + 1, 0) as avg_daily_usd
FROM summary_stats s
CROSS JOIN top_asset t;