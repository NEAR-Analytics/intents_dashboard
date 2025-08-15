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
SELECT * FROM top_assets;


