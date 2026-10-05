# SynthEPEL-R75 v2.2 — Authenticated Tick Research Bot

This build deliberately reuses the **same Deriv REST + account lookup + OTP WebSocket architecture used by the working SynthTrade Pro bot**.

## Research configuration
- Market: R_75
- Tick stream: authenticated WebSocket
- Horizons: 3, 5, 7, 10, 15 ticks
- Signal seed: mean-reversion after a 20-tick displacement of at least 0.05% (configurable)
- EPEL: λ=0.50, minimum 50 prior same-direction outcomes, 95% Wilson lower bound
- Trading: **hard-disabled**
- Account: demo recommended

## Render environment
Set:
- DERIV_APP_ID
- DERIV_API_TOKEN
- DERIV_ACCOUNT_TYPE=demo
- ASSET=R_75

Do not send your API token to anyone. Store it only in Render environment variables.

## Why this connection
The bot first calls `/trading/v1/options/accounts`, selects the configured demo/real account, calls `/trading/v1/options/accounts/{accountId}/otp`, receives Deriv's ready-to-use WebSocket URL, and connects immediately. This is the same architecture used by SynthTrade Pro and is the current Deriv-documented authenticated workflow.

## Output
`data/r75_tick_epel_ledger.csv` is append-only. It records signal and resolution rows for each horizon so the research can be audited later.
