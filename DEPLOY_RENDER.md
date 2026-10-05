# Render deployment

1. Create/update the Render Web Service from this project.
2. Root directory: `synthtrade-server` if deploying the folder directly, otherwise use the included package.
3. Build command: `npm install`
4. Start command: `npm start`
5. Add environment variables:
   - `DERIV_APP_ID` = the same App ID used by the working SynthTrade bot
   - `DERIV_API_TOKEN` = the same Deriv API token used by the working SynthTrade bot
   - `DERIV_ACCOUNT_TYPE` = `demo`
   - `ASSET` = `R_75`
   - `ENABLE_TRADING` = `false`

The service exposes `/health` on Render's PORT and logs account lookup, OTP generation, authenticated WebSocket open, active-symbol confirmation, historical ticks, and live tick arrival.
