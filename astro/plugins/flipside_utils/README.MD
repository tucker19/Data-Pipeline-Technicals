# Flipside Technical:
## Setup:
* Utilizing Python and Postgres SQL. Originally tried having everything running in Docker and local but could never get Airflow/Astro to see internal Postgres DB so switched to a DB instance on another machine that I use for a homelab
* Will need to update `airflow_settings.yaml` in the main directory and update `technicals_postgres` entry. Other option is to spin up Astro and via the GUI add the connection.
	
### Note: Normally I would setup Slack or some type of alerting for if a task fails but in this example I did put my general code in but did not hook it all up

## Current Queries to be ran:
1) Top 10 Senders by amount in last hour
2) Top 10 Receiver by amount in last hour
3) Top 10 Senders for a block_id by transfers
4) Top 10 Receivers for a block_id by transfers
5) Number of Transactions per Day over last week
