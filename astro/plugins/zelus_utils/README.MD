# Zelus Technical:
## Setup:
* Utilizing Python and Postgres SQL. Originally tried having everything running in Docker and local but could never get Airflow/Astro to see internal Postgres DB so switched to a DB instance on another machine that I use for a homelab
	
### Note: Normally I would setup Slack or some type of alerting for if a task fails but in this example I did put my general code in but did not hook it all up

## Questions:
0) 1 - No knowledge of Cricket
1) Need to download Astro CLI and then within the astro directory run `Astr dev start`. From there need to update the Postgres info for server with the DAG and wait a few minutes for changes to be picked up. Enable the DAG and then hit play. This will get "ALL Match Data". During full run it took nearly 24 hours. Would love to add parallel processing but ran out of time. 
   - Note: The extra docker-compose does not need to be spun up unless wanting ease of looking at logs

2) All Queris can be found in the `astro/plugins/zelus_utils/sql/uestion2.sql` file.
   - Note: Was unable to figure out the highest strike rate but also worked on other possible useful queries and insights within `more_queries.sql`

3) I would add logic to `get_successful_rows` to check to see if the table exists in the first place. If that little bit of logic was added then as long as the `match_info` table existed the pipeline would skip all files already processed.