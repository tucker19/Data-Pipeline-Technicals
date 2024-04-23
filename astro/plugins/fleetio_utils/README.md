# Fleetio Technical:
## Setup:
	- Utilizing Python and Postgres SQL. Originally tried having everything running in Docker but could never get Airflow/Astro to see internal Postgres DB so switched to a DB instance on another machine that I use for a homelab
	- Could never get my personal account working with connecting via API so utilized a Service Account, granted using a Service Account is the proper way of doing this as then pipelines are not tied to a single individual but an organization
	- Did also play with just publishing the Google Sheet as shown in the "published_csv" which then just need a link but found via that way I would need a whole different way of doing my process as for the decimal value columns it actually came as a decimal and not a String
	- I also could not for the life of me get my code to insert the JSON as a single JSON object so to not lose that data created another table using the "vehicle_id" as the key and then key value columns to capture that data

### Note: Normally I would setup Slack or some type of alerting for if a task fails but in this example I did put my general code in but did not hook it all up

## Questions:
1) How would you check for updates in the sheet?
	- Never saw a good way via api with Service Account and think it would be better to setup polling in some interval to check for updated/new rows to then update/insert into issues table
	- Another solution could be is to ingest this data into a data lake with a auto-gen "inserted_at" that in another step polled the data lake which if found new or updated data hanlde that with the data warehouse table. For my current ingestion I do more my cleaning within the python code

2) How would you orchestrate this script in a production setting?
	- My personal choice is Apache Airflow as shown in this code base

3) Given more time, what else would you have liked to do?
	- I would have liked to set this all up in a data lake -> data warehouse style format. Then over time if something does go wrong we could always replay based off the "inserted_at" date to rebuild the data warehouse

	- I would not rely on the "updated_at" as much, in theory once a issue is resolved or closed that row should never be touched again but that may not be the reality 

4) If you were assigned this problem in real life, what other approaches would you have taken?
	- I would be asking a lot of questions on goals of the data. For this task when state to "clean" the data I really did not know what was meant by that. I did notice "vehicle_meter_value" could be negative so I did an absolute call to always have positive amounts. A few questions I would have:
		* Is there a common Unit do we want to use ie - mi or km?
		* How would we want to handle the case of "hr"?
		* Are mi and km the only ones we should accept, during ingestion say we just get m to then convert to km?
		* Currently defaulting to NULL for when data is not provided, is there a different default we want?
		* Is the "custom_fields" data important? Are there known values that can go in there or is it completely random?
		* Do we only want to accept English Descriptions or maybe translate to English?

	- As this was local and still not well versed in dbt I would probably want to explore setting Airflow pipelines to auto gen for visualization using dbt via the Cosmos library

	- I would also maybe stress more robust descriptions or columns for like "Cosmetic" or "Mechanical" so then could model based off that for downstream could determine higher priority issues. Example, row 22 issue is a rock chip which could be lower priority to row 8 of "brakes need to be replaced" and these two things would fall in different cosmetic vs mechanical.

	- I would also like to add maybe "vehicle_type" as a column with possible options "truck", "car", "bike"

	- If I was truly doing this in a production level env I would strongly push for a web application for the data to be used for input data and not need to rely on Google Sheets. Then data could be collected via an API endpoint into a data lake and an hourly or daily job could run from data lake to data warehouse with new or updated data