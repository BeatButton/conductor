# Job Scheduler

A service that runs generic jobs on a schedule. Useful for automating incoming
and outgoing EDI tasks.

## Each job

Each job is defined in a TOML file. There must be:

- Job name/ID
- Something to run (path to exe/script or inline script)
- Env vars to give that something (optional)
- A [crontab](https://en.wikipedia.org/wiki/Cron#Overview) schedule
- A start date for when to start running the job
- An end date (optional)

## Scheduling and running jobs

- The service is started
- If there is a run-next file/record, load it and run anything that was missed
- Load the job definitions
- Parse them and schedule the next run for each job (persist to run-next file)
- Log to stderr whenever a job is run
