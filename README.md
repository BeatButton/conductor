

# Conductor

A service that runs generic jobs on a schedule. Useful for automating incoming and outgoing EDI tasks.

## Job specification

Each job is defined in a TOML file.

### Required section: `[job]`
#### Required fields 
- `name`: A human-readable identifier

- `command`: Something to run (path to exe/script or inline script)

- `crontab`: A [crontab](https://en.wikipedia.org/wiki/Cron#Overview) schedule

#### Optional fields 
- `directory`: The working directory in which to start the job. The default is the jobs directory.

- `start_date`: The date at which to start running this job. The default is immediately.

- `stop_date`: The date at which to stop running this job. The default is never.
  
### Optional section: `[environment]`

Each field in this section is turned into a string and passed to the execution environment of the job.

## Scheduling and running jobs

- The service is started

- If there is a run-next file/record, load it and run anything that was missed

- Load the job definitions

- Parse them and schedule the next run for each job (persist to run-next file)

- Log to stdout whenever a job is run

## Deploying

- Install Python 3.7+

- Install conductor with `pip install git+ssh://git@gitlab.com/ttadmin/conductor.git`

- Alternatively, you can clone it and install it with `pip install -e /path/to/conductor`

- Set environment variables CONDUCTOR_JOBS_DIR and CONDUCTOR_RUN_NEXT_DIR

- Run with `python -m conductor`

## Notes

- Conductor does not respond to keyboard interrupts while sleeping. It will process them as soon as it polls again or a task wakes up. This is a known bug in Python and will be fixed with the release of 3.8.
