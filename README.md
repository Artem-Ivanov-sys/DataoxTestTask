# README

## Running

You must create `.env` in script directory file to store some configuration there. Example of `.env` structure:
```
SCHEDULED_TIME="12:00"
DUMP_TIME="12:00"
POSTGRES_DB="dbmain"
POSTGRES_USER="admin"
POSTGRES_PASSWORD="admin"
```

To run the script, you are gonna need docker compose and run this command in script's root directory:
```
docker compose up --build
```

## Structure

* `index.py` - main script
* `docker-compose.yaml` - docker compose file
* `Dockerfile` - dockerfile for configuring app container
* `dumps/` - folder for database dumps
* `dumps/init.sql` - database initialization SQL script
