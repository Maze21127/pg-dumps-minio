# pg-dumps-minio

Export all tables in csv from PostgreSQL to S3 bucket

Envs:

| key           | value                                                  |
|---------------|--------------------------------------------------------|
| S3_ENDPOINT   | http://127.0.0.1:9000                                  |
| S3_BUCKET     | bucket_name                                            |
| S3_ACCESS_KEY | access_key                                             |
| S3_SECRET_KEY | secret_key                                             |
| DB_DSN_1      | postgresql://postgres:postgres@localhost:5432/postgres |
| DB_SCHEMA_1   | public                                                 |
| BATCH_SIZE    | 10000                                                  |

For export data from few databases you can set many envs for db_dsn, e.g:
```bash
DB_DSN_dev=postgresql://postgres:postgres@localhost:5432/postgres_dev
DB_SCHEMA_dev=public

DB_DSN_prod=postgresql://postgres:postgres@localhost:5432/postgres_prod
DB_SCHEMA_prod=public
```
Make sure that values after prefix DB_DSN_ and DB_SCHEMA are equals

> ❗delete DB_SCHEMA to copy all schemas from database

## Install and run
Create virtual environment

### Poetry
```bash
poetry install
```

### Pip
```bash
python -m venv venv
source venv/bin/activate
```

for >= 3.10
```bash
pip install -r requirements.txt
```

for < 3.10
```bash
pip install -r requirements-old.txt
```

Create own .env file
```bash
cp .env.dist .env
```
delete DB_SCHEMA to copy all schemas

Run script
```bash
python main.py
```

### Add to crontab

poetry
```bash
cat <(crontab -l) <(echo "@daily cd $(pwd) && poetry run main.py >> $(pwd)/cron_log.txt 2>&1") | crontab - 
```

pip
```bash
cat <(crontab -l) <(echo "@daily cd $(pwd) && $(pwd)/venv/bin/python $(pwd)/main.py >> $(pwd)/cron_log.txt 2>&1") | crontab - 
```

## Copy from Postgres in Docker
### Docker compose
Add volume in db service
```yaml
volumes:
  - /var/tmp/pg_dumps_minio:/var/tmp/pg_dumps_minio
```

### Docker
run with `-v /var/tmp/pg_dumps_minio:/var/tmp/pg_dumps_minio`  
e.g
```bash
export PG_DUMPS_MINIO_DIR=/var/tmp/pg_dumps_minio
docker run -p 5434:5432 \
  -d --name postgres \
  -v ${PG_DUMPS_MINIO_DIR}:${PG_DUMPS_MINIO_DIR} \
  postgres:15
```