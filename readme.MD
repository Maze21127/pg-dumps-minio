# pg-dumps-minio

Export all tables from PostgreSQL schema to S3 bucket

Envs:

| key           | value                                                  |
|---------------|--------------------------------------------------------|
| S3_ENDPOINT   | http://127.0.0.1                                       |
| S3_BUCKET     | bucket_name                                            |
| S3_ACCESS_KEY | access_key                                             |
| S3_SECRET_KEY | secret_key                                             |
| DB_DSN        | postgresql://postgres:postgres@localhost:5432/postgres |
| DB_SCHEMA     | public                                                 |

## Install and run
Create environment
```bash
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

Create own .env file
```bash
cp .env.dist .env
```

Run script
```bash
python main.py
```
