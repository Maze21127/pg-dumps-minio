from pg_dumps_minio.exporter import Exporter
from settings import Settings

if __name__ == "__main__":
    settings = Settings()
    exporter = Exporter(settings)
    exporter.export_all()
