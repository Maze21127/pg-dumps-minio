from loguru import logger

from pg_dumps_minio.exporter import Exporter
from pg_dumps_minio.utils import cleanup_dirs
from settings import Settings

if __name__ == "__main__":
    settings = Settings()
    exporter = Exporter(settings)
    try:
        exporter.export_all()
    except Exception as e:
        logger.exception(e)
    finally:
        cleanup_dirs(exporter.root_path)
