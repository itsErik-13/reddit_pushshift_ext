import logging
import os

def setup_logging(name="log", log_level=logging.INFO, log_file="log.log", log_dir="logs", max_bytes=1024*1024*16, backup_count=5):
    """Sets up logging to both console and file."""

    log = logging.getLogger(name)
    log.setLevel(log_level) 
    
    

    log_formatter = logging.Formatter('%(asctime)s - %(levelname)s: %(message)s')

    log_str_handler = logging.StreamHandler()
    log_str_handler.setFormatter(log_formatter)
    log.addHandler(log_str_handler)

    if not os.path.exists(log_dir):
        os.makedirs(log_dir)

    log_file_path = os.path.join(log_dir, log_file)
    log_file_handler = logging.handlers.RotatingFileHandler(
        log_file_path, maxBytes=max_bytes, backupCount=backup_count, encoding="utf-8")
    log_file_handler.setFormatter(log_formatter)
    log.addHandler(log_file_handler)

    return log