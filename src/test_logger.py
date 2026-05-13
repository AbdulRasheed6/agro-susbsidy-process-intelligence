from src.utils.logger import get_logger 
#from logger import get_logger


logger= get_logger(__name__, log_to_file=True)


logger.debug("This is a debug message")
logger.info("This is an info message")
logger.error("This is an error message")

try:
    1 / 0
except Exception:
    logger.exception("An exception occured")