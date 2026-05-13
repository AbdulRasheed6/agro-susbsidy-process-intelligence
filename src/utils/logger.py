import  logging
import os 
import sys
import json
from pathlib import Path
from logging.handlers import RotatingFileHandler
from datetime import datetime, timezone
from src.utils.config import LOG_LEVEL, LOG_FORMAT, LOG_DIR


PROJECT_NAME= "lakehouse"

#LOG_LEVEL= os.getenv("LOG_LEVEL", "INFO").upper()
LOG_AS_JSON= os.getenv("LOG_FORMAT", "TEXT").upper()=="JSON"



# Custom Json Formatter


class JSONFormatter(logging.Formatter):
    """"Outputs  logs in structured json format (UTC timestamps)"""

    def format(self, record):
        log_record= {
            "timestamp": datetime.now(timezone.utc).isoformat() + "Z", # creates a utc timestamp in ISO format
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "module": record.module,
            "line": record.lineno,



        }

        if record.exec_info:
            log_record["exception"]= self.formatException(record.exec_info)

        return json.dumps(log_record)
    



def get_logger(name:str, log_to_file:bool=False) -> logging.Logger:

    if not name.startswith(PROJECT_NAME):
        name= f"{PROJECT_NAME}.{name}" # ensures every logger name is prefixed lakeouse
    
    logger= logging.getLogger(name) # gets or creates logging instance

    if logger.handlers: # Prevents duplicate andlers
        return logger

    logger.setLevel(LOG_LEVEL)
    logger.propagate= False # dissables propagation

    if LOG_AS_JSON:
        formatter= JSONFormatter() #use custom jsonformatter

    else: 
        formatter= logging.Formatter(
            fmt="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S"
        )

    # console handler

    console_handler= logging.StreamHandler(sys.stdout) # outputs los to terminal
    console_handler.setLevel(LOG_LEVEL)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)


    #file handler

    if log_to_file:
        file_handler= RotatingFileHandler(
            LOG_DIR/"lakehouse_core.log", #creates this in logs dir
            maxBytes= 10 * 1024 *1024 , # rotates when file exceeds 5MB
            backupCount=5, # keeps 5 old rotated files
        )

        file_handler.setLevel(LOG_LEVEL)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)



    return logger

