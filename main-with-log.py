import logging
import traceback
import main
import sys



logger = logging.getLogger("oltp-staging")
logger.setLevel(logging.INFO)


handler = logging.FileHandler("oltp-staging.log")
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

def excepthook(type, value, tb):
    # print(type, value, tb)
    logger.critical(type)
    logger.critical(value)
    logger.critical("---- Stack start")
    for l in traceback.extract_tb(tb, limit=None):
        logger.critical("line no: " + str(l.lineno) + " name: " + l.name + " file name: " + l.filename)
    logger.critical("---- Stack end")

    


sys.excepthook = excepthook


logger.info("Start")


main.run_task(logger=logger)
  
  


logger.info("End")




