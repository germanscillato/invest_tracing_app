from Controller import Controller
from scraper import Source_PPI, Source_IOL

import datetime
import time
#import calendar
import threading
from time import sleep




import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

formatter = logging.Formatter('%(asctime)s:%(levelname)s:%(name)s:%(message)s')

file_handler = logging.FileHandler('App_main.log')
file_handler.setFormatter(formatter)

logger.addHandler(file_handler)


class my_thread(threading.Thread):

    def __init__(self, threadId, source, funcion_run, *args):
        # constructor de clase padre. Necesario por low code...
        threading.Thread.__init__(self)
        self.threadId = threadId
        self.source = source
        self.funcion = funcion_run
        self.args = args

    # parametro threadID es la security que va a ejecutar la func. ejemplo ppi.scraper_ppi("cedear")

    def run(self):
        # Iteracion sobre lista de argumentos adicionales
        for security in self.args:
            logger.debug("Stranting: " + security + "\n")
            
            self.result = self.funcion(self.source, security)
            logger.debug("thread:{}  Exiting: {} ".format(self.source, security ))


if __name__ == "__main__":
    # acomodar esto

    t_inicio = datetime.time(hour=11, minute=00, second=0)

    t_final = datetime.time(hour=21, minute=30, second=0)

    ppi = Source_PPI()
    iol = Source_IOL()
    controller = Controller()
    
    
    while True:
            #Controller.security_selector(source= "PPI" , security= "bond")
        dt = datetime.datetime.now()
        logger.debug(f"started at {dt}")

        if dt.date().weekday() != 5 | 6:
            dt = datetime.datetime.now().time()
            while dt > t_inicio and dt < t_final:
                # Adentro del thread no va la función con (). porque la estoy ejecutando. Va sin parentesis,que es el objeto func.
                thread1 = my_thread(1,
                                    "IOL",
                                    controller.security_selector,
                                    "cedear",
                                    "options"  )
                #thread2 = my_thread(2, "ON", gestor_datos.grabar_cot_ON)
                thread3 = my_thread(3,
                                    "PPI",
                                    controller.security_selector,
                                    "bond", 
                                    "adr" , 
                                    "futures",
                                    "stock")

                thread1.start()
                # thread2.start()
                thread3.start()
                thread1.join()  # esperamos que thread termine de ejecutar para finalizar programa
                # thread2.join()
                thread3.join()
                logger.debug("listo terminado thread ppal")
                

                time.sleep(30*60)
                logger.debug(f"Nuevo loop en {dt}") 
                dt = datetime.datetime.now().time()

            else:
                logger.debug(f"Terminó en {dt}")
                logger.debug("Horario no Bursatil")
                # duerme 1 hora y verifica que inicie tiempo bursatil
                time.sleep(60*60)
        else:
            # Duerme 8 hs, para no pasarme por si es un domingo
            logger.debug("durmiendo, horario finde")
            time.sleep(8*60*60)

