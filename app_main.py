from Controller import Controller
from scraper import Source_PPI, Source_IOL

import datetime
import time
#import calendar
import threading
from time import sleep


class my_thread(threading.Thread):

    def __init__(self, threadId, name, funcion_run, *args):
        # constructor de clase padre. Necesario por low code...
        threading.Thread.__init__(self)
        self.threadId = threadId
        self.name = name
        self.funcion = funcion_run
        self.args = args

    # parametro threadID es la security que va a ejecutar la func. ejemplo ppi.scraper_ppi("cedear")

    def run(self):
        for security in self.args:
            print("Stranting: " + security + "\n")
            self.result = self.funcion(security)
            print("thread: ", self.name, "Exiting: " + security + "\n")
        print()


if __name__ == "__main__":
    # acomodar esto

    t_inicio = datetime.time(hour=10, minute=00, second=0)

    t_final = datetime.time(hour=18, minute=30, second=0)

    ppi = Source_PPI()
    iol = Source_IOL()
    # Adentro del thread no va la función con (). porque la estoy ejecutando. Va sin parentesis,que es el objeto func.
    thread1 = my_thread(1, "iol", iol.scraper_iol, "cedear", "options" ,"stock" )
    #thread2 = my_thread(2, "ON", gestor_datos.grabar_cot_ON)
    thread3 = my_thread(3, "ppi", ppi.scraper_ppi, "bond", "adr" , "futures")

    thread1.start()
    # thread2.start()
    thread3.start()
    thread1.join()  # esperamos que thread termine de ejecutar para finalizar programa
    # thread2.join()
    thread3.join()
    print("listo terminado thread ppal")
    """
    while True:

        dt = datetime.datetime.now()
        print(f"started at {dt}")
        gestor_datos = Gestor_datos()
        if dt.date().weekday() != 5 | 6:
            dt = datetime.datetime.now().time()
            while dt > t_inicio and dt < t_final:

                gestor_datos.grabar_cot_opciones()
                gestor_datos.grabar_cot_ON()
                gestor_datos.grabar_cot_acciones_arg()
                gestor_datos.grabar_cot_bonos()
                gestor_datos.grabar_cot_cedear()

                time.sleep(30*60)
                print(f"Nuevo loop en {dt}")
                dt = datetime.datetime.now().time()

            else:
                print(f"Terminó en {dt}")
                print("Horario no Bursatil")
                # duerme 1 hora y verifica que inicie tiempo bursatil
                time.sleep(60*60)
        else:
            # Duerme 8 hs, para no pasarme por si es un domingo
            print("durmiendo, horario finde")
            time.sleep(8*60*60)

"""