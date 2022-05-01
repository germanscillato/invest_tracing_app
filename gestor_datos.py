from operador_BD import (Agregar_portafolio, Leer_resumen_portafolio,
                         Leer_portafolio, Agregar_bco, Cot_usd_BD,
                         Dataframe_BD)

from re import search

from broker import IOL_broker
import pandas_datareader as web  # pip install pandas-datareader
import datetime
import pandas as pd
import requests
# from estrategia import Estrategia

from scraper import Scraper

# TEMPORAL; ESTO VA EN LA APP TRADING MACHINE
from visualizador import interfaz_visual


class Gestor_datos():
    """ Genera importanción, llama a broker para conexión API y llama BD para grabar  """

    def __init__(self):
        self.scraper = Scraper()
        self.dataframe_BD = Dataframe_BD()
        self.cot_dolar = Cot_usd_BD()

    def Activar_Broker(self):
        # instancio lo necesario
        self.broker = IOL_broker()

    def importar_csv_pf(self, archivo_csv):
        # importar el csv solicitado
        df = pd.read_csv(archivo_csv, sep=";")
        return df

    def importar_csv_multiple(self, direccion_path):

        import glob
        import os

        #path = r'C:\Users\Usuario\.spyder-py3\cot_cedear'

        all_files = glob.glob(os.path.join(direccion_path, "*.xlsx"))

        # Grabar excel
        dft = pd.DataFrame()
        i = 0
        for file in all_files:
            df = pd.read_excel(file,
                               index_col=None,
                               names=[
                                   "ticker", "fecha_registro",
                                   "precio_apertura", "precio_max",
                                   "precio_min", "precio_cierre", "volumen"
                               ],
                               header=0)

            dft = dft.append(df)
            i += 1
            print(i)

    def grabar_portafolio_api(self):
        # Trae portafolio desde api
        self.port1 = self.broker.get_portafolio()
        agregar = Agregar_portafolio(self.port1)

        # agregar otros portafolios por API.
        # uniría todo en 1 solo df, return ese df
        print(self.port1)

    def operaciones_api(self):
        # Trae portafolio desde api
        ope = self.broker.get_operaciones()()
        # falta agregar fecha a traer,

        # agregar otras operaciones por API.
        # uniría todo en 1 solo df, return ese df
        return ope

    def grabar_cot_dolar(self):
        try:

            self.cot_dolar.persistir(self.scraper.cotizacion_dolar_hoy())
            return print("Cot Dolar persistido")
        except Exception as e:
            print(e)

    # armar esto con herencia o decoradores
    def grabar_cot_opciones(self):

        try:
            self.dataframe_BD.persistir_df(
                df=self.scraper.cotizacion_opciones(),
                nombre_tabla="cotizacion_opciones")

            return print("Cot opciones ejecutado - ver BD")
        except Exception as e:
            print(e)

    def grabar_cot_bonos(self):

        try:
            self.dataframe_BD.persistir_df(df=self.scraper.cotizacion_bonos(),
                                           nombre_tabla="Cotizacion_Bonos_arg")

            return print("Cot bonos ejecutado - ver BD")
        except Exception as e:
            print(e)

    def grabar_cot_acciones_arg(self):

        try:
            self.dataframe_BD.persistir_df(
                df=self.scraper.cotizacion_acciones_arg(),
                nombre_tabla="Cotizacion_acciones_arg")

            return print("Cot acciones lider ejecutado - ver BD")
        except Exception as e:
            print(e)

    def grabar_cot_cedear(self):

        try:
            self.dataframe_BD.persistir_df(df=self.scraper.cotizacion_cedear(),
                                           nombre_tabla="Cotizacion_cedear")

            return print("Cot Cedear ejecutado - ver BD")
        except Exception as e:
            print(e)

    def grabar_cot_ON(self):
        try:
            self.dataframe_BD.persistir_df(df=self.scraper.cotizacion_ON(),
                                           nombre_tabla="Cotizacion_ON_arg")

            return print("Cot ON arg ejecutado - ver BD")
        except Exception as e:
            print(e)

    # armar esto con herencia o decoradores

    def portafolio_general(self):
        self.resumen = Leer_resumen_portafolio()
        self.df, self.usd = self.resumen.resumen_portafolio()
        n = 0
        for i in self.df["moneda"]:
            if search("usd", i):
                self.df.iloc[n, 1] = self.df.iloc[n, 1] * self.usd

            n += 1
        # Renombra columna cantidad a valorizado para visualizador
        self.df.rename(columns={"cantidad": "valorizado"}, inplace=True)
        return self.df


class Visualizacion():

    def visualizar_datos(self):

        # portafolio = Leer_portafolio()
        #self.df = portafolio.dataframe()
        self.df = gestor_datos.portafolio_general()

        interfaz_visual(self.df)


# Prueba de funcionamiento
if __name__ == "__main__":

    gestor_datos = Gestor_datos()
    # gestor_datos.grabar_cot_cedear()
    visuals = Visualizacion()
    visuals.visualizar_datos()
"""    
    grabar_cot_opciones()
    gestor_datos.grabar_cot_ON()
    gestor_datos.grabar_cot_acciones_arg()
    gestor_datos.grabar_cot_bonos()
    # gestor_datos.Activar_Broker()
    # gestor_datos.grabar_portafolio_api()

    #df_bco = gestor_datos.importar_csv_pf("pf_inv.csv")
    #banco = Agregar_bco(df_bco)
    # print(df_bco)
    #visuals = Visualizacion()
    # visuals.visualizar_datos()
    # Traer datos de API e importar en BD
    # gestor_datos = Gestor_datos()



    def importar_historicos(self):
        
        #Importa datos, con la fuente, symbol y fechas definidas previamente
        #en la instanciación de la clase
        
        # web.Datareader no lleva self, porque no es de la instancia
        try:

            self.datos_historicos = web.DataReader(
                self.symbol,
                self.fuente,
                self.fecha_inicio,
                self.fecha_fin
            )
        # chequear except!!!
        except KeyError:

            return None
        # ej para traer que tipos de datos a utilizar
        # por estrategia (MAC usaria 100 y 400 datos pasados)
        # self.tipo_datos = Estrategia.tipo_datos

        return self.datos_historicos

    def ultimo_datos_backtest(self, max_window, i):
        
        #toma de a 1 dato de los importados en históricos.
        #Se considera la ventana máxima de los datos previos que necesita la estrategia con max_window
        
        n = max_window+i
        fecha = self.fecha_inicio + \
            datetime.timedelta(n)
        # chequeo que no sea sabado y domingo.
        if fecha.strftime("%a") == "Sat":
            fecha = fecha + datetime.timedelta(2)
            return "Sábado - No opera"
        elif fecha.strftime("%a") == "Sun":
            fecha = fecha + datetime.timedelta(1)
            return "Domingo - No opera"
        else:
            # En live usaría web.DataReader, aca tomo de los históricos
            try:
                self.ultimo_dato = self.datos_historicos.iloc[max_window+i]
                return self.ultimo_dato

            except KeyError:

                return None

    def ultimo_datos_live(self, n):
        
        # Importa datos de una fecha especifica, se utiliza para traer el ultimo
        # dato del symbol. Utilizar en live trading.
        

        fecha = self.fecha_fin + datetime.timedelta(n)
        # chequeo que no sea sabado y domingo.
        if fecha.strftime("%a") == "Sat":
            fecha = fecha + datetime.timedelta(2)
            return "Sábado - No opera"
        elif fecha.strftime("%a") == "Sun":
            fecha = fecha + datetime.timedelta(1)
            return "Domingo - No opera"
        else:
            # web.Datareader no lleva self, porque no es de la instancia
            try:
                self.ultimo_dato = web.DataReader(
                    self.symbol,
                    self.fuente,
                    fecha,
                    fecha)
            # chequear except!!!
            except KeyError:

                return None

            return self.ultimo_dato


"""
#if __name__ == "__main__":




# temporal, va en trading_machine. Ejecuta la interfaz visual
