
import pandas as pd

#from broker import IOL_broker
from operador_BD import Dataframe_BD
from scraper import Source_PPI ,Source_IOL

# TEMPORAL; ESTO VA EN LA APP TRADING MACHINE
#from visualizador import interfaz_visual


#### CONFIG LOGGING ########
import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

formatter = logging.Formatter('%(asctime)s:%(levelname)s:%(name)s:%(message)s')

file_handler = logging.FileHandler('Controller.log')
file_handler.setFormatter(formatter)

logger.addHandler(file_handler)


#### ####


class Controller():
    """ Genera importanción, llama a broker para conexión API y
     llama BD para grabar  """

    def __init__(self):
        self.ppi = Source_PPI()
        self.iol = Source_IOL()
        self.dataframe_BD = Dataframe_BD()

    def security_selector(self, source,security):
        """
        Source = PPI ; IOL
        Security = 'bond'; 'cedear';'options'; futures' (SOlo PPI); 'stock';
        'adr'
        
        """
        self.list_securities = ['bond', 'cedear','options','futures' , 'stock', 'adr']
        self.list_source = ["PPI", "IOL" ]
        # Verifico Securities y clases sean correctas
        if security in self.list_securities and source in self.list_source:
            # Selecciono que metodo de scraper voya usar en func de la Source selecionada
            self.nombre_tabla = security+"_price"

            if source == "PPI":

                df = self.ppi.scraper_ppi(security)
                
                
            else:
                df = self.iol.scraper_iol(security)

            
            try:
                self.dataframe_BD.persistir_df(
                df=df,
                nombre_tabla=self.nombre_tabla)

            except Exception as e:
                logger.exception('Persistir {security} y source {source} error: {}'.format(e))


        else: 
            logger.exception('Cargar Security o Source correcta: {source} {security}')


    # armar esto con herencia o decoradores
    def grabar_cot_opciones(self):

        try:
            self.dataframe_BD.persistir_df(
                df=self.scraper.cotizacion_opciones(),
                nombre_tabla="cotizacion_opciones")

        except Exception as e:
            logger.exception('Grabar cotizacion opcionse: {}'.format(e))

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
    """
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
    """
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

    def operaciones_api(self):
        # Trae portafolio desde api
        ope = self.broker.get_operaciones()()
        # falta agregar fecha a traer,

        # agregar otras operaciones por API.
        # uniría todo en 1 solo df, return ese df
        return ope    
    
    """
    def grabar_portafolio_api(self):
        # Trae portafolio desde api
        self.port1 = self.broker.get_portafolio()
        agregar = Agregar_portafolio(self.port1)

        # agregar otros portafolios por API.
        # uniría todo en 1 solo df, return ese df
        print(self.port1)
    """

"""
class Visualizacion():

    def visualizar_datos(self):

        # portafolio = Leer_portafolio()
        #self.df = portafolio.dataframe()
        self.df = Controller.portafolio_general()

        interfaz_visual(self.df)
"""

# Prueba de funcionamiento
if __name__ == "__main__":

    Controller = Controller()
    Controller.security_selector(source= "PPI" , security= "bond")
    

