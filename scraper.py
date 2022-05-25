

import pandas as pd
import random as random
import datetime as datetime
from time import sleep


from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager


#### CONFIG LOGGING ####
import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

formatter = logging.Formatter('%(asctime)s:%(levelname)s:%(name)s:%(message)s')

file_handler = logging.FileHandler('scraper.log')
file_handler.setFormatter(formatter)

logger.addHandler(file_handler)


#### ####



class Scraper():
    """ Verificar fechas para operaciones intradiarias.
     Con fuente PPI, guarda fecha de cot y fecha de registro, con el resto fecha de registro.
    Puede variar (PPI tiene 20 min de atraso) y fecha de registro es del sistema."""

    def __init__(self):
        """ Al instanciar clase, configura parametros del
        controlador de chrome para evitar error "Message: invalid session id"

                TERMINAR!!!!!!!!!!!!!!!!!!
        """

        try:
           
            # self.argument_chrome = '--disable-blink-features=AutomationControlled'

            self.options_chrome = webdriver.ChromeOptions()
            self.options_chrome.add_experimental_option(
                'excludeSwitches', ['enable-logging'])
            # self.options_chrome.add_argument(self.argument_chrome)

            # Intervalo para tiempo aleatorio de SLEEP en la carga de las páginas
            self.random_inf = 5
            self.random_sup = 10

        except Exception as e:
            logger.exception('init Scraper Class: {}'.format(e))

    # Definicion func auxiliares

    def con_browser(self, url):
        """

        iteración en la func de cada source.


        """
        try:
            # inicializar chrome drive y abrir website. Tiene que ser la ultima versión.
            # , options=self.options_chrome
            # Inicializa chrome drive, y con esto la session. Para que no repita SESSION ID error.
            self.browser = webdriver.Chrome(service=Service(ChromeDriverManager().install()),
                                            options=self.options_chrome)

            self.url = url
            self.browser.get(url=self.url)
            logger.info("getting url: {}".format(self.url))

        except Exception as e:
            logger.exception('connection to web/browser: {}'.format(e))

    def iter_browser(self, col_web, xpath, col_name):
        logger.info("start sleeping")
        sleep(random.uniform(self.random_inf, self.random_sup))
        logger.info("Finish sleeping")

        self.columnas_data = []

        for i in col_web:
            # i is a column in table web
            col_table = f'[{i}]'
            self.data_web = self.browser.find_elements(by=By.XPATH,
                                                       value=xpath + col_table)
            # convierto cada item de cada lista en text, desde webelemnt de selenium

            self.data_web = [item.text for item in self.data_web]
            self.columnas_data.append(self.data_web)

        # index before transpose, then column name.
        self.df = pd.DataFrame(self.columnas_data, index=col_name).transpose()
        logger.info("Scrapping Web")
        return self.df

    def close_browser(self):
        self.browser.close()
        logger.info("Close Browser")


class Source(Scraper):
    """
    Clase de metodos para definir tablas a scrapear , metodos auxiliares para casos particulares:
    PPI para lectura de varias pestañas dentro de 1 misma web.

    """

    def __init__(self):
        super().__init__()
        # Definición parametros a utilziar en todos los source
        self.col_nombre = [
            "ticker", "plazo", "ultimo_precio", "caja_punta_cc",
            "caja_punta_pc", "caja_punta_cv", "caja_punta_pv", "volumen",
            "open", "max", "min", "precio_anterior", "fecha"
        ]

    def limpiar_float(self, x):
        """ Ingresa scalar, devuelve scalar con separadores de miles y decimales
        convertidos del español al ingles
        Reemplaza "-" por ceros. Particular para caja de puntas en IOL
        """

        self.x = x
        self.x = self.x.replace(".", "")
        # reemplazo separador decimal españo ',' por '.'
        self.x = self.x.replace(",", ".")
        # retorna float
        self.x = self.x.replace("-", '0')
        return float(self.x)


class Source_PPI(Source):
    """ VERIFICAR el constructor......"""

    def __init__(self):
        super().__init__()

        # DEFINICION PARAMETROS A UTILZIAR PARA SOURCE PPI UNICAMENTE

        self.xpath = f'//table[@class="table table-striped market-quotes-values tablesorter"]/tbody/tr/td'

        self.securities = {
            'bond': 'https://www.portfoliopersonal.com/Cotizaciones/Bonos',
            'cedear':
            'https://portfoliopersonal.com/Cotizaciones/Acciones/12938',
            'options': 'https://portfoliopersonal.com/Cotizaciones/Opciones',
            'futures': 'https://portfoliopersonal.com/Cotizaciones/Futuros',
            'stock': 'https://portfoliopersonal.com/Cotizaciones/Acciones',
            'adr': 'https://portfoliopersonal.com/Cotizaciones/Acciones/12936'
        }
        # creo numeros de columnas para PPI, sin considerar la col 4 que es var actual vs cierre anterior.
        self.columnas_web = [i for i in range(1, 15) if i != 4]

    def crawler_source_ppi(self, url, security, col_web, xpath, col_name):
        """
        Esta security iría con self. o solo como esta???????????????

        """

        # llamado por herencia a metodo de 'conexión con browser' de super clase 'Scraper'
        self.con_browser(url)
        # def dictionary for results
        self.dict_ppi = {}

        self.bond_stock_check = ['bond', 'stock']

        if security in self.bond_stock_check:

            # definción dictionario conteniendo nombres para click en web.
            bond_stock_scrap = {
                "bond": [
                    "Soberanos en USD", "SOBERANOS PESOS",
                    "SOBERANOS USD-LINK", "PROVINCIALES",
                    "PROVINCIALES USD-LINK", "Cupones PBI", "Corporativos"
                ],
                "stock": ['S&P Merval', 'General']
            }

            # iteración por cada elto de web, para bonos y stocks argy
            try:
                for elto in bond_stock_scrap[security]:
                    self.browser.find_element(by=By.LINK_TEXT,
                                              value=elto).click()
                    logger.info("click en elemento: {}".format(elto))
                    self.dict_ppi["df" + elto] = self.iter_browser(
                        col_web, xpath, col_name)

                # convierto dict de dataframe en un data frame.
                self.df_ppi = pd.concat([v for k, v in self.dict_ppi.items()],
                                        axis=0,
                                        ignore_index=True)
            except Exception as e:
                logger.exception('Exception en scrap de {}: {}'.format(
                    security, e))

        # para resto de security, toma de 1 sola pagina.
        # Metodo iter_browser es heredado de clase Scraper
        else:
            try:
                self.df_ppi = self.iter_browser(col_web, xpath, col_name)
            except Exception as e:
                logger.exception('Exception en scrap de {}: {}'.format(
                    security, e))
        # cierre browser con método heredado de clase Scraper

        self.close_browser()

        return self.df_ppi

    def formato_fecha(self, x):
        """Para utilizarse con applymap a una serie
        Toma una serie de fecha, verifica su contenido
        y devuelve en formato datetime.

        PPI muestra fechas sin año.
        En datos actuales del día muestra la hora en col fecha

        EVALUAR MEJORA de CONDICIONALES

        """
        try:

            self.year = str(datetime.datetime.now().year)
            # Convierte a formato datetime, los que contenga este año como fecha
            if self.year in x:
                self.x = datetime.datetime.strptime(x, '%d/%m/%Y')

            # datos con solo hora actual, convierte formato de hora a datetime
            elif ":" in x:
                self.x = datetime.datetime.strptime(x, '%H:%M')
                # agrega año, mes y día con formato datetime.
                self.x = self.x.replace(year=datetime.datetime.now().year,
                                        month=datetime.datetime.now().month,
                                        day=datetime.datetime.now().day)
            
            
            return self.x
        except Exception as e:
            logger.exception('Exception formato fecha: {}'.format(e))

    def scraper_ppi(self, security):
        """
        securities : bonds, stocks, .....


        """
        # EJECUCION DE SCRAP

        try:

            # a partir de parametro de security, elige dir web a scrappear
            self.url = self.securities[security]
            # aplica metodo auxiliar de crawler para caso BOND y STOCK.
            # Resto de security no crawler, solo scrappear 1 sola web.
            self.df = self.crawler_source_ppi(url=self.url,
                                              security=security,
                                              col_web=self.columnas_web,
                                              xpath=self.xpath,
                                              col_name=self.col_nombre)

            #### Transofrmación de formato de datos #####
            logger.debug("Inicia transformación de datos")
            # Elimina tipo de moneda, deja solo número en ultimo precio. SE agrega en otra BD relación ticker/moneda
            self.df["ultimo_precio"] = self.df["ultimo_precio"].apply(
                lambda x: x.replace("$ ", ""))
            self.df["ultimo_precio"] = self.df["ultimo_precio"].apply(
                lambda x: x.replace("USD ", ""))
            logger.debug("Paso 3")
            # mejoro formato plazo. Elimino 'A '
            self.df["plazo"] = self.df["plazo"].apply(
                lambda x: x.replace("A ", ""))
            logger.debug("Paso 4")
            # convertir str en float, reemplazo . por nada, y ',' por '.'
            # def mascara para columnas float. No incluye ticker, plazo, fecha y moneda.
            self.mask_float = self.df.columns[range(2, 12)].tolist()
            logger.debug("Paso 5")
            # aplica metodo heredada limpiar_float()
            self.df[self.mask_float] = self.df[self.mask_float].applymap(
                lambda x: self.limpiar_float(x))
            logger.debug("Paso 6")
            # aplica metodo propio de la clase SourcePPI, formato fecha
            # Convierte formato str a datetime
            self.df['fecha'] = self.df['fecha'].apply(
                lambda x: self.formato_fecha(x))
            logger.debug("Paso 7")
            ##### FINALIZA TRANSOFRMACION DE DATOS #####
            logger.info("scrap realizado en {}".format(security))
            ########### TEMPORAL PARA DEBBUNG#############
            self.df.to_csv(r'C:/python39/virtual_env/tradebot/df_ppi.csv',
                           index=False)
            print(self.df)
            ########### TEMPORAL PARA DEBBUNG FIN####################

            return self.df

        except Exception as e:
            logger.exception('Exception en scrap de{} en url {}: {}'.format(
                security, self.url, e))


class Source_IOL(Source):
    """ VERIFICAR el constructor......"""

    def __init__(self):
        super().__init__()

        # DEFINICION PARAMETROS A UTILZIAR PARA SOURCE IOL UNICAMENTE
        # Especifico para bonos y obligaciones negociables.
        self.xpath_bond = f'//table[@class="table table-striped table-hover fs12 middleContent cotizacionestb dataTable no-footer"]/tbody/tr/td'
        # General
        self.xpath = f'//table[@class="table table-striped theadh36 table-hover fs12 middleContent cotizacionestb row dataTable no-footer"]/tbody/tr/td'
        self.securities = {
            'bond':
            'https://iol.invertironline.com/mercado/cotizaciones/argentina/bonos/todos',
            'cedear':
            'https://iol.invertironline.com/mercado/cotizaciones/argentina/cedears/todos',
            'options':
            'https://iol.invertironline.com/mercado/cotizaciones/argentina/opciones/todas',
            'futures':
            'IOL no tiene cotización futuros',
            'stock':
            'https://iol.invertironline.com/mercado/cotizaciones/argentina/acciones',
            'adr':
            'https://iol.invertironline.com/mercado/cotizaciones/estados-unidos/adrs/argentina'
        }
        # creo numeros de columnas para IOL, sin considerar la col 4 que es var actual vs cierre anterior.
        self.columnas_web = [i for i in range(1, 13) if i != 3]
        # IOL no tiene para scrap plazo ni fecha. elimino col. Despues las reemplazo por 48hs para plazo, y fecha cuando
        # fue tomada ultimo dato, si es dentro de horarios de mercado....

        self.col_nombre_iol = [
            i for i in self.col_nombre if i not in ["plazo", "fecha"]
        ]

        # Re ordeno para que coincida con datos en WEB: Col Open y col Volumen.
        self.col_nombre_iol.insert(2, self.col_nombre_iol.pop(7))
        self.col_nombre_iol.insert(12, self.col_nombre_iol.pop(7))

    def select_popup(self, popup_click):
        # selecciono opcion de mostrar "todos" en lista pop up.
        logger.info("click popup {}".format(popup_click))
        self.popup = {
            "lider": ['//*[@id="paneles"]/option[1]', "Panel Líderes"],
            "general": ['//*[@id="paneles"]/option[2]', "Panel General"],
            "todos": '//*[@id="cotizaciones_length"]/label/select/option[4]'
        }

        logger.info("click popup paso 2 {}".format(self.popup[popup_click]))

        self.xpath_popup = self.popup[popup_click]
        logger.info("click popup paso 3")
        self.select_element = self.browser.find_element(
            by=By.XPATH, value=self.xpath_popup).click()
        logger.info("click popup paso 3 realizado")
        # self.select_object = Select(self.select_element)
        # self.select_object.select_by_value(self.select_popup[popup_click][1])
        #logger.info("click popup paso 5")

    def cambio_col_bond(self, security, col_web):
        """
        Realiza cambio de nro columna para IOL particular bonos y ON.
        Agrega columna 3 para variación intradiaria, y retira columna 12.
        Calcula precio de apertura y crea columna apertura
        """
        if security == "bond":
            return [i for i in range(1, 12)]
        else:
            return col_web

    def crawler_source_iol(self, url, security, col_web, xpath, col_name):
        """
        AGREGAR ON en bond.
        STOCK son varios click.
        Futuros, log mensaje de que no hay.


        """
        # llamado por herencia a metodo de 'conexión con browser' de super clase 'Scraper'
        self.con_browser(url)
        # def dictionary for results
        self.dict_iol = {}
        self.bond_stock_check = ['bond', 'stock']

        try:

            if security in self.bond_stock_check:

                # definción dictionario conteniendo nombres para click en web.
                self.bond_stock_scrap = {
                    "bond": ["Bonos", "Obligaciones Negociables"],
                    "stock": ['Panel Líderes', 'Panel General']
                }

                # iteración por cada elto de web, para bonos y stocks argy

                for elto in self.bond_stock_scrap[security]:

                    self.browser.find_element(by=By.LINK_TEXT,
                                              value=elto).click()

                    logger.info("click en elemento : {}".format(elto))
                    # Para esperar cargar pagina cuando cambio de pestaña.
                    sleep(4)
                    self.select_popup(popup_click="todos")
                    logger.info(
                        "click en todos para security: {}".format(elto))
                    # cambio las columnas para BOND. Usa xpath especial para bond
                    self.dict_iol["df" + elto] = self.iter_browser(
                        self.cambio_col_bond(security, col_web),
                        self.xpath_bond, col_name)

                    logger.info("genero dict: df + {}".format(elto))

                # convierto dict de dataframe en un data frame.
                self.df_iol = pd.concat([v for k, v in self.dict_iol.items()],
                                        axis=0,
                                        ignore_index=True)

                logger.info("concat de dict: df + {}".format(elto))

                # Se realiza pre transformación especifica para 'bond' : Calculo col OPEN con variación diaria.
                self.df_iol["open"] = self.df_iol["open"].apply(
                    lambda x: x.replace("%", ""))

            # para resto de security, toma de 1 sola pagina.
            # Metodo iter_browser es heredado de clase Scraper
            else:
                self.select_popup(popup_click="todos")
                logger.info(
                    "click en todos para security: {}".format(security))
                self.df_iol = self.iter_browser(col_web, xpath, col_name)

        except Exception as e:
            logger.exception('Exception en scrap de {}: {}'.format(
                security, e))

        # cierre browser con método heredado de clase Scraper

        self.close_browser()

        return self.df_iol

    def scraper_iol(self, security):
        """
        securities : bonds, stocks, .....


        ver si regex mas eficiente que replace and split.
        """
        # EJECUCION DE SCRAP

        try:

            # a partir de parametro de security, elige dir web a scrappear
            self.url = self.securities[security]
            # aplica metodo auxiliar de crawler para caso BOND y STOCK.
            # Resto de security no crawler, solo scrappear 1 sola web.
            self.df = self.crawler_source_iol(url=self.url,
                                              security=security,
                                              col_web=self.columnas_web,
                                              xpath=self.xpath,
                                              col_name=self.col_nombre_iol)

            #### Transofrmación de formato de datos #####
            logger.debug("Inicia transformación de datos")

            # Agrego plazo para consistencia con resto de source. Todo 48hs.
            self.df["plazo"] = ['48HS' for i in range(0, len(self.df))]
            logger.debug("Paso 4")

            # convertir str en float, reemplazo . por nada, y ',' por '.'
            # def mascara para columnas float. No incluye ticker
            self.mask_float = self.df.columns[range(1, 11)].tolist()
            logger.debug("Paso 5")

            # aplica metodo heredada limpiar_float()
            self.df[self.mask_float] = self.df[self.mask_float].applymap(
                lambda x: self.limpiar_float(x))
            logger.debug("Paso 6")
            # Creo columna fecha con fecha actual sistema. No contiene datos de fecha IOL.
            # Evaluar IF con fechas de market open y demas.
            self.df['fecha'] = [
                datetime.datetime.now().replace(second=0, microsecond=0)
                for i in range(0, len(self.df))
            ]

            logger.debug("Paso 7")

            # repr(obj) convierte al objeto en un string, tenga lo que tenga (escapa de \)
            # Reemplazo \ por " ", luego divido y se queda con 1er elto
            self.df['ticker'] = self.df['ticker'].apply(
                lambda x: repr(x).replace("\\", " ").split(" ")[0])
            # por el repr queda una  " ' " , la reemplazo por nada.
            self.df['ticker'] = self.df['ticker'].apply(
                lambda x: x.replace("'", ""))

            # si es bond, hay que calcular el open. ACLARAR QUE ES CALCULADO ESTE VALOR
            if security == "bond":
                self.df["open"] = self.df["ultimo_precio"] / \
                    (self.df["open"]/100+1)

            logger.info(
                "scrap realizado en {} , termina transformación".format(
                    security))
            ##### FINALIZA TRANSOFRMACION DE DATOS #####

            # Reordeno columnas para coherencia con otras source.

            self.df = self.df[self.col_nombre]
            self.df.to_csv(r'C:/python39/virtual_env/tradebot/df_iol.csv',
                           index=False)
            print(self.df)
            return self.df

        except Exception as e:
            logger.exception('Exception en scrap de{} en url {}: {}'.format(
                security, self.url, e))


class Dolar(Source):
    """ completar     """

    def __init__(self):
        super().__init__()

        # DEFINICION PARAMETROS A UTILZIAR PARA scrap dolar
        # General
        self.xpath = f'//table[@class="table table-hover table-striped cotizacionestb dataTable no-footer"]/tbody/tr/td'
        self.url_bolsa= f'https://iol.invertironline.com/mercado/cotizaciones/argentina/monedas' 
        self.url_blue =f'https://www.dolarhoy.com/'

        self.xpath_blue = ['//*[@id="home_0"]/div[2]/section/div/div/div/div[1]/div/div[1]/div/div[1]/div[1]/div[2]'
        ,'//*[@id="home_0"]/div[2]/section/div/div/div/div[1]/div/div[1]/div/div[1]/div[2]/div[2]',
         '//*[@id="home_0"]/div[2]/section/div/div/div/div[1]/div/div[1]/div/div[2]/span'
        ]
        self.col_web = [i for i in range(1,5)]
        self.col_nombre = ["moneda" , "compra" , "venta" , "fecha"]
    def scraper_dolar(self):
        """
        completar.

        """
        # llamado por herencia a metodo de 'conexión con browser' de super clase 'Scraper'
        
        self.con_browser(self.url_bolsa)
        # def dictionary for results
        
        try:
            # Metodo iter_browser es heredado de clase Scraper
          
            logger.info(
                "scrap: dolar URL: ".format(self.url_bolsa))
            self.df_dolar = self.iter_browser(self.col_web, self.xpath,self.col_nombre)

        except Exception as e:
            logger.exception('Exception en scrap de dolar', e)

        # cierre browser con método heredado de clase Scraper
        self.close_browser()
        
        #Conecta con dolarhoy para obtener dato dolar Blue. 
        
        # En caso de usar una vez mas scrap algo puntual, armar una nueva func en clase padre para hacer esto:

        self.browser = webdriver.Chrome(service=Service(ChromeDriverManager().install()),
                                            options=self.options_chrome)
        self.browser.get(self.url_blue)
        sleep(5)
        try:
            # Metodo iter_browser es heredado de clase Scraper

            logger.info("scrap: dolar URL: ".format(self.url_blue))
            self.blue_usd = ["blue"]  
            for xpath in self.xpath_blue:

                self.data_web = self.browser.find_elements(by=By.XPATH,
                                                       value=xpath)
                # convierto cada item de cada lista en text, desde webelemnt de selenium

                self.data_web = [item.text for item in self.data_web]
                self.blue_usd.append(self.data_web[0])
                
            
            logger.info("convierte datos scrap ")
            
            self.df_blue = pd.DataFrame(self.blue_usd, index=self.col_nombre).transpose()
        

        except Exception as e:
            logger.exception('Exception en scrap de dolar blue', e)

        # cierre browser con método heredado de clase Scraper

        self.browser.close()
        
        logger.debug("Inicia transformación de datos")
        self.ccl_mep = [i for i in range(1, 6)]
        # Descarto ultima columna: Variaciones
        self.df_dolar = self.df_dolar.iloc[self.ccl_mep,:]
        logger.debug("Paso 2: Formato fecha")
        self.df_dolar["fecha"]= self.df_dolar["fecha"].apply(
                lambda x: datetime.datetime.strptime(x, '%d/%m/%Y %H:%M:%S'))


        logger.debug("Paso 3: Dolar blue limpio datos")
        mask_blue = ["compra","venta"]
        # elimina simbolo pesos
        self.df_blue[mask_blue]= self.df_blue[mask_blue].applymap(
                lambda x: x.replace("$", ""))
        # elimina texto de dato de fecha
        self.df_blue["fecha"]= self.df_blue["fecha"].apply(
                lambda x: x.replace("Actualizado el ", ""))
        # convierte a formato datetime
        self.df_blue["fecha"]= self.df_blue["fecha"].apply(
                lambda x: datetime.datetime.strptime(x, '%d/%m/%y %I:%M %p'))

        self.df_dolar = pd.concat([self.df_dolar, self.df_blue],axis=0)
        
        return self.df_dolar


if __name__ == "__main__":

    usd = Dolar()
    df = usd.scraper_dolar()

    print(df)

    #df.to_csv(r'C:/python39/virtual_env/tradebot/df.csv', index=False)
