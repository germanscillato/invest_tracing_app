"""
Armar LOGGING

Generalizar, no hacer 1 clase para cada tabla, sino ver como generalizar con heredar,
args o kwargs


DISEÑO : 1 tabla por fuente de datos.
Generar una tabla completa con datos de portafolio global



"""
# IMPORTANCIONES TEMPORALES; BORRAR una vez debug
from scraper import Scraper

####
from tkinter import messagebox
import datetime

from sqlalchemy import create_engine, update
# from sqlalchemy.orm import declarative_base
from sqlalchemy.orm.session import sessionmaker
from sqlalchemy.sql.expression import column
from sqlalchemy.sql import func, desc
from sqlalchemy.sql.schema import PrimaryKeyConstraint
from sqlalchemy.sql.sqltypes import Date, DateTime
from sqlalchemy import Column, Integer, String, Float
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

import pandas as pd

try:

    # from sqlalchemy.orm import declarative_base    esta se usa en el docs

    engine = create_engine(
        r'sqlite:///C:\python39\virtual_env\tradebot\tradebot_version_001\BD.db',
        echo=True)

    Base = declarative_base()

    class Tabla_Portfolio(Base):
        __tablename__ = 'Portfolio_api'

        id = Column(Integer, primary_key=True)
        simbolo = Column(String(100))
        cantidad = Column(Float)
        puntosVariacion = Column(Float)
        variacionDiaria = Column(Float)
        ultimoPrecio = Column(Float)
        precioCompra = Column(Float)
        ganaciaPorcentaje = Column(Float)
        ganaciaDinero = Column(Float)
        valorizado = Column(Float)
        descripcion = Column(String(100))
        pais = Column(String(100))
        mercado = Column(String(20))
        tipo = Column(String(20))
        plazo = Column(String(10))
        moneda = Column(String(20))
        fecha_registro = Column(DateTime)

    class Tabla_Banco(Base):
        __tablename__ = 'PF_Banco_historicos'

        id = Column(Integer, primary_key=True)
        simbolo = Column(String)
        cantidad = Column(Float)
        tasa_esperada = Column(Float)
        tasa_real = Column(Float)
        plazo = Column(Float)
        fecha_inicio = Column(Date)
        fecha_cierre = Column(Date)
        renta_TNA = Column(Float)
        moneda = Column(String)

    class Tabla_Cotizacion_Dolar(Base):
        __tablename__ = 'Cotizacion_Dolar'

        id = Column(Integer, primary_key=True)
        Moneda = Column(String)
        Precio_compra = Column(Float)
        Precio_venta = Column(Float)
        fecha = Column(DateTime)

    Base.metadata.create_all(engine)

    # fabrica para las sesiones a definir mas adelante.
    Session = sessionmaker(bind=engine)

except Exception as e:
    print(e)
    messagebox.showinfo("Error", e)


class Agregar_portafolio():

    def __init__(self, portafolio):

        try:

            session = Session()
            # Borro la tabla para re escribirla. VEr si hacer el commit esta bien o es mas adelante!!!
            session.query(Tabla_Portfolio).delete()
            session.commit()

            for i in range(0, len(portafolio)):
                port = Tabla_Portfolio()
                port.simbolo = portafolio.iloc[i, 0]
                port.cantidad = portafolio.iloc[i, 1]
                port.puntosVariacion = portafolio.iloc[i, 3]
                port.variacionDiaria = portafolio.iloc[i, 4]
                port.ultimoPrecio = portafolio.iloc[i, 5]
                port.precioCompra = portafolio.iloc[i, 6]
                port.ganaciaPorcentaje = portafolio.iloc[i, 7]
                port.ganaciaDinero = portafolio.iloc[i, 8]
                port.valorizado = portafolio.iloc[i, 9]
                port.descripcion = portafolio.iloc[i, 11]
                port.pais = portafolio.iloc[i, 12]
                port.mercado = portafolio.iloc[i, 13]
                port.tipo = portafolio.iloc[i, 14]
                port.plazo = portafolio.iloc[i, 15]
                port.moneda = portafolio.iloc[i, 16]
                # datetime.datetime.now().replace(second=0, microsecond=0)
                port.fecha_registro = datetime.datetime.now().date()
                session.add(port)

            session.commit()
            session.close()

        except Exception as e:
            messagebox.showinfo("Error", e)


class Agregar_bco():

    def __init__(self, df_banco):

        try:

            session = Session()
            for i in range(0, len(df_banco)):

                port = Tabla_Banco()
                port.simbolo = df_banco.iloc[i, 0]
                port.cantidad = df_banco.iloc[i, 1]
                port.tasa_esperada = df_banco.iloc[i, 2]
                port.tasa_real = df_banco.iloc[i, 3]
                port.plazo = df_banco.iloc[i, 4]
                # https://strftime.org/  grabo con dayfirst, y date() toma solo fecha
                port.fecha_inicio = pd.to_datetime(df_banco.iloc[i, 5],
                                                   dayfirst=True).date()
                port.fecha_cierre = pd.to_datetime(df_banco.iloc[i, 6],
                                                   dayfirst=True).date()
                port.renta_TNA = df_banco.iloc[i, 7]
                port.moneda = df_banco.iloc[i, 8]
                session.add(port)

            session.commit()
            session.close()

        except Exception as e:
            print(e)
            messagebox.showinfo("Error", e)


class Agregar_externos():
    """A utilizar 1 vez, actualizar valores solamente"""

    def __init__(self):

        sqlite_connection = engine.connect()

        df = pd.read_csv("usd_ext.csv",
                         sep=";",
                         dtype={
                             0: str,
                             1: float,
                             2: str,
                             3: str
                         })  # sino los importa como str,parse_dates=True

        sqlite_table = "usd_ext"

        df.to_sql(name=sqlite_table, con=sqlite_connection, if_exists='fail')

        sqlite_connection.close()


################ ON #########################


class Agregar_ON():
    """A utilizar 1 vez, actualizar valores cotizacion"""

    def __init__(self):

        sqlite_connection = engine.connect()

        df = pd.read_csv("on.csv",
                         sep=";",
                         dtype={
                             0: str,
                             1: float,
                             2: float,
                             3: float,
                             4: float,
                             5: str,
                             6: str,
                             7: str,
                             8: str,
                             9: str
                         })

        sqlite_table = "Obligaciones_Negociables"

        df.to_sql(name=sqlite_table, con=sqlite_connection, if_exists='fail')

        sqlite_connection.close()

        ################ COT DOLAR ################################


class Cot_usd_BD():

    def persistir(self, df):

        try:

            session = Session()
            for i in range(0, len(df)):

                bd = Tabla_Cotizacion_Dolar()
                bd.Moneda = df.iloc[i, 0]
                bd.Precio_compra = df.iloc[i, 1]
                bd.Precio_venta = df.iloc[i, 2]
                bd.fecha = pd.to_datetime(df.iloc[i, 3])

                session.add(bd)

            session.commit()
            session.close()

        except Exception as e:
            print(e)
            messagebox.showinfo("Error", e)


class Leer_portafolio():
    # https://pandas.pydata.org/docs/reference/api/pandas.read_sql.html
    def dataframe(self):

        try:

            # session = Session()
            # df = session.query(Tabla_Portfolio)

            df = pd.read_sql('SELECT * FROM Portfolio_api', engine)
            # print(df)
            return df

        except Exception as e:
            messagebox.showinfo("Error", e)


class Leer_resumen_portafolio():

    def resumen_portafolio(self):
        " devuelve DF resumen de activos, en ARS y usd, + valor usd ultimo"
        try:

            on = pd.read_sql(
                'SELECT Obligaciones_Negociables.simbolo, Obligaciones_Negociables.cantidad, Obligaciones_Negociables.moneda FROM Obligaciones_Negociables',
                engine)
            usd_ext = pd.read_sql(
                'SELECT usd_ext.simbolo, usd_ext.cantidad , usd_ext.moneda FROM usd_ext',
                engine)

            session = Session()

            port_api = pd.DataFrame(session.query(
                Tabla_Portfolio.simbolo, Tabla_Portfolio.valorizado,
                Tabla_Portfolio.moneda).all(),
                                    columns=on.columns)
            PF = pd.DataFrame(session.query(
                Tabla_Banco.simbolo, Tabla_Banco.cantidad,
                Tabla_Banco.moneda).filter(
                    Tabla_Banco.fecha_cierre > datetime.datetime.now().date()).
                              all(),
                              columns=on.columns)

            usd_precio = session.query(
                Tabla_Cotizacion_Dolar.Precio_compra).order_by(
                    desc(Tabla_Cotizacion_Dolar.fecha)).filter(
                        Tabla_Cotizacion_Dolar.Moneda == "Mep").first()[
                            0]  # sale solo el float

            session.commit()
            session.close()
            # consolida todo en un solo df.
            port_general = pd.DataFrame()

            port_general = port_general.append([on, usd_ext, port_api, PF],
                                               ignore_index=True)
            return port_general, usd_precio

        except Exception as e:
            print(e)


class Dataframe_BD():
    """ Gestión de datos en DF a BD sqlite3. Para persistir DF, ingresar df y nombre. Realizará append si la tabla ya existe, sino crea la tabla.
    No persiste en BD tiempo de registro
    Para leer DF"""

    # def _init_(self):

    engine = create_engine(
        r'sqlite:///C:\python39\virtual_env\tradebot\tradebot_version_001\BD.db',
        echo=True)

    def persistir_df(self, df, nombre_tabla):
        try:

            df.to_sql(name=nombre_tabla,
                      con=engine.connect(),
                      if_exists='append',
                      index=False)
        except Exception as e:
            print(e)

    def leer_df_basico(self, nombre_tabla):
        """Lee ticker, ult cotizacion, volumen, variacion diaria, fecha de registro.
        Ingresar nombre tabla."""
        try:

            self.sql = (f'SELECT {nombre_tabla}.ticker, '
                        f'{nombre_tabla}.precio_cierre, '
                        f'{nombre_tabla}.variacion_diaria, '
                        f'{nombre_tabla}.volumen, '
                        f'{nombre_tabla}.fecha_registro '
                        f' FROM {nombre_tabla}')

            self.df = pd.read_sql(sql=self.sql, con=engine)

            return self.df
        except Exception as e:
            print(e)


# rpueba de funcionamiento
if __name__ == "__main__":

    #data = Dataframe_BD()
    # print(data.leer_df_basico("cotizacion_opciones"))
    scraper = Scraper()
    df = scraper.cotizacion_dolar_hoy()
    # data.persistir_df(df=df,
    #                  nombre_tabla="Cotizacion_Bonos_arg")

    cot_dolar = Cot_usd_BD()
    cot_dolar.persistir(df)
    print(df)
    #df = portafolio.dataframe()
    #print(df, "prueba")
    # iol = IOL_broker()
    # portafolio = iol.get_portafolio()
    # agregar_datos = Agregar_portafolio(portafolio)
