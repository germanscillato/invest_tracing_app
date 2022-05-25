"""
Armar LOGGING

Generalizar, no hacer 1 clase para cada tabla, sino ver como generalizar con heredar,
args o kwargs


DISEÑO : 1 tabla por fuente de datos.
Generar una tabla completa con datos de portafolio global



"""
# IMPORTANCIONES TEMPORALES; BORRAR una vez debug
from email import header
from wsgiref import headers
from scraper import Source_PPI, Source_IOL

####
from tkinter import messagebox
import datetime

from sqlalchemy import create_engine, update

from sqlalchemy.orm.session import sessionmaker
from sqlalchemy.sql.expression import column
from sqlalchemy.sql import func, desc
from sqlalchemy.sql.schema import PrimaryKeyConstraint
from sqlalchemy.sql.sqltypes import Date, DateTime
from sqlalchemy import Column, Integer, String, Float
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

import pandas as pd
from tabulate import tabulate
import os


# AUTOMATIZAR QUE ELIJA EL DIRECTORIO ACTUAL DE TRABAJO.
BD_path = 'sqlite:///' + os.getcwd() + '\BD.db'

try:

    # from sqlalchemy.orm import declarative_base    esta se usa en el docs

    engine = create_engine(
        BD_path,
        echo=True)

except Exception as e:
    print(e)
    messagebox.showinfo("Error", e)


class Agregar_externos():
    """A utilizar 1 vez, actualizar valores solamente"""

    def __init__(self):

        # With statement close connection. Oficial docs. 
        # https://docs.sqlalchemy.org/en/13/core/connections.html
        try: 
        
            with engine.connect() as conn:
                
                df = pd.read_csv("usd_ext.csv",
                            sep=";",
                            dtype={
                                0: str,
                                1: float,
                                2: str,
                                3: str
                            })  # sino los importa como str,parse_dates=True

                sqlite_table = "usd_ext"
            

                df.to_sql(name=sqlite_table, con=conn, if_exists='fail')
        except Exception as e:
            print(e) 

        


################ ON #########################
"""
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

    def dataframe(self):

        try: 
        
            with engine.connect() as conn:



                df = pd.read_sql('SELECT * FROM Portfolio_api', conn)
    
                return df

        except Exception as e:
            messagebox.showinfo("Error", e)
"""

class Dataframe_BD():
    """ Gestión de datos en DF a BD sqlite3. Para persistir DF, ingresar df y nombre. 
    Realizará append si la tabla ya existe, sino crea la tabla.
    No persiste en BD tiempo de registro
    Para leer DF"""

    
    def persistir_df(self, df, table_name):
        try:
            
        
            with engine.connect() as conn:
                df.to_sql(name=table_name,
                        con=conn,
                        if_exists='append',
                        index=False)
        
        except Exception as e:
            print(e)

    def leer_df_basico(self, table_name):
        """ Ingresar nombre tabla. Trae tabla completa sin query"""
        try:
            with engine.connect() as conn:

                self.df = pd.read_sql(sql=table_name, con=conn)

                return self.df
        except Exception as e:
            print(e)

    def table_query(self,table_name):
        """ Ingresar nombre tabla y query a solicitar"""
        col ="tipo, simbolo, fechaOperada,"
        col_agg_func = "cantidadOperada, precioOperado"
        # FALTA: groupbygroupb

        query = '''SELECT tipo,
                        simbolo,
                        fechaOperada,
                        SUM(cantidadOperada),
                        AVG(precioOperado)
                FROM {}
                WHERE estado = 'terminada' AND tipo IN ('Compra' , 'Venta')
                GROUP BY "cantidadOperada", "precioOperado"
                ORDER BY fechaOperada ASC
                
                '''.format(table_name)
        try:
            with engine.connect() as conn:

                self.df = pd.read_sql_query(sql=query, con=conn)

                return self.df
        except Exception as e:
            print(e)


# pueba de funcionamiento
if __name__ == "__main__":

    df_bd = Dataframe_BD()
    df = df_bd.table_query("operaciones_IOL")
    print("\n",tabulate(df.head(20),
    headers = 'keys' ,
     tablefmt = "grid"))
