"""Archivo referido a calculos      """

import pandas as pd
import re
import numpy as np

import datetime

from operador_BD import Dataframe_BD


# Grabar en BD el calculo de rendimiento diario. Ver cual es el ultimo rendidmienot calculado, y partir con el calculo
# desde esa fecha.
# Precio operado reemplace a cotización para el día de la fecha de operación, para el resto no.




############# FILTRADO OPERACIONES TERMINADAS DE COMPRA Y VENTA ##########################################################################################

########################################################################################################################################################################################################################
####################################################################################################################################################################################
# CUANDO PASE A QUERY, YA TRAER TODO FILATRADO: OPERACIONES TERMIANDAS, COMPRA Y VENTA, ETC..
# El historico ya va a estar grabado, asi qeu traiga solo lo no "evaluado" de fechas
# las ventas en negativo. Multiplico cantidad por -1




"""

# Agrupo movimientos de capital, de forma de diferenciar ganancia con ingreso.
#  Multiplico cantOpe con precioOpe, y lo sumo. agrupo todo por fechas.

# AGREGO A MANO EN op_f_grouped cot de JPM y BABA cuando se compraron en USD.
# BABA 10/8/2021 en pesos
cot_babad_pesos = 3859
op_f_grouped.iloc[17,1] = cot_babad_pesos
# JPM 21/10/2021 en pesos
cot_JPM_pesos= 6913
op_f_grouped.iloc[25,1] = cot_JPM_pesos

"""
##################################################################################################################################################################
########################################################################################################################################################################################################################
############################################################################################################################################################################################################################################################
##### A TENER EN CUENTA ##########




"""

comisiones e impuestos tener en cuenta
hacer una tabla auxiliar con los ratios para cada ticker, 
ejemplo t2x2 10:1 ; tmb los cedear para calc CCL
tarifas : AHORA ARTIFICIAL SUMO A MONTO INVERTIDO UN 0.5%

BABAD se lo sume a BABA. pensar como tener en cuenta esta salvedad.
# separo cartera en USD y en ARS? o solo considero ars y lo paso a pesos? 
ver como diferencio lo que compre de lo que tenía, 
# porque va a haber diferencia entre precios en un mismo día


"""

####################################################




class Historic_Profit():

    def str_to_date(self,x):
        """func para convertir str to date en index de dataframe con doble index"""
        return datetime.datetime.fromisoformat(str(x)).date()


        # Verificacion de simbolos de distinta moneda para misma empresa, ej BABA y BABAD


    def rev_simbolo_D(self,df):
        """
        Input: Dataframe con columnas con los ticker , y filas con fechas y nominales
        regex que saque si tiene al final una D, y compare con el resto
        si encuentra coincidencia, que sume las columnas. Compara si retirando la D,
        esta en las columnas de 
        """
        self.df = df # ESTA BIEN ESTO????
        self.pattern  = '^[a-z|A-Z|0-9]+[^D]\s?D{1}$'

        for col in self.df.columns:
                
            match = re.search(self.pattern, col)

            if match and col.replace("D","") in self.df.columns:
                    # agregar un log de cuales se sumo.
                    self.df[col.replace("D","")]= self.df[col.replace("D","")] + self.df.pop(col)
        return self.df


    def df_resultado(self,operations,precio):
        """Construccion de dataframe de resultados, con fechas de operaciones

        Con compras suma, completo hasta el fondo (apply), y despues con ventas resta.
        2 columnas mas , 1 para precio de compra, y otro para suma del nominal x precio, 

        sumar nominales en el mismo día y promedio los precios de compra
        porq en 1 dia puede haber varias compras del mismo, y solo va a tomar la primera
        ejemplo PG, los que estan en el 11-11-21, solo toma la primera compra, el resto NO!

        Logica de calculo:
        - *-1 col nominales filtrado por ventas
        - defino df tiempos, con columnas para c/ticker. 
        - creacion col para c/ticker con ceros.
        - Convertir a doble index DF operaciones.
        - Completo df con nominales para cada fecha y simbolo de operations.
        - Acumulada de compras y ventas de nominales en df resultado. VER DE OPTIMIZAR!
        - Sumo nominales de ticker en usd y ars. Con metodo rev_simbolo_D
        - Calculo el mov de capital invertido en func de la fecha (sumo para cada fecha monto operado).Para calculo de 
        rendimiento sin afectarlo por ingreso de capital
        - Realiza merge entre dataframe de nominales y mov de capital.
        - Convierto NA con 0 para los días que nohubo cambio de capital.
        - Acumulo capital
        - Agrupo promediando precios por fecha e instrumento
        - Creo fila valorizado con cero para grabar las sumas de los nominales*cotizacion
        - Calculo de valorizado por fecha. Multiplica nominal por precio.
        - CALCULO DE % GANANCIAS, comparando con cotización.
        # optimizar con NUMPY  
        https://stackoverflow.com/questions/16476924/how-to-iterate-over-rows-in-a-dataframe-in-pandas
        # VALORES DE COTIZACION MAL, revisar BD para el calculo. 
        # AHora voy a poner NA en valorizado, y despues lo reemplazo con el promedio.

        """
        self.operations = operations
     
            
        # Nominales ventas a negativo.
        mask_venta = self.operations.tipo.isin(["Venta"])
        self.operations = self.operations.loc[ mask_venta,["cantidadOperada"]]*-1

        
        # defino df tiempos, con columnas para c/ticker. 
        self.dti = datetime.datetime.strptime(self.operations.loc[:,"fechaOperada"].min(), '%Y-%m-%d')
        self.dtf = datetime.datetime.strptime(self.operations.loc[:,"fechaOperada"].max(), '%Y-%m-%d')
        # rango fechas inicio y fin de operaciones
        self.dates = pd.Series(pd.bdate_range( start = self.dti, end = self.dtf ))


        # creacion col para c/ticker con ceros.
        self.df= pd.DataFrame(0,
                    index = self.dates, 
                    columns = self.operations["simbolo"].unique() )


        # Convertir a doble index DF operaciones. 
        self.operations = pd.pivot_table(data = self.operations ,
            values = ["cantidadOperada" ,"precioOperado" ],
            index = ["fechaOperada" , "simbolo"] ,
            aggfunc = {"cantidadOperada" : "sum" ,
             "precioOperado": "mean"} )


        # completo df con nominales para cada fecha y simbolo de operations.
        # Doble index, index[0]  es fecha y 1 es simbolo
        for index in self.operations.index:
            self.df.loc[
                index[0],
                index[1]
                ] =  self.operations.loc[index,"cantidadOperada"]


        # acumulada de compras y ventas de nominales en df resultado. VER DE OPTIMZIAR!
        for j in range(0,len(self.df.columns)):
            self.val = 0
            for i in range(0, len(self.df)):
                self.val += self.df.iloc[i,j]
                
                self.df.iloc[i,j]= self.val
                

        # Sumo nominales de ticker en usd y ars.
        self.df = self.rev_simbolo_D(self.df)
    

        # Calculo el capital invertido en func de la fecha (sumo para cada fecha monto operado)
        self.mov_capital =  self.operations['cantidadOperada']*self.operations['precioOperado']
        #elimino index de simbolo (ya no aplica por los sume, estan por fecha)
        self.mov_capital= self.mov_capital.droplevel(level=1,axis=0)
        # Agrupo por fecha. Devuelve una pd.serie
        self.mov_capital = self.mov_capital.groupby(by = "fechaOperada").agg("sum")
        #Renombro serie
        self.mov_capital.name = "ingresoCapital"
        # Convierto index a datetime, para realizar merge.
        index_cap = pd.Series(self.mov_capital.index)
        self.mov_capital.index = index_cap.apply(lambda x : self.str_to_date(x))


        # OJO CON ESTO, creo que ya no va:  Genero index de fecha registro, para merge
        self.df = self.df.merge(self.mov_capital,
                                how="outer" ,
                                left_index=True,
                                right_index=True )
        

        # Convierto NA con 0 para los días que nohubo cambio de capital.
        self.df["ingresoCapital"].fillna(0,inplace = True)


        # Acumulo capital
        self.val = 0
        for i in self.df.index:
            self.val += self.df.loc[i,"ingresoCapital"]
            self.df.loc[i, "ingresoCapital"]= self.val

     
        # Agrupo promediando precios por fecha e instrumento 
        precio_grouped = pd.pivot_table(data = precio,
                                    values = ["Precio"],
                                    index = ["Fecha" , "Instrumento"] ,
                                    aggfunc =  {"Precio":"mean"} )


        

        # Creo fila valorizado para grabar las sumas de los nominales*cotizacion
        self.df['valorizado'] = 0
        # Calculo de valorizado por fecha. Multiplica nominal por precio.
        # Iteracion por cada fila(fecha):
        # Iter por cada col (ticker)
        # si es 0, no hace nada. Si no es cero, busca el precio del valor de la col,
        # multiplica por precio y suma a valorizado
        for i in self.df.index:
            self.val = 0 # se resetea para cada iter de fila
            
            for j in self.df.columns[:-2]: # resta 2 por ult col no son ticker
                if self.df.loc[i,j] == 0: #  si es cero pasa a la proxima col
                    pass
                else:
                    try:
                        self.val += float(precio_grouped.loc[i,j]*self.df.loc[i,j])
                    except:
                        self.val = np.NaN
                        break
            
            self.df.loc[i,'valorizado'] = self.val
            

        self.df['valorizado'].fillna(method = "ffill", inplace = True)
            

        self.ganancia = self.df['valorizado'] - self.df['ingresoCapital']

        return self.ganancia 


"""
def check_float(x):
            if isinstance(x, float) | isinstance(x, int):
                return x
            else:
                x = 0
                return x

"""
class Day_profit(Historic_Profit):
    "Hereda de hisorico formas de calculo"
    pass

if __name__ == "__main__":




