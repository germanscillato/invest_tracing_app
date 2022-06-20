
import datetime

def formato_fecha(x):
        """Para utilizarse con applymap a una serie
        Toma una serie de fecha, verifica su contenido
        y devuelve en formato datetime.

        PPI muestra fechas sin año.
        En datos actuales del día muestra la hora en col fecha

        EVALUAR MEJORA de CONDICIONALES

        """
    

        year = str(datetime.datetime.now().year)
        # Convierte a formato datetime, los que contenga este año como fecha
        if year in x:
            x = datetime.datetime.strptime(x, '%d/%m/%Y')

        # datos con solo hora actual, convierte formato de hora a datetime
        elif ":" in x:
            x = datetime.datetime.strptime(x, '%H:%M')
            # agrega año, mes y día con formato datetime.
            x = x.replace(year=datetime.datetime.now().year,
                                    month=datetime.datetime.now().month,
                                    day=datetime.datetime.now().day)
        
        
        return x
    