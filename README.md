# INVEST TRACING APP

Descripción

# Setup

    1. Scraper
    - pip install selenium

    - pip install pandas

    - pip install webdriver-manager # Manage Version and Path for browser.
        https://pypi.org/project/webdriver-manager/

    2. DataBase
    - pip install sqlalchemy

    Combination between ORM sqlalchemy and Pandas.read_sql

    3.Visuals
    - pip install dash

### Section 1

Scraper: Source PPI and IOL for
{
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
