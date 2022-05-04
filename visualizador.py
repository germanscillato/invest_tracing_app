# pip install cufflinks  . conector pandas plotly
# pip install dash

import dash
from dash.dependencies import Input, Output
import dash_table
import dash_core_components as dcc
import dash_html_components as html
import plotly.express as px
import pandas as pd

df = pd.read_csv("inv.csv", sep=";",
                 dtype={0: str, 1: str, 2: float, 3: float, 4: float, 5: float,
                        6: float, 7: float, 8: float}  # sino los importa como str,
                 )


def interfaz_visual(df):

    # DEFINO GRAFICOS

    # Instancio clase que genera el df. asi lo independizo de este archivo la importación de datos

    # puedo definir color para familia de datos, por ejemplo tipo de activo!
    colors = {
        'background': '#111111',
        'text': '#7FDBFF'}

    fig = px.pie(df, values='valorizado', names='simbolo', hole=0.4)

    # leer docs https://plotly.com/python/reference/pie/
    # agrego etiqueta dentro del graf
    fig.update_traces(textinfo="percent+label",
                      insidetextfont=dict(color="white"))
    fig.update_layout(
        plot_bgcolor=colors['background'],
        paper_bgcolor=colors['background'],
        font_color=colors['text']
    )

    # Esto inicia la app
    external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']

    app = dash.Dash(__name__, prevent_initial_callbacks=True,
                    external_stylesheets=external_stylesheets)

    # https://dash.plotly.com/layout
    # aca van dash components

    # hot_reloading, actualiza solo modificaciones del código o de los datos.
    app.layout = html.Div(style={'backgroundColor': colors['background']}, children=[

        html.H1("PORTAFOLIO INVERSION", style={
                "textAlign": "center", 'color': colors['text']}),



        dcc.Graph(
            id='TORTA',
            figure=fig,
        ),

        dash_table.DataTable(

            id='datatable-interactivity',
            columns=[
                {"name": i, "id": i, "deletable": True,
                    "selectable": True, "hideable": True}
                if i == "iso_alpha3" or i == "year" or i == "id"
                else {"name": i, "id": i, "deletable": True, "selectable": True}
                for i in df.columns
            ],
            data=df.to_dict('records'),  # the contents of the table
            editable=True,              # allow editing of data inside all cells
            # allow filtering of data by user ('native') or not ('none')
            filter_action="none",
            # enables data to be sorted per-column by user or not ('none')
            sort_action="native",
            sort_mode="single",         # sort across 'multi' or 'single' columns
            column_selectable="multi",  # allow users to select 'multi' or 'single' columns
            row_selectable="multi",     # allow users to select 'multi' or 'single' rows
            # choose if user can delete a row (True) or not (False)
            row_deletable=True,
            selected_columns=[],        # ids of columns that user selects
            selected_rows=[],           # indices of rows that user selects
            # all data is passed to the table up-front or not ('none')
            page_action="native",
            page_current=0,             # page number that user is on
            page_size=20,                # number of rows visible per page
            style_cell={                # ensure adequate header width when text is shorter than cell's text
                'minWidth': 95, 'maxWidth': 95, 'width': 95
            },
            style_data={                # overflow cells' content into multiple lines
                'whiteSpace': 'normal',
                'height': 'auto'
            },
        ),

        html.Br(),

        html.Div(id='bar-container')


    ])

    # -------------------------------------------------------------------------------------
    # Create bar chart

    @app.callback(
        Output(component_id='bar-container', component_property='children'),
        [Input(component_id='datatable-interactivity', component_property="derived_virtual_data"),
         Input(component_id='datatable-interactivity',
               component_property='derived_virtual_selected_rows'),
         Input(component_id='datatable-interactivity',
               component_property='derived_virtual_selected_row_ids'),
         Input(component_id='datatable-interactivity',
               component_property='selected_rows'),
         Input(component_id='datatable-interactivity',
               component_property='derived_virtual_indices'),
         Input(component_id='datatable-interactivity',
               component_property='derived_virtual_row_ids'),
         Input(component_id='datatable-interactivity',
               component_property='active_cell'),
         Input(component_id='datatable-interactivity', component_property='selected_cells')]
    )
    def update_bar(all_rows_data, slctd_row_indices, slct_rows_names, slctd_rows,
                   order_of_rows_indices, order_of_rows_names, actv_cell, slctd_cell):
        print('***************************************************************************')
        print('Data across all pages pre or post filtering: {}'.format(all_rows_data))
        print('---------------------------------------------')
        print("Indices of selected rows if part of table after filtering:{}".format(
            slctd_row_indices))
        print("Names of selected rows if part of table after filtering: {}".format(
            slct_rows_names))
        print("Indices of selected rows regardless of filtering results: {}".format(
            slctd_rows))
        print('---------------------------------------------')
        print("Indices of all rows pre or post filtering: {}".format(
            order_of_rows_indices))
        print("Names of all rows pre or post filtering: {}".format(
            order_of_rows_names))
        print("---------------------------------------------")
        print("Complete data of active cell: {}".format(actv_cell))
        print("Complete data of all selected cells: {}".format(slctd_cell))

        dff = pd.DataFrame(all_rows_data)

        # used to highlight selected countries on bar chart
        colors = ['#7FDBFF' if i in slctd_row_indices else '#0074D9'
                  for i in range(len(dff))]

        return dcc.Graph(id='bar-chart',
                         figure=px.bar(
                            data_frame=dff,
                            x="simbolo",
                            y='valorizado',
                            labels={
                                "capital": "Capital en ARS"}
                         ).update_layout(showlegend=False, xaxis={'categoryorder': 'total ascending'})
                         .update_traces(marker_color=colors, hovertemplate="<b>%{y}%</b><extra></extra>")
                         )

    # -------------------------------------------------------------------------------------
    # Highlight selected column

    @app.callback(
        Output('datatable-interactivity', 'style_data_conditional'),
        [Input('datatable-interactivity', 'selected_columns')]
    )
    def update_styles(selected_columns):
        return [{
            'if': {'column_id': i},
            'background_color': '#D2F3FF'
        } for i in selected_columns]

    # class Run_visuals():
    #    def __init__(self):
    app.run_server(debug=True)
    # -------------------------------------------------------------------------------------

# para ejecutarlo

# interfaz_visual(df)

# if __name__ == '__main__':
#    running_app = Run_visuals()
