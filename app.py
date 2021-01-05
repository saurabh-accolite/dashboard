import dash 
import dash_bootstrap_components as dbc 

external_stylesheets=[dbc.themes.MATERIA]

app = dash.Dash(__name__, external_stylesheets=external_stylesheets ,prevent_initial_callbacks=False)
app.title = 'Pulse Dashboard'

app.config.suppress_callback_exceptions = True