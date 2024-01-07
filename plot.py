from dash import dcc, html, Dash
from dash.dependencies import Input, Output
import plotly.express as px
import pandas as pd
import plotly.express as px
from dash.exceptions import PreventUpdate


# Assuming 'consolidated_logs_df' is a DataFrame defined outside this function
# You should pass this DataFrame as a parameter to the run_dash_app function

def run_dash_app(df):
    app = Dash(__name__)

    # Generate options for the dropdowns based on the DataFrame
    event_options = [{'label': i, 'value': i} for i in df['eventName'].unique()]
    ip_options = [{'label': i, 'value': i} for i in df['sourceIPAddress'].unique()]

    app.layout = html.Div([
        html.H1("Interactive Event Filtering"),
        dcc.Dropdown(
            id='event-dropdown',
            options=event_options,
            value=[option['value'] for option in event_options],
            multi=True
        ),
        dcc.Dropdown(
            id='ip-dropdown',
            options=ip_options,
            value=[option['value'] for option in ip_options],
            multi=True
        ),
        dcc.Graph(id='events-graph')
    ])

    @app.callback(
        Output('events-graph', 'figure'),
        [Input('event-dropdown', 'value'),
         Input('ip-dropdown', 'value')]
    )
    def update_graph(selected_events, selected_ips):
        # Filter based on selected event names and IP addresses
        filtered_df = df[
            df['eventName'].isin(selected_events) & 
            df['sourceIPAddress'].isin(selected_ips)
        ]

        # Create the figure using Plotly Express
        fig = px.scatter(
            filtered_df,
            x='eventTime',
            y='sourceIPAddress',
            color='eventName',
            title="Event Activities Over Time",
            hover_data=['eventID', 'eventName']
        )
        
        # Customize hover template to display additional data
        fig.update_traces(
            hovertemplate='<b>%{y}</b><br>Time: %{x|%Y-%m-%d %H:%M:%S}<br>Event: %{marker.color}<br>ID: %{customdata[0]}'
        )

        return fig

    app.run_server(debug=True)

# Function to read CSV and process data
def load_and_process_csv(csv_file, exclude_detections=None):
    exclude_detections = exclude_detections or []
    df = pd.read_csv(csv_file)
    df.fillna('NAN', inplace=True)
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    df['detections_y'] = df['detections'].astype('category').cat.codes
    df = df[~df['detections'].isin(exclude_detections)]
    return df

# Function to create Plotly figure
def create_plotly_figure(df):
    fig = px.scatter(
        df,
        x='timestamp',
        y='detections_y',
        color='detections',
        labels={'detections_y': 'Detections'},
        title='Sigma Detections Over Time'
    )
    
    # Update layout to remove y-axis labels
    fig.update_layout(
        yaxis=dict(showticklabels=False),
        xaxis_title='Timestamp',
        legend_title='Event Detections'
    )
    
    # You can further customize the hover_data to include any additional details you want to display
    # and also customize markers if necessary.
    
    return fig

def run_wlogs_app(csv_file, exclude_detections=None):
    df = load_and_process_csv(csv_file, exclude_detections)
    
    app = Dash(__name__)
    
    event_options = [{'label': i, 'value': i} for i in df['detections'].unique()]

    app.layout = html.Div([
        html.H1("Interactive Sigma Detections"),
        dcc.Dropdown(
            id='event-dropdown',
            options=event_options,
            value=[option['value'] for option in event_options],
            multi=True
        ),
        dcc.Graph(id='sigma-graph')
    ])

    @app.callback(
        Output('sigma-graph', 'figure'),
        [Input('event-dropdown', 'value')]
    )
    def update_graph(selected_detections):
    # Ensure selected_detections is always a list, even if it's a single value
        if not selected_detections:
            raise PreventUpdate

        # If selected_detections is not already a list (e.g., when a single value is selected), make it a list
        if not isinstance(selected_detections, list):
            selected_detections = [selected_detections]

        # Now we can safely use isin() as we're sure selected_detections is a list
        filtered_df = df[df['detections'].isin(selected_detections)]
        fig = create_plotly_figure(filtered_df)
        return fig

    
    app.run_server(debug=True)

