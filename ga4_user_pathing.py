import pandas as pd
import plotly.graph_objects as go
from google.cloud import bigquery
import re
import pytz
import os


all_colors = [
 '#8E8C99',
 '#35297F',
 '#2D1BB5',
 '#80447B',
 '#F40EA4',
 '#8EE7FF',
 '#FFB1FF',
 '#197EB2',
 '#F861C3',
 '#6F1C67',
 '#8E8C99',
 '#35297F',
 '#2D1BB5',
 '#B61CA7',
 '#F40EA4',
 '#35297F',
 '#2D1BB5',
 '#80447B',
 '#F40EA4',
 '#8EE7FF',
 '#FFB1FF',
 '#197EB2',
 '#F861C3',
 '#6F1C67',
 '#8E8C99',
 '#35297F',
 '#2D1BB5',
 '#B61CA7',
 '#F40EA4',
 '#35297F',
 '#2D1BB5',
 '#80447B',
 '#F40EA4',
 '#8EE7FF',
 '#FFB1FF',
 '#197EB2',
 '#F861C3',
 '#6F1C67',
 '#8E8C99',
 '#35297F',
 '#2D1BB5',
 '#B61CA7',
 '#F40EA4',
 '#35297F',
 '#2D1BB5',
 '#80447B',
 '#F40EA4',
 '#8EE7FF',
 '#FFB1FF',
 '#197EB2',
 '#F861C3',
 '#6F1C67',
 '#8E8C99',
 '#35297F',
 '#2D1BB5',
 '#B61CA7',
 '#F40EA4'




]

def bq_query_to_df(json_path, table_id, date=None):
    """
    Return the pathing table as a dataframe. Return all paths and steps. Any filtering is done in notebook.
    Provices an optional date field. If used, expects a field name "date" where weekly, monthly, quarterly
    new journeys are appended to table.
    Parameters:
        json_path (str): Path to json cred.
        table_id (str): Full table ID for query in format project.dataset.table
        date (str): Optional date in format YYYY-mm-dd. Use when continuiously appending
                    to table. Assumes field name is "date"
    Rerurns:
        pd.DataFrame: Raw Journey data frame for analysis an pathing in study
    """

    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = json_path

    if date:
        if not re.search(r'^\d{4}-\d{2}-\d{2}$', date):
            raise ValueError(f'Please use date format YYYY-mm-dd. Date format found does not match expected format: {date}')
            return
        else:
            query = f"""
                SELECT * FROM `{table_id}`
                WHERE date = {date}
            """
    else:

        query = f"""
            SELECT * FROM `{table_id}`
        """

    client = bigquery.Client()

    return client.query(query).to_dataframe()


def add_journey_end_string(df, journey_col='journey', conversion_column='Conversion', sep_val = ' > '):
    def append_to_string(s):
        if not s.strip().endswith(conversion_column):
            return s + sep_val + 'Journey End'
        else:
            return s

    df[journey_col] = df.apply(lambda x: append_to_string(x[journey_col]), axis=1)
    return df


def parse_steps(df, sep_val=' > ', max_steps=10):
    journey_list = list(df['journey'])

    def split_step(all_steps):
        steps_dict = {}
        for i, step in (enumerate(all_steps.split(sep_val), start=1)):
            if i <= max_steps:
                steps_dict[f'step_{i}'] = step
        return steps_dict

    mapped_steps = map(split_step, journey_list)
    steps_df = pd.DataFrame.from_records(list(mapped_steps))

    return pd.concat([df, steps_df], axis=1)


def remove_long_tail_jouneys(df, count_col, min_total_percentage=.9, min_path_count=4):

    """
    Parameters:
            df (pd.DataFrame): The input dataframe grouped by journey with n users per journey
            count_col (str): Name of column with count of users
            min_total_percentage (float): Minimum percentag of data to keep in modeling dataset
            min_path_count (int): Minimum number of user per journey
    Returns:
            pd.DataFrame: Input dataframe filtered to include only paths with rule-based level of traffic
    """
    total_users = df[count_col].sum()

    df.sort_values(by=count_col, ascending=False, inplace=True)
    df.reset_index(drop=True, inplace=True)
    df['count_percent'] = df[count_col] / total_users
    df['count_percent_cum'] = df['count_percent'].cumsum()

    df_out = df[df[count_col] >= min_path_count]
    if df_out['count_percent_cum'].max() >= min_total_percentage:
        print(f'Modeling DF contains {df_out["count_percent_cum"].max()*100:.0f}% of original data')
        return df_out
    else:
        suggested_path_min = max(df[df['count_percent_cum'] >= min_total_percentage][count_col])
        print(f'To retain {min_total_percentage*100:.0f}% of original data, min_path_count must be {suggested_path_min}')
        print(f'If you have removed too many users from the analysis, consider broader content groups or fewer max steps.')


def get_step_cols(df, prefix='step_'):

    """
        Ensure All the correct linear steps appear in the df. Checks the columns names
        against a generated list of a prefedined prefix with a linear integers:
        step_1, step_2, ..., step_n
        Parameters:
            df (pd.DataFrame): The input dataframe with linear steps.
            prefix (str): prefix on all linear columns. Defaults to 'step_'
        Rerurns:
            (list): Descibes if columns match expected list
    """

    # Get all steps cols in df
    data_step_cols = [col_name for col_name in list(df.columns) if prefix in col_name]
    data_step_ints = [int(col_name.replace(prefix, '')) for col_name in data_step_cols]

    # Generate list of expected steps
    expected_step_list = [f'{prefix}{expected_step}' for expected_step in list(range(1, max(data_step_ints)+1))]

    if data_step_cols != expected_step_list:
        raise ValueError(f'Error parsing step columns.\nExpected:{expected_step_list}\nFound:{data_step_cols}')
        return
    else:
        return data_step_cols


def hex_to_rgba(hex_string, opacity):
    """ Convert hex codes - #FFB1FF - to Plotly RGG with opacity - rgb(12,21,54,.6)"""
    hex_string = hex_string.lstrip('#')
    rgb_tup = tuple(int(hex_string[i:i+2], 16) for i in (0, 2, 4)) + (opacity,)

    return f'rgba{str(rgb_tup)}'


def generate_sankey_inputs(df, count_field, node_opacity=.9):

    """
        Generate the inputs to the Sankey diagram as well as a label index diagram.
    """
    # Set up both output Dataframes
    sankey_df = pd.DataFrame(columns = [count_field, 'source', 'target'])
    df_index_keys = pd.DataFrame(columns = ['value', count_field])

    # Get a list of all step columns
    data_step_cols = get_step_cols(df)

    # Loop thorough to union values into melted deduped output df formats
    for i, val in enumerate(data_step_cols):

        # Loop through source target pairs to aggregate all relationship totals
        if i < len(data_step_cols)-1:
            melt_cols = [count_field, data_step_cols[i], data_step_cols[i+1]]
            melt_df = df[melt_cols].copy()
            melt_df.columns = [count_field, 'source', 'target']
            sankey_df = pd.concat([sankey_df, melt_df[(melt_df.target.notnull()) & (melt_df.source.notnull())]])


        # Simultaiously loop through all steps to get all unique values.
        key_step_df = df[[val, count_field]].copy()
        key_step_df.columns = ['value', count_field]
        df_index_keys = pd.concat([df_index_keys, key_step_df[key_step_df.value.notnull()]])


    # Groups the index df
    df_index_keys = df_index_keys.groupby('value')\
        .sum()\
        .reset_index()\
        .rename_axis(None, axis=1)\
        .sort_values(count_field, ascending=False)\
        .reset_index(drop=True)


    # Generate index list used to match names to positions in sakey
    s_index = list(range(0, len(df_index_keys)))
    df_index_keys['value_index'] = s_index

    # generate color list based on a list of hex values - Join to index DF
    node_colors_rgba = [hex_to_rgba(c, node_opacity) for c in all_colors]
    df_index_keys['color'] = node_colors_rgba[0:len(df_index_keys)]

    # Group/sum repeated rows in sankey df for conscise output
    sankey_df = sankey_df.groupby(['source', 'target']).agg({count_field: 'sum'}).reset_index()

    # Join SOURCE node index IDs and color value
    sankey_df = pd.merge(sankey_df,
                         df_index_keys[['value', 'value_index', 'color']],
                         how='left',
                         left_on='source',
                         right_on='value').drop('value', 1)
    sankey_df.rename(columns = {'value_index':'source_index'}, inplace = True)

    # Join TARGET node index IDs and color value
    sankey_df = pd.merge(sankey_df,
                         df_index_keys[['value', 'value_index']],
                         how='left',
                         left_on='target',
                         right_on='value').drop('value', 1)
    sankey_df.rename(columns = {'value_index':'target_index'}, inplace = True)

    # return BOTH dfs for use in building sankey
    return sankey_df, df_index_keys


def display_sankey(df_sankey, df_index, count_field,node_opacity=.9, link_opacity=.3, write_html=False, html_file_name=False):


    # Change opacity in links
    link_colors = [c.replace(str(node_opacity), str(link_opacity)) for c in list(df_sankey['color'])]

    fig = go.Figure(go.Sankey(
        arrangement = "freeform",
        valueformat = ".0f",
        node = {
            "label": list(df_index['value']),
            "thickness": 20,
            'line': dict(color = "gray", width = 0),
            "pad": 30,
            'color': list(df_index['color'])
        },
        link = {
            "source": list(df_sankey['source_index']),
            "target": list(df_sankey['target_index']),
            "value":  list(df_sankey[count_field]),
            'color': link_colors,
        }
    ))

    fig.show()