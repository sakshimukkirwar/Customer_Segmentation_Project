"""
Copyright (c) 2022 Snowflake Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""
from typing import Iterable

import pandas as pd
import plotly.express as px
import streamlit as st
from snowflake.snowpark.session import Session
from snowflake.snowpark.table import Table
from toolz.itertoolz import pluck

from lib.chart_helpers import mk_labels
from lib.filterwidget import MyFilter

account_url = "https://fx34478.us-central1.gcp.snowflakecomputing.com"
organization = "ESODLJG"
account = "RU55705"
email = "data228.project@gmail.com"

connection_parameters = {
    "account": "ESODLJG-RU55705",
    "user": "DATA228PROJECT",
    "password": "Project228",
    "database": "data_228_project",  # optional
    "schema": "yelp",  # optional
}


MY_TABLE = "USERS"


def _get_active_filters() -> filter:
    return filter(lambda _: _.is_enabled, st.session_state.filters)


def _is_any_filter_enabled() -> bool:
    return any(pluck("is_enabled", st.session_state.filters))
#This function checks if any filter is enabled by using the any function along with the pluck function to extract the "is_enabled" attribute from each filter in the Streamlit session state

def _get_human_filter_names(_iter: Iterable) -> Iterable:
    return pluck("human_name", _iter)


# Initialize connection.
def init_connection() -> Session:
    return Session.builder.configs(connection_parameters).create()


@st.cache_data
def convert_df(df: pd.DataFrame):
    # IMPORTANT: Cache the conversion to prevent computation on every rerun
    return df.to_csv().encode("utf-8")


def draw_sidebar():
    """Should include dynamically generated filters"""

    with st.sidebar:
        st.sidebar.title("Features for Segmentation")
        selected_filters = st.multiselect(
            "Select which filters to enable",
            list(_get_human_filter_names(st.session_state.filters)),
            []#list(_get_human_filter_names(st.session_state.filters)),
        )
        for _f in st.session_state.filters:
            if _f.human_name in selected_filters:
                _f.enable()

        if _is_any_filter_enabled():
            with st.form(key="input_form"):

                for _f in _get_active_filters():
                    _f.create_widget()
                st.session_state.clicked = st.form_submit_button(label="Submit")
        else:
            st.write("Please enable a filter")


def draw_table_data(table_sequence):
    st.header("Raw Dataset")
    print("table_sequence = ", type(table_sequence[-1]), table_sequence[-1].dtypes)
    st.write(table_sequence[-1].sample(n=5).to_pandas().head())


def draw_map(table_sequence):
    # Create a map of the world
    st.header("Geographical Distribution of the data.")

    df = table_sequence[-1].to_pandas()
    print(df.head())
    print(df.columns)
    mp = df[["LATITUDE", "LONGITUDE"]].dropna()
    print(mp.head())
    st.map(mp)


def get_popular_words(table_sequence):
    import json
    from wordcloud import WordCloud

    st.header("Popular Categories where People Go")
    df = table_sequence[-1].to_pandas()
    df['FREQUENT_WORDS_MAP'] = df['FREQUENT_WORDS_MAP'].apply(json.loads)
    # print(df['CATEGORY_MAP'].tolist())
    result_df = pd.json_normalize(df['FREQUENT_WORDS_MAP']).melt(var_name='Words', value_name='Count').dropna() \
        .groupby('Words')['Count'].sum().reset_index().nlargest(20, 'Count')
    wordcloud_data = dict(zip(result_df['Words'], result_df['Count']))
    wordcloud = WordCloud(width=800, height=400).generate_from_frequencies(wordcloud_data)

    # Display the word cloud using Streamlit
    st.image(wordcloud.to_array(), caption="Word Cloud")


def get_popular_categories(table_sequence):
    import json
    from wordcloud import WordCloud

    st.header("Popular Categories where People Go")
    df = table_sequence[-1].to_pandas()
    df['CATEGORY_MAP'] = df['CATEGORY_MAP'].apply(json.loads)
    # print(df['CATEGORY_MAP'].tolist())
    result_df = pd.json_normalize(df['CATEGORY_MAP']).melt(var_name='Categories', value_name='Review_Count').dropna() \
        .groupby('Categories')['Review_Count'].sum().reset_index().nlargest(20, 'Review_Count')
    wordcloud_data = dict(zip(result_df['Categories'], result_df['Review_Count']))
    wordcloud = WordCloud(width=800, height=400).generate_from_frequencies(wordcloud_data)

    # Display the word cloud using Streamlit
    st.image(wordcloud.to_array(), caption="Word Cloud")


def get_elite_customers(table_sequence):
    st.header("Number of Elite Customers per Year Distribution")
    df = table_sequence[-1].to_pandas()
    df = df['ELITE'].str.split(",").explode().str.extract('(\d+)') \
        .value_counts().rename_axis('Years').reset_index(name='Number of Elite Users')
    df = df[df['Years'] != '20']
    from matplotlib.pyplot import hist
    st.markdown(hist(df['Years'], weights=df['Number of Elite Users']))
    st.bar_chart(df, x="Years", y="Number of Elite Users")


def emotions_distribution(table_sequence):
    import json
    st.header("Sentiment Analysis for the reviews")
    df = table_sequence[-1].to_pandas()
    df['SENTIMENT_MAP'] = df['SENTIMENT_MAP'].apply(json.loads)
    result_df = pd.json_normalize(df['SENTIMENT_MAP']).melt(var_name='Emotion', value_name='Value_Count')
    print(result_df.head())
    fig = px.pie(result_df, values='Value_Count', names='Emotion', title=f'Pie Chart for Emotion of the review')
    # Display the pie chart using Streamlit
    st.plotly_chart(fig)
    print(result_df.head())


def draw_table_query_sequence(table_sequence: list):
    # Add the SQL statement sequence table
    statement_sequence = """
| number | filter name | query, transformation |
| ------ | ----------- | --------------------- |"""
    st.header("Statement sequence")
    statments = []
    for number, (_label, _table) in enumerate(
            zip(mk_labels(_get_human_filter_names(_get_active_filters())), table_sequence)):
        statments.append(f"""\n| {number+1} | {_label} | ```{_table.queries['queries'][0]}``` |""")

    statement_sequence += "".join(statments[::-1])

    st.markdown(statement_sequence)

    # Materialize the result <=> the button was clicked
    try:
        if st.session_state.clicked:
            with st.spinner("Converting results..."):
                st.download_button(
                    label="Download as CSV",
                    data=convert_df(table_sequence[-1].to_pandas()),
                    file_name="customers.csv",
                    mime="text/csv",
                )
    except Exception as e:
        print(e)


def draw_main_ui(_session: Session):
    """Contains the logic and the presentation of main section of UI"""
    customers: Table = _session.table(MY_TABLE)
    table_sequence = [customers]

    if _is_any_filter_enabled():
        # _f: MyFilter
        for _f in _get_active_filters():
            last_table = table_sequence[-1]

            new_table = last_table[ _f(last_table) ]
            table_sequence += [new_table]
    else:
        st.write("Please enable a filter in the sidebar to show transformations")

    draw_table_data(table_sequence)
    draw_map(table_sequence)
    get_popular_categories(table_sequence)
    emotions_distribution(table_sequence)
    # get_popular_words(table_sequence)
    get_elite_customers(table_sequence)
    draw_table_query_sequence(table_sequence)


if __name__ == "__main__":
    # Initialize the filters
    st.header("Customer Segmentation With Yelp Review Dataset")
    session = init_connection()
    MyFilter.session = session
    MyFilter.table_name = MY_TABLE

    st.session_state.filters = (
        MyFilter(
            human_name="Number of Friends",
            table_column="FRIENDS_COUNT",
            widget_id="FRIENDS_COUNT_slider",
            widget_type=st.select_slider,
        ),
        MyFilter(
            human_name="Average Stars Given",
            table_column="AVERAGE_STARS",
            widget_id="AVERAGE_STARS_slider",
            widget_type=st.select_slider,
        ),
        MyFilter(
            human_name="Number of Review Given",
            table_column="Review_COUNT",
            widget_id="Review_Count_slider",
            widget_type=st.select_slider,
        ),
        MyFilter(
            human_name="Active on Yelp Days",
            table_column="DATE_DIFF",
            widget_id="Active On Yelp Days",
            widget_type=st.select_slider,
        ),
        MyFilter(
            human_name="People Find Useful",
            table_column="USEFUL",
            widget_id="People Find Useful",
            widget_type=st.select_slider,
        )
    )

    draw_sidebar()
    draw_main_ui(session)
#%%
