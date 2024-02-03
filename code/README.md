# Customer Segmentation using Yelp Review Data


To use tableu dashboard - please use below Snowflake credentials. "password = Project228"

Streamlit app url, its connected to the same database. - https://yelp-customer-segmentation.streamlit.app/ 


### Package Dependency
``` shell
pip install sqlalchemy
pip install ipynb
pip install snowflake-connector-python
pip install snowflake-sqlalchemy
pip install tqdm
pip install nltk
pip install wordCloud

pip install streamlit
pip install watchdog
pip install yfinance

# for this we need python 3.8+
pip install snowflake-snowpark


```


### Snowflake connection
```
account_url = "https://fx34478.us-central1.gcp.snowflakecomputing.com"
organization = "ESODLJG"
account = "RU55705"
email = "data228.project@gmail.com"

snowflake_options = {
    "sfURL": "https://fx34478.us-central1.gcp.snowflakecomputing.com",
    "sfUser": "DATA228PROJECT",
    "sfPassword": "Project228",
    "sfWarehouse": "COMPUTE_WH",
    "sfDatabase": "data_228_project",
    "sfSchema": "yelp",
    "sfTable": "test",
    "dbtable": "test"
}
```

### StreamLit App

https://yelp-customer-segmentation.streamlit.app/
