<!-- Add banner here -->

# Project Title

ETL Pipeline for Trending Tokens in each country using Twitter's 'recent search' API

# Demo-Preview

[GIF](images/dashboard_screenshots.gif)

# Table of contents

- [Project Title](#project-title)
- [Demo-Preview](#demo-preview)
- [Table of contents](#table-of-contents)
- [Repository Structure](#repository_structure)
- [Implementation](#implementation)


# Repository Structure
[(Back to top)](#table-of-contents)
- dashboard_data
    - country_tokens.csv (final data for the dashboard)
- database_lib
    - mongoDB_lib.py (functions for MongoDB operations)
    - postgre_lib.py (functions for PostGRE on AWS RDS operations)
- processing_lib
    - util.py (general utility functions used across files)
    - twitter_lib.py (functions for handling Twitter API calls)
    - spark_lib.py (functions for data transformation using PySpark)
    - dashboard_data_lib.py (functions for supporting creation of final dataframe for the dashboard)
- InitializationScript.py (Day 0 script for database initialization)
- ExtractLoadTweets.py (Twitter API -> PySpark -> PostGre)
- TransformLoadTOkens.py (PostGre -> PySpark -> MongoDB)
- GenerateDashboardData.py (MongoDB -> PySpark -> CSV)
- dataPipeline.bat (To run the ETL pipeline)
- GeoDashboard.ipynb (The final dashboard notebook)


# Implementation
[(Back to top)](#table-of-contents)

The data pipeline is implemented using Python and makes use of PostGreSQL on AWS RDS and MongoDB databases for storing raw and processed data respectively. Credentials for the API and databases are stored in the .env file.

Steps in the pipeline:
1. Database Initialization (Only on Day 0):
- The initialization of the pipeline involves creating the required databases, tables and inserting static records into the database that can be accessed everyday by the pipeline (for eg. latitude and longitude data for countries)

2. Extract and Load: 
- This step makes numerous calls to Twitter's recent search API using an expression specifying the country name and verified account flag, and receives recent tweets for that country, posted by verified accounts.
- The received response data is transformed into a tuple format with the required attributes, compatible for upsert into the relational database, using PySpark
- Finally, the data obtained in tuple format is upserted into the 'tweets' table on PostGre

3. Transform and Load:
- This step extracts tweet data from the relational PostGre database and performs transformations using PySpark. The transformation steps include tokenization of tweets or each country, stopword and punctuation removal, word count computation, filtering words based on number of occurences, etc. using a series of map and shuffle operations in PySpark.
- After obtaining the processed tokens per country per date, this data is loaded into a mongoDb collection

4. Extract data for dashboard:
- Final step in the pipeline involves extracting the required data from the mongoDB collection (latest tokens per country) and PostGRE database (latitude and longitude values for countries), and using PySpark to transform data in a way to construct a dataframe for the dashboard
- The dataframe is then coverted to a CSV file which is used by the dashboard to display trending tokens per country