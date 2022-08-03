<!-- Add banner here -->

# Project Title

ETL Pipeline for Trending Tokens in each country using Twitter's 'recent search' API

# Demo-Preview

![Dashboard Screenshot] (images/screenshot) -->

# Table of contents

- [Project Title](#project-title)
- [Demo-Preview](#demo-preview)
- [Table of contents](#table-of-contents)
- [Repository Structure](#repository_structure)
- [Dependencies](#dependencies)
- [Implementation](#implementation)

# Dependencies
[(Back to top)](#table-of-contents)

The project requires the following libraries to be installed. Commands are as follows:

pip install loadenv
pip install python-dotenv
pip install psycopg2
pip install nltk
pip install pandas
pip install py4j==0.10.9.3
pip install findspark
pip install requests
pip install pymongo
pip install psutil
pip install dnspython

conda install -c pyviz geoviews=1.9.4
conda install bokeh


# Repository Structure
[(Back to top)](#table-of-contents)


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