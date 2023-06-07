```python
import os
from datetime import date

import requests

HOME = os.path.expanduser('~')
DATALAKE_ROOT_FOLDER = HOME + "/datalake/"


def fetch_data_from_imdb1(**kwargs):
   current_day = date.today().strftime("%Y%m%d")
   TARGET_PATH = DATALAKE_ROOT_FOLDER + "raw/imdb/MovieRating/" + current_day + "/"
   if not os.path.exists(TARGET_PATH):
       os.makedirs(TARGET_PATH)

   url = 'https://datasets.imdbws.com/title.ratings.tsv.gz'
   r = requests.get(url, allow_redirects=True)
   open(TARGET_PATH + 'title.ratings.tsv.gz', 'wb').write(r.content)

```


```python
fetch_data_from_imdb1()
```


```python
HOME = os.path.expanduser('~')
DATALAKE_ROOT_FOLDER = HOME + "/datalake/"

def fetch_data_from_imdb2(url, data_entity_name, **kwargs):
    current_day = date.today().strftime("%Y%m%d")
    TARGET_PATH = DATALAKE_ROOT_FOLDER + "raw/imdb/" + data_entity_name + "/" + current_day + "/"
    if not os.path.exists(TARGET_PATH):
        os.makedirs(TARGET_PATH)
        
    file_name = url.split("/")[-1]
    file_path = os.path.join(TARGET_PATH, file_name)
    
    r = requests.get(url, allow_redirects=True)
    open(file_path, 'wb').write(r.content)

```


```python
fetch_data_from_imdb2('https://datasets.imdbws.com/title.ratings.tsv.gz', 'MovieRating')
fetch_data_from_imdb2('https://datasets.imdbws.com/name.basics.tsv.gz', 'MovieName')
```


```python

import pandas as pd

HOME = os.path.expanduser('~')
DATALAKE_ROOT_FOLDER = HOME + "/datalake/"


def convert_raw_to_formatted(file_name, current_day):
   RATING_PATH = DATALAKE_ROOT_FOLDER + "raw/imdb/MovieRating/" + current_day + "/" + file_name
   FORMATTED_RATING_FOLDER = DATALAKE_ROOT_FOLDER + "formatted/imdb/MovieRating/" + current_day + "/"
   if not os.path.exists(FORMATTED_RATING_FOLDER):
       os.makedirs(FORMATTED_RATING_FOLDER)
   df = pd.read_csv(RATING_PATH, sep='\t')
   parquet_file_name = file_name.replace(".tsv.gz", ".snappy.parquet")
   df.to_parquet(FORMATTED_RATING_FOLDER + parquet_file_name)

```


```python
# Specify the file name and current day
file_name = "title.ratings.tsv.gz"
current_day = "20230606"

# Call the function
convert_raw_to_formatted(file_name, current_day)

```


```python
import os
from pyspark.sql import SQLContext

HOME = os.path.expanduser('~')
DATALAKE_ROOT_FOLDER = HOME + "/datalake/"


def combine_data(current_day):
   RATING_PATH = DATALAKE_ROOT_FOLDER + "formatted/imdb/MovieRating/" + current_day + "/"
   USAGE_OUTPUT_FOLDER_STATS = DATALAKE_ROOT_FOLDER + "usage/movieAnalysis/MovieStatistics/" + current_day + "/"
   USAGE_OUTPUT_FOLDER_BEST = DATALAKE_ROOT_FOLDER + "usage/movieAnalysis/MovieTop10/" + current_day + "/"
   if not os.path.exists(USAGE_OUTPUT_FOLDER_STATS):
       os.makedirs(USAGE_OUTPUT_FOLDER_STATS)
   if not os.path.exists(USAGE_OUTPUT_FOLDER_BEST):
       os.makedirs(USAGE_OUTPUT_FOLDER_BEST)

   from pyspark import SparkContext

   sc = SparkContext(appName="CombineData")
   sqlContext = SQLContext(sc)
   df_ratings = sqlContext.read.parquet(RATING_PATH)
   df_ratings.registerTempTable("ratings")

   # Check content of the DataFrame df_ratings:
   print(df_ratings.show())

   stats_df = sqlContext.sql("SELECT AVG(averageRating) AS avg_rating,"
                             "       MAX(averageRating) AS max_rating,"
                             "       MIN(averageRating) AS min_rating,"
                             "       COUNT(averageRating) AS count_rating"
                             "    FROM ratings LIMIT 10")
   top10_df = sqlContext.sql("SELECT tconst, averageRating"
                             "    FROM ratings"
                             "    WHERE numVotes > 50000 "
                             "    ORDER BY averageRating DESC"
                             "    LIMIT 10")

   # Check content of the DataFrame stats_df and save it:
   print(stats_df.show())
   stats_df.write.save(USAGE_OUTPUT_FOLDER_STATS + "res.snappy.parquet", mode="overwrite")

   # Check content of the DataFrame top10_df  and save it:
   print(top10_df.show())
   stats_df.write.save(USAGE_OUTPUT_FOLDER_BEST + "res.snappy.parquet", mode="overwrite")

```


```python
# Specify the current day
current_day = "20230606"

# Call the function
combine_data(current_day)
```


    ---------------------------------------------------------------------------

    ValueError                                Traceback (most recent call last)

    Cell In[25], line 5
          2 current_day = "20230606"
          4 # Call the function
    ----> 5 combine_data(current_day)
    

    Cell In[22], line 19, in combine_data(current_day)
         15     os.makedirs(USAGE_OUTPUT_FOLDER_BEST)
         17 from pyspark import SparkContext
    ---> 19 sc = SparkContext(appName="CombineData")
         20 sqlContext = SQLContext(sc)
         21 df_ratings = sqlContext.read.parquet(RATING_PATH)
    

    File D:\software\Conda\lib\site-packages\pyspark\context.py:198, in SparkContext.__init__(self, master, appName, sparkHome, pyFiles, environment, batchSize, serializer, conf, gateway, jsc, profiler_cls, udf_profiler_cls, memory_profiler_cls)
        192 if gateway is not None and gateway.gateway_parameters.auth_token is None:
        193     raise ValueError(
        194         "You are trying to pass an insecure Py4j gateway to Spark. This"
        195         " is not allowed as it is a security risk."
        196     )
    --> 198 SparkContext._ensure_initialized(self, gateway=gateway, conf=conf)
        199 try:
        200     self._do_init(
        201         master,
        202         appName,
       (...)
        212         memory_profiler_cls,
        213     )
    

    File D:\software\Conda\lib\site-packages\pyspark\context.py:445, in SparkContext._ensure_initialized(cls, instance, gateway, conf)
        442     callsite = SparkContext._active_spark_context._callsite
        444     # Raise error if there is already a running Spark context
    --> 445     raise ValueError(
        446         "Cannot run multiple SparkContexts at once; "
        447         "existing SparkContext(app=%s, master=%s)"
        448         " created by %s at %s:%s "
        449         % (
        450             currentAppName,
        451             currentMaster,
        452             callsite.function,
        453             callsite.file,
        454             callsite.linenum,
        455         )
        456     )
        457 else:
        458     SparkContext._active_spark_context = instance
    

    ValueError: Cannot run multiple SparkContexts at once; existing SparkContext(app=CombineData, master=local[*]) created by __init__ at C:\Users\qcxl9\AppData\Local\Temp\ipykernel_44300\3611609488.py:19 



```python

```


```python

```
