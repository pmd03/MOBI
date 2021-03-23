# -*- coding: utf-8 -*-
"""
Created on Wed Dec  9 19:33:34 2020

@author: P. Douglas

Using a Schema is a practical way to validate source data and capture via exceptions, any data that may need cleaned or
is potentially problematic for example at row level data value with £ sign or missing keys in a json file.
"""
__author__ = "P. Douglas"
__copyright__ = "Copyright (C) 2021 P. Douglas"
__license__ = "Public Domain"
__version__ = "0.1"

#dates_list = [dt.datetime.strptime(date, '"%Y-%m-%d"').date() for date in dates]

import logging
from time import time

from datetime import timedelta, date, datetime

import os, sys, fnmatch, json ,traceback
ver = sys.version.split()[0]

def pythonVer(v):
    return tuple(map(int, (v.split("."))))

if pythonVer(ver) < pythonVer('3.6') :
    raise Exception("Python 3.6 or a more recent version is expected.")

class T():
    def __enter__(self):
        self.start = time()
    def __exit__(self, type, value, traceback):
        self.end = time()
        elapsed = self.end - self.start
        print(str(timedelta(seconds=elapsed)))

def pythonVer(v):
    return tuple(map(int, (v.split("."))))

def dirList(path=None):
    # hardcoded feed names
    feedNames = ('fx_rates.csv','closing_prices.xml', 'instruments.json')
    if path == None:
        path = os.getcwd()
        return [fname for fname in os.listdir(path)
                if os.path.isfile(fname)
                if [f for f in feedNames if fnmatch.fnmatch(fname, f)]
                ]

def sum_scores(scores):
  sums = {}
  for score in scores:
    sums.setdefault(score[0], 0)
    sums[score[0]] += score[1]
  return sums

import re
def sort_bskt(undlyng_isins):
    # 'GB00B019KW72,JE00B4T3BW64,GB0007980591,ES0177542018,GB00B1XZS820,GB00BDR05C01,GB00BH4HKS39'
    pd_str_sorted = re.split(',|, ', undlyng_isins)
    pd_str_sorted.sort()
    return ','.join(pd_str_sorted)


from pyspark.sql import Window
from pyspark.shell import spark
from pyspark.sql import SparkSession
from pyspark.sql.types import *


""" 
__all__ = [
    "DataType", "NullType", "StringType", "BinaryType", "BooleanType", "DateType",
    "TimestampType", "DecimalType", "DoubleType", "FloatType", "ByteType", "IntegerType",
    "LongType", "ShortType", "ArrayType", "MapType", "StructField", "StructType"]

"""

# Create a schema for the dataframe originally xml source
schemaClosingPrice = StructType([
        StructField('fininstrmgnlattrbts_id', StringType(), False),
        StructField('trade_date', DateType(), False),
        StructField('ccy', StringType(), False),
        StructField('eod_price', DecimalType(scale=4), False),
        StructField('day_high_price', DecimalType(scale=4), False),
        StructField('day_low_price', DecimalType(scale=4), False)
    ])

# Create a schema for the dataframe originally csv source
schemaFXrate = StructType([
        StructField('fx_date', DateType(), False),
        StructField('from_ccy', StringType(), False),
        StructField('to_ccy', StringType(), False),
        StructField('fx_rate', DecimalType(scale=3), False)
    ])

# Create a schema for the dataframe originally json source
schemaInstrument = StructType([
        StructField('instrm_id', StringType(), False),
        StructField('industry_nm', StringType(), False),
        StructField('type', StringType(), False),
        StructField('instrm_type', StringType(), False),
        StructField('instrm_fullnm', StringType(), False),
        StructField('instrm_shrtnm', StringType(), False),
        StructField('instrm_clssfctntp', StringType(), False),
        StructField('instrm_ntnlccy', StringType(), False),
        StructField('instrm_cmmdtyderivind', StringType(), False),
        StructField('issr', StringType(), False),
        StructField('trad_id', StringType(), False),
        StructField('trad_issrreq', StringType(), False),
        StructField('trad_frsttraddt', TimestampType(), False),
        StructField('trad_termntndt', TimestampType(), False),
        StructField('tech_rlvntcmptntauthrty', StringType(), False),
        StructField('undlyng_type', StringType(), False),
        StructField('undlyng_subtype', StringType(), False),
        StructField('undlyng_isins', StringType(), False)
        ])

"""  ['fininstrmgnlattrbts_id', 'industry_nm', 'type', 'fininstrm_type',
         'fininstrmgnlattrbts_fullnm', 'fininstrmgnlattrbts_shrtnm',
         'fininstrmgnlattrbts_clssfctntp', 'fininstrmgnlattrbts_ntnlccy',
         'fininstrmgnlattrbts_cmmdtyderivind', 'issr', 'tradgvnrltdattrbts_id',
         'tradgvnrltdattrbts_issrreq', 'tradgvnrltdattrbts_frsttraddt',
         'tradgvnrltdattrbts_termntndt', 'techattrbts_rlvntcmptntauthrty',
         'undlyng_type', 'undlyng_subtype', 'undlyng_isins']
"""

# Start a pyspark session
print(f'pySpark Session context: {spark.sparkContext._conf.getAll()}')
#spark.sparkContext._conf.getAll()

# capture the config context and modify as require for the environment to run with
conf = spark.sparkContext._conf.setAll([('spark.master', 'local[*]'), \
        ('spark.app.name', 'MOBI_ETL'), \
        ('spark.sql.shuffle.partitions', 600)] \
        #("spark.sql.execution.arrow.pyspark.enabled", "true") \
        )
# stop the context we have setup the con fig we need to run with now
spark.sparkContext.stop()

def json_validator(data):
    try:
        json.loads(data)
        return True
    except ValueError as error:
        print("invalid json: %s" % error)
        return False
    
def fca_ETL():
    # feeds ['users.json', 'venues.json']
    # assume script and feeds in current folder also output in current folder
    fcaFeeds = sorted(dirList())
    
    #if len(fcaFeeds) < 3:
        #raise Exception("Three data feeds required: 'fx_rates.csv','closing_prices.xml', 'instruments.json'")

    # FXrate python read small csv file, not using spark.read
    try:
        import csv
        from decimal import Decimal
        x = 0
        with open(fcaFeeds[1], newline='') as csvfile:
            print(fcaFeeds[1])
            # read the FXrate csv data into a dict which is easy to manipulate via key/values referencing.
            readerDict = csv.DictReader(csvfile, fieldnames=("fx_date", "fromTo", "fx_rate"))
            print(readerDict)
            # break out the from and to currency columns
            rateList = []
            for row in readerDict:
                newDict = {}
                fromTo = row['fromTo'].split('/', 1)
                #print(row['fx_date'], row['fromTo'].split('/', 1), row['rate'])
                newDict = {'fx_date': datetime.strptime(row['fx_date'], '%Y-%m-%d').date(), 'from_ccy': fromTo[0], 'to_ccy': fromTo[1], 'fx_rate': Decimal(row['fx_rate'])}

                rateList.append(newDict)

                """
                    x += 1
                    print(x)
                    if x > 10:
                        print(f'rateList:{rateList}')
                        break
                """
    except Exception as error:
        traceback.print_exc()
        print('###### An exception occurred ######')
    else:
        print(f'\nETL {fcaFeeds[1]} present')

    # instruments json python read json small file, not using spark.read

    userData = []
    print("Started Reading JSON file which contains multiple JSON document objects and blank header (not an array)")
    try:
        with open(fcaFeeds[2]) as f:
            next(f) # blank line not a header
            for jsonObj in f:
                userData_dict = json.loads(jsonObj)
                userData.append(userData_dict)
            #print('userData list:', userData)

            x = 0
            instrumentList = []
            for row in userData:
                newDictInstr = {}
                #print('userData row:', row['tradgvnrltdattrbts_termntndt'])
                try:
                    newDictInstr = {
                                'instrm_id': row['fininstrmgnlattrbts_id'],
                                'industry_nm': row['industry_nm'],
                                'type': row['type'],
                                'instrm_type': row['fininstrm_type'],
                                'instrm_fullnm': row['fininstrmgnlattrbts_fullnm'],
                                'instrm_shrtnm': row['fininstrmgnlattrbts_shrtnm'],
                                'instrm_clssfctntp': row['fininstrmgnlattrbts_clssfctntp'],
                                'instrm_ntnlccy': row['fininstrmgnlattrbts_ntnlccy'],
                                'instrm_cmmdtyderivind': row['fininstrmgnlattrbts_cmmdtyderivind'],
                                'issr': row['issr'],
                                'trad_id': row['tradgvnrltdattrbts_id'],
                                'trad_issrreq': row['tradgvnrltdattrbts_issrreq'],
                                'trad_frsttraddt': datetime.strptime(row['tradgvnrltdattrbts_frsttraddt'], '%Y-%m-%dT%H:%M:%S.%f%z'),
                                'trad_termntndt': datetime.strptime(row['tradgvnrltdattrbts_termntndt'], '%Y-%m-%dT%H:%M:%S.%f%z'),
                                'tech_rlvntcmptntauthrty': row['techattrbts_rlvntcmptntauthrty'],
                                'undlyng_type': row['undlyng_type'],
                                'undlyng_subtype': row['undlyng_subtype'],
                                # sorted string undlyng_isins could be used as a unique id for determining dim_instrumrnt_attr.basket_id
                                'undlyng_isins': sort_bskt(row['undlyng_isins'])
                                }
                    instrumentList.append(newDictInstr)
                    if newDictInstr['undlyng_type'] == 'Bskt':
                        print(f'newDictInstr:{newDictInstr}')
                    #x += 1
                    #print(x)
                    # "2018-02-02T16:31:59.000Z"
                except KeyError:
                    logging.info('tradgvnrltdattrbts_termntndt NOT FOUND')

                #instrumentList.append(newDictInstr)
                #print(f'instrumentList:{instrumentList}')

    except Exception as error:
            traceback.print_exc()
            print('###### An exception occurred during instrument json schema validation ######')
    else:
        print(f'\nETL {fcaFeeds[0]} present')
    finally:
        print('CONTINUE to spark session')
        #exit()
        #spark.stop()  # stop context

    # spark session begins and processing using spark dataframes
    import pyspark.sql.functions as f

    try:
            # .format('com.databricks.spark.xml')
            # SparkSession provides immediate access to sparkContext
            # instantiate the session with bespoke configuration (there can be many sessions)
            spark = SparkSession.builder.config(conf=conf).getOrCreate()
            print(f'pySpark Session bespoke context: {spark.sparkContext._conf.getAll()}')
            # spark.sparkContext._conf.getAll()

            # There is only one context that services all the sessions.
            # Also, could have a for performance configured session, for each source dataframe processing based on
            # source data volumes.

            # spark session processing for Instrument ##############################

            # a_dict = json.dumps(a_dict, ensure_ascii=True, indent=4)
            # Create data frame for loading to postgresql ORDBMS

            dfInstrument = spark.createDataFrame(instrumentList, schemaInstrument)
            # print(dfInstrument.schema)
            dfInstrument.show()

            # RI dedupe....
            w = Window.partitionBy('instrm_id').orderBy(f.col('undlyng_isins').desc())
            df3 = dfInstrument.select('undlyng_isins', 'instrm_id', 'undlyng_type', f.row_number().over(w).alias('n')).where('n > 1')
            print('\naggregates:')
            df3.show(50)

            # without the truncate option the existing table in postgres will be dropped and created.
            dfInstrument.write.option("truncate", "true").jdbc(url=rdbm_url, table='integration.staged_instrument', \
                                    mode='overwrite', properties=rdbm_properties)

            #dfInstrument.write.jdbc(url=rdbm_url, table='integration.staged_instrument', \
                                    #mode='overwrite', properties=rdbm_properties)

            # spark session processing for RateFX #################################

            # time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time()))
            # Create data frame for loading to postgresql ORDBMS
            dfRateFX = spark.createDataFrame(rateList, schemaFXrate)
            print(f'dfRateFX:{dfRateFX.schema}')
            dfRateFX.printSchema()
            dfRateFX.show()
            dfRateFX.na.drop()

            # Query spark data frame using dataframe methods
            dfRateFX.select(f.date_format('fx_date', 'yyyy-MM-dd').alias('day')).groupby('day').count().show()
            dfRateFX.select(f.date_format('fx_date', 'yyyy-MM').alias('month'), f.col('from_ccy')).groupby('month', 'from_ccy').count().alias( \
                'count').sort(f.col('from_ccy').desc(), f.col('count').desc()).show()

            # Test quality
            expected_result_count = 3497
            #result_count = dfRateFX.distinct().count()
            result_count = dfRateFX.count()
            assert (
                    result_count == expected_result_count), f'Error count dfRateFX {result_count} NE {expected_result_count}'
            print(expected_result_count)

            # Query spark data frame using spark sql
            dfRateFX.createOrReplaceTempView('dfRateFX')

            pd_sql = spark.sql('SELECT MONTH(fx_date) AS month, SUM(fx_rate) AS sum_rate \
                                           FROM dfRateFX \
                                           GROUP BY MONTH(fx_date)' \
                               )
            pd_sql.show()

            # Save the dataframe to the postgresql database table.

            #dfRateFX.write.jdbc(url=rdbm_url, table='integration.staged_fx_rate', \
                          #mode='append', properties=rdbm_properties)

            # without the truncate option the existing table in postgres will be dropped and created.
            dfRateFX.write.option("truncate", "true").jdbc(url=rdbm_url, table='integration.staged_fx_rate', \
                          mode='overwrite', properties=rdbm_properties)

            #dfRateFX.write.jdbc(url=rdbm_url, table='integration.staged_fx_rate', \
                            #mode='overwrite', properties=rdbm_properties)

            # Read from database postgresql integration.staged_fx_rate table
            jdbcDF = spark.read.format("jdbc"). \
                options( \
                url=rdbm_url, \
                dbtable='integration.staged_fx_rate', \
                user=rdbm_properties['user'], \
                password=rdbm_properties['password'], \
                driver=rdbm_properties['driver']). \
                load()

            # will return DataFrame
            jdbcDF.show()
            print('jdbcDF schema')
            print(f'jdbcDF: {type(jdbcDF)}')
            jdbcDF.printSchema()

            # test1
            result_count = jdbcDF.distinct().count()
            assert (result_count == expected_result_count), f'Error count jdbcDF {result_count} NE {expected_result_count}'

            # NEXT process the closing prices json file
            print(fcaFeeds[2])
            # remove pound sign line 37 in xml file <eod_price>£13.0940</eod_price>
            # this can be done programmatically using xmltodict cleansing source before writing to postgresql database.
            #import xmltodict
            # Initially used samplingRatio to Infer a schema but this meant FASTFAIL exited and with DROPMALFORMED no opportunity to clean data.
            # lets keep xml data as stringType and apply validation afterwards to the Spark df using withColumn
            schemaClosingPriceStr = StructType([
                StructField('fininstrmgnlattrbts_id', StringType(), False),
                StructField('trd_dt', StringType(), False),
                StructField('ccy', StringType(), False),
                StructField('eod_price', StringType(), False),
                StructField('day_high_price', StringType(), False),
                StructField('day_low_price', StringType(), False)
            ])

            df1 = (spark.read \
                   .format('com.databricks.spark.xml') \
                   .option("rootTag", "prices") \
                   .option('rowTag', 'price') \
                   .option('attributePrefix', '_') \
                   #.option('mode', 'DROPMALFORMED') \
                   .option('mode', 'FAILFAST') \
                   .schema(schemaClosingPriceStr) \
                   #.option('samplingRatio', '0.50') \
                   #columnNameOfCorruptRecord='CORRUPTED'
                   .load(xmlPath + '\closing_prices.xml'))

            # 21/03/14 17:09:03 WARN StaxXmlParser$: Dropping malformed line: <price>        <fininstrmgnlattrbts_id>GB00B03MM408</fininstrmgnlattrbts_id>        <trd_dt>2020-12-03</trd_dt>        <eod_price>�13.0940</eod_price>        <day_high_price>13.1661</day_high_price>        <day_low_price>12.8480</day_low_price>        <ccy>GBP</ccy>    </price>. Reason: Unparseable number: "�13.0940"

            df1.printSchema()

            df1.na.drop()
            # cleanse any price which has special characters currency symbol before a number

            try:
                # regexp_replace based on any character is NOT a number.

                dfClosingPriceValidated = df1.withColumn('eod_price', f.regexp_replace('eod_price', '£', ''))

                print('dfClosingPriceValidated strings')
                print(f'dfClosingPriceValidated count:{dfClosingPriceValidated.count()}')
                dfClosingPriceValidated.show(50)
            except Exception as error:
                traceback.print_exc()
                print('###### An exception occurred during schema check of closing price source######')
                # logging.info('£ in eod_price')
                spark.stop()  # stop context
                exit()

            ta = dfClosingPriceValidated.alias('ta')
            tb = jdbcDF.alias('tb')

            #joined_df = df1.join(df2,
            #                     (df1['name'] == df2['name']) &
            #                     (df1['phone'] == df2['phone'])
            #                     )

            inner_join = ta.join(tb,
                                 ((ta.ccy == tb.from_ccy) &
                                 (ta.trd_dt == tb.fx_date)), how='left'
                                )
            print('inner_join:')
            #inner_join.filter(f.col('ta.ccy') != 'GBP').show()


            print(f'df1:{df1.count()}')
            print(f'inner_join:{inner_join.count()}')
            # Some Transformation begins to staged tables, this could also be performed during update of MOBI_SS star schema.
            # However, better in this test to validate/see results in staged before persisting in warehouse.
            # FXrate for GBP rate = 1.0

            #dataDF.withColumn("new_column",
            #                 when((col("code") == "a") | (col("code") == "d"), "A")
            #                .when((col("code") == "b") & (col("amt") == "4"), "B")
            #                .otherwise("A1")).show()

            df2 = inner_join.withColumn("fx_rate", f.when((f.col("ccy") == "GBP"), 1.0).otherwise(f.col("fx_rate"))) \
                .withColumn("fx_date", f.when((f.col("ccy") == "GBP"), f.col("trd_dt")).otherwise(f.col("fx_date"))) \
                .withColumn("from_ccy", f.when((f.col("ccy") == "GBP"), f.col("ccy")).otherwise(f.col("from_ccy"))) \
                .withColumn("to_ccy", f.when((f.col("ccy") == "GBP"), f.col("ccy")).otherwise(f.col("to_ccy")))

            # https://www.investopedia.com/articles/forex/090314/how-calculate-exchange-rate.asp

            df3 = df2.withColumn("eod_price_converted", f.col('eod_price') * (1/f.col('fx_rate'))) \
                .withColumn("day_low_price_converted", f.col('day_low_price') * (1/f.col('fx_rate'))) \
                .withColumn("day_high_price_converted", f.col('day_high_price') * (1/f.col('fx_rate'))) \
                .select('fininstrmgnlattrbts_id', 'trd_dt', 'ccy', 'eod_price', 'day_high_price', 'day_low_price', \
                'eod_price_converted', 'day_high_price_converted', 'day_low_price_converted')

            # inferred schema above over a samplingRatio, but now use an explicit schema to ensure validation of source.
            # closing price processing write to the stage schema postgresql database MOBI.integration

            try:

                dfClosingPrice = df3.withColumn('ccy', f.col('ccy').cast(StringType())) \
                    .withColumn('day_high_price', f.col('day_high_price').cast(DecimalType(scale=4))) \
                    .withColumn('day_low_price', f.col('day_low_price').cast(DecimalType(scale=4))) \
                    .withColumn('eod_price', f.col('eod_price').cast(DecimalType(scale=4))) \
                    .withColumn('day_high_price_converted', f.col('day_high_price_converted').cast(DecimalType(scale=4))) \
                    .withColumn('day_low_price_converted', f.col('day_low_price_converted').cast(DecimalType(scale=4))) \
                    .withColumn('eod_price_converted', f.col('eod_price_converted').cast(DecimalType(scale=4))) \
                    .withColumn('fininstrmgnlattrbts_id', f.col('fininstrmgnlattrbts_id').cast(StringType())) \
                    .withColumn('trd_dt', f.col('trd_dt').cast(DateType()))
            except Exception as error:
                traceback.print_exc()
                print('###### An exception occurred during schema check of closing price source######')
                #logging.info('£ in eod_price')
                spark.stop()  # stop context
                exit()
                #raise Exception("£ in eod_price")

            dfClosingPrice.printSchema()

            print('\naggregates ClosingPrice:')
            dfClosingPrice.selectExpr('year(trd_dt) AS year', 'eod_price', 'eod_price_converted', 'fininstrmgnlattrbts_id').groupBy('year', 'fininstrmgnlattrbts_id').agg( \
                f.avg('eod_price').alias('avg_eod_price'), \
                f.sum('eod_price_converted').alias('sum_eod_price_converted'), \
                f.min('eod_price').alias('min_eod_price'), \
                f.max('eod_price').alias('max_eod_price') \
                ).where(f.col('avg_eod_price') >= 10).sort(f.col('year').desc()).show()

            dfClosingPrice.groupBy("ccy").count().show()
            # null values removed represents some transformation cleaning
            dfClosingPrice.na.drop()
            dfClosingPrice.groupBy("ccy").count().show()
            expected_result_count = dfClosingPrice.count() # before writing to database

            #dfClosingPrice.write.jdbc(url=rdbm_url, table='integration.staged_closing_price', \
                                                                 #mode='overwrite', properties=rdbm_properties)

            dfClosingPrice.write.option("truncate", "true").jdbc(url=rdbm_url, table='integration.staged_closing_price', \
                                                           mode='overwrite', properties=rdbm_properties)

            # Read from database postgresql integration.staged_instrument table
            jdbcDfClosingPrice = spark.read.format("jdbc"). \
                options( \
                url=rdbm_url, \
                dbtable='integration.staged_closing_price', \
                user=rdbm_properties['user'], \
                password=rdbm_properties['password'], \
                driver=rdbm_properties['driver']). \
                load()

            # will return DataFrame
            jdbcDfClosingPrice.show()
            print('jdbcDfClosingPrice schema')
            print(f'jdbcDfClosingPrice: {type(jdbcDfClosingPrice)}')
            jdbcDfClosingPrice.printSchema()

            # Test quality
            #expected_result_count = 2207
            # result_count = dfRateFX.distinct().count()
            result_count = jdbcDfClosingPrice.count()
            assert (
                    result_count == expected_result_count), f'Error count Closing Price {result_count} NE {expected_result_count}'
            print(expected_result_count)

            print('Completed Analysis')

            print('stop context')
            spark.stop()  # stop context

    except Exception as error:
        traceback.print_exc()
        print('###### An exception occurred ######')
    else:
        print('\nETL completed')
    finally:
        print('stop context')
        spark.stop()  # stop context

        #https://gist.github.com/hakib/7e723d2c113b947f7920bf55737e4d16

def fca_dbms_build():
    CLIENT_PAGINATION_LIMIT = 500
    SERVER_PAGINATION_LIMIT = 500

    from psycopg2 import connect
    #print(f'psycopg2.__version__:{psycopg2.__version__}')

    print('Start warehouse BUILD')

    # establishing a connection
    conn = connect(
            database=rdbm_properties['Database'], \
            user=rdbm_properties['user'], \
            password=rdbm_properties['password'], \
            host=rdbm_properties['host'], \
            port=rdbm_properties['port']
                    )
    #sql_query = 'SELECT * FROM integration.staged_instrument'
    sql_query = 'select *, unnest(string_to_array(undlyng_isins, E\',\')) as basket from integration.staged_instrument \
                  where length(undlyng_isins) > 12 LIMIT 2000'
    print(sql_query)


    try:
        with conn.cursor('paul') as cur:
            # This fetches only 100 records from DB as batches
            # If you don't specify, the default value is 2000
            cur.itersize = SERVER_PAGINATION_LIMIT
            cur.execute(sql_query)

            while manyRows := cur.fetchmany(CLIENT_PAGINATION_LIMIT):
                kount = 0
                if not manyRows:
                    break
                for rows in manyRows:
                    kount += 1
                    print(f'count {kount}')
                    #print(type(rows))
                    if kount <= 50:  # show(50) equivalent
                        print(rows)  # process the batch here
            # build an appropriate update delete insert sql for a merge operation for dimensions and then finally fact table.
            # some test script string follows merely as a place holder test..

            cur2 = conn.cursor()

            ver = """select version()"""
            cur2.execute(ver)
            curr_ver = cur2.fetchone()
            print (curr_ver)
            # "PostgreSQL 12.5, compiled by Visual C++ build 1914, 64-bit"

            curr_dt = """select current_date"""
            cur2.execute(curr_dt)
            curr_date = cur2.fetchone()
            print(curr_date[0])

            update = """
                 update mobi_ss.dim_instrument 
                    set
                        instrm_shrtnm = staged_instrument.instrm_shrtnm,
                        instrm_fullnm = staged_instrument.instrm_fullnm,
                        last_modified_date = now()
                   from integration.staged_instrument
                  where dim_instrument.instrm_id = staged_instrument.instrm_id
                    and staged_instrument.trad_termntndt > %(curr_date)s
                    and (dim_instrument.instrm_shrtnm != staged_instrument.instrm_shrtnm
                     or  dim_instrument.instrm_fullnm != staged_instrument.instrm_fullnm)
                """
            print(cur2.mogrify(update, {'curr_date': curr_date}))
            cur2.execute(update, {'curr_date': curr_date})

            remove = """
                 delete
                   from integration.staged_instrument
                  using   mobi_ss.dim_instrument
                  where   dim_instrument.instrm_id = staged_instrument.instrm_id
                    and   staged_instrument.type = 'NewRcd'
            """
            print(cur2.mogrify(remove))
            cur2.execute(remove)

            insrt = """
                    insert into mobi_ss.dim_instrument
                            ( -- instrm_key
				                instrm_id,industry_nm, type, instrm_type, instrm_fullnm
				                , instrm_shrtnm, instrm_clssfctntp, instrm_ntnlccy
				                , instrm_cmmdtyderivind, issr, trad_id, trad_issrreq
				                , trad_frsttraddt, trad_termntndt, tech_rlvntcmptntauthrty
				                , undlyng_type, undlyng_subtype, undlyng_isins
				                , basket_array, created_date, last_modified_date, current_active
			                )
                    select  instrm_id,
		   	                industry_nm, type, instrm_type, instrm_fullnm
		                    , instrm_shrtnm, instrm_clssfctntp, instrm_ntnlccy
		                    , instrm_cmmdtyderivind, issr, trad_id, trad_issrreq
		                    , trad_frsttraddt, trad_termntndt, tech_rlvntcmptntauthrty
		                    , undlyng_type, undlyng_subtype, undlyng_isins
			                , string_to_array(undlyng_isins,',')
				            , now() --created_date
	                        , now() --last_modified_date
	  			            , 1 --current_active
  				        from integration.staged_instrument
                  """
            print(cur2.mogrify(insrt))
            cur2.execute(insrt)

            cur2.close()

    except Exception as error:
        traceback.print_exc()
        print('###### An exception occurred ######')
    else:
        print('\nWarehouse build completed')

if __name__ == "__main__":
    # change confPath to the absolute path location of the db_properties.ini file.
    confPath = r'C:\Users\username\Spark\spark-3.0.1-bin-hadoop2.7\db_properties.ini'
    xmlPath = r'C:\Users\username\Spark\spark-3.0.1-bin-hadoop2.7\spark_xml'

    import configparser, traceback

    rdbm_properties = {}
    config = configparser.ConfigParser()
    config.read(confPath)
    rdbm_prop = config['postgres']
    rdbm_url = rdbm_prop['url']
    rdbm_properties['user'] = rdbm_prop['user']
    rdbm_properties['password'] = rdbm_prop['password']
    # rdbm_properties['url'] = rdbm_prop['url']
    rdbm_properties['driver'] = rdbm_prop['driver']

    print(rdbm_prop)
    #print(rdbm_properties)

    try:
        with T():
            logging.basicConfig(filename='FCA_MOBI.log', level=logging.INFO)
            logging.info('Started')
            fca_ETL()
            logging.info('Finished')

    except Exception as error:
        traceback.print_exc()
        print('###### An exception occurred ######')
    else:
        print('\nApplication completed')

    rdbm_properties['Database'] = rdbm_prop['Database']
    rdbm_properties['host'] = rdbm_prop['host']
    rdbm_properties['port'] = rdbm_prop['port']

    print(rdbm_prop)
    #print(rdbm_properties)

    try:
        with T():
            # full load type 1 or incremental load type 2 merge dimensions first,
            # followed by fact(s) merge insert/update
            fca_dbms_build()
    except Exception as error:
        traceback.print_exc()
        print('###### An exception occurred during warehouse build ######')
        exit()
    else:
        print('\nApplication completed')