# MOBI
Task: ETL with pySpark 3.0.1

Windows 10 (for example: PS Get-ChildItem Env: | sort name) SPARK_HOME=C:\Users\username\Spark\spark-3.0.1-bin-hadoop2.7 JAVA_HOME=C:\Java\jdk-15.0.1 HADOOP_HOME=C:\Users\username\Spark\spark-3.0.1-bin-hadoop2.7\hadoop

All jars were pip installed to C:\Users\username\Spark\spark-3.0.1-bin-hadoop2.7\jars for example postgresql-42.2.18.jar spark-xml_2.12-0.9.0

IDE used was PyCharm professional 2020.3  (free trial)

make sure all of these files are in the local directory where the FCA_MOBI_final.py script is fx_rates.csv', 'instruments.json , 'closing_prices.xml'
NB Also copy the 'closing_prices.xml' to a path C:\Users\user\Spark\spark-3.0.1-bin-hadoop2.7\spark_xml

there are three files to deploy/implement besides above mentioned environment variables checking/setting.

db_properties.ini
MOBItablescript.sql
FCA_MOBI_final.py
Note RunCaptureOutput.log (is example of captured output of a run as standalone laptop using Windows 10)

Run FCA_MOBI_final.py from an IDE of choice ( python.exe FCA_MOBI_final.py [probably can add absolute path for each file] ) FCA_MOBI_final.py requires a hardcode change in main dunder conditional section(confPath and xmlPath) to set absolute path of ini file and 
the closing_prices.xml

Logon as user postgres create a database if required, run the MOBItablescript.sql script to create an empty database tables
Manually Create two schemas integration and mobi_ss before running.

NB Cautionary note whichever tablename used, will be overwritten which means in postgresql if it already exists it will be dropped and recreated, 
any data in this table beforehand will be lost. The tablename you decide to use is important make sure it is a new tablename for saftey :)
to this point I have used write.option("truncate", "true") to preserve table structures.

Did not have enough time to finalise(physicalise) a star schema and incremental/full load.
The design approximately is dim_rate, dim_closing_price, dim_date (these three dimensions have implicit 'effective' dates as is)The fact table can not be rebuilt from scratch with using the three core date dimensions. 
fact_MOBI_security has FKs to the dimensions and measures (numbers, event) as the six prices 
    day_high_price numeric(10,4),
    day_low_price numeric(10,4),
    eod_price numeric(10,4),
	day_high_price_converted numeric(10,4),
    day_low_price_converted numeric(10,4),
    eod_price_converted numeric(10,4)
	trd_date_key
	instrm_key
	instrm_id
	basket_key

dim_instrument will hold instrm_key, basket_key[] using postgresql array. This dimension holds most of the instruments.json
dim_instrument_attr holds the one or many ISINS of the basket_key(instrm_key)

Did not containerize - will try later. Logging/monitoring pipeline results in persistent table(s) TBC next.
