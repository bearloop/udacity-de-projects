from pyspark.sql import functions as F
from datetime import datetime, timedelta
from pyspark.sql.types import *
import re

def print_size(df, name):
    '''
    Prints the size (rows, columns) of a Spark dataframe.
    ArgumentsL
    df: Spark dataframe
    name: string
    '''
    print(name+' (rows, columns): ' + '(' + str(df.count())+','+str(len(df.columns))+')')

    
# # Create list of valid ports
# i94_sas_desc = "I94_SAS_Labels_Descriptions.SAS"
# with open(i94_sas_desc) as f:
#     sas_lines = f.readlines()
    
# re_compiled = re.compile(r"\'(.*)\'.*\'(.*)\'")
# valid_ports = {}
# for line in sas_lines[302:961]:
#     results = re_compiled.search(line)
#     valid_ports[results.group(1)] = results.group(2).rstrip()
    
    
# def convert_entry_point(x):
#     if x:
#         return valid_ports[x]
#     return None


# fun_entry_point = F.udf(lambda x: convert_entry_point(x), StringType())


# df_immigration_clean.withColumn('entry_point_full',  fun_entry_point(df_immigration_clean.entry_point)).show()
    
# print(len(valid_ports))
# print(valid_ports)
    
    
def clean_up_immigration_df(df):
    '''
    A method to clean up the immigration data set and to keep certain columns that we will be using in the data modeling exercise.
    It also converts SAS dates to date strings. Columns 'i94yr','i94mon','i94port','i94mode','i94addr','i94cit','i94res' are 
    not allowed to have null values, otherwise they're dropped from the dataframe.
    
    Arguments:
    immigration_df: Spark dataframe that includes immigration data
    
    Returns:
    clean immigration_df
    '''
    
#     # Create new column: approx_age with condition (if positive age difference, else None) to make the age of visitors available to analysts
#     df = df.withColumn('approx_age',
#                        F.when(F.col("i94yr") - F.col('biryear') > 0,
#                               F.col("i94yr") - F.col('biryear')
#                              ).otherwise(None)
#                       )

    # A.Columns to drop:

    cols_to_drop = ['matflag', 'dtaddto', 'gender', 'insnum', 'airline', 'admnum',
                    'fltno', 'visatype', 'entdepu', 'entdepd', 'entdepa', 'occup',
                    'biryear', 'visapost', 'dtadfile']

    df = df.drop(*cols_to_drop)


 
    # B. Columns to convert:

    # Create udf to convert SAS date to PySpark date 
    def convert_datetime(x):
        if x:
            return (datetime(1960, 1, 1).date() + timedelta(x)).isoformat()
        return None


    fun_date = F.udf(lambda x: convert_datetime(x), StringType())

    # Convert arrival and departure date to strings
    df = df.withColumn("arrdate", fun_date(df.arrdate))
    df = df.withColumn("depdate", fun_date(df.depdate))
    
    
     # C. Rows to drop:

    # Drop rows where mode of transport is not reported or missing
    df = df.na.drop(subset=['i94mode']).filter(F.col('i94mode')!=9)

    df = df.na.drop(subset=['i94yr','i94mon','i94port','i94mode','i94addr','i94cit','i94res'])
    
    df = df.select(F.col("cicid").alias("id"),
                   F.col("i94yr").alias("year"),
                   F.col("i94mon").alias("month"),
                   F.col("i94cit").alias("country_it"),
                   F.col("i94res").alias('country_res'),
                   F.col("i94port").alias('entry_point'),
                   F.col("arrdate").alias('date_arrival'),
                   F.col("depdate").alias('date_depart'),
                   F.col("i94mode").alias('mode_transport'),
                   F.col("i94addr").alias('us_state'),
                   F.col("i94bir").alias('visitor_birth_yr'),
                   F.col("i94visa").alias('visa_type'),
                   F.col("count").alias('col_one')
                  )
    
    return df



def clean_up_demographics_df(df):
    '''
    A method to clean up the US cities demographics data set, by converting some columns' data types from string to int or double.
    
    Arguments:
    demographics_df: Spark dataframe that includes US cities demographics data
    
    Returns:
    clean demographics_df
    '''
    
    # Change Average Household Size type to double

    try:
        df = df.withColumn('Average Household Size', df['Average Household Size'].cast("double"))

    except:
        print('Column: Average Household Size not in demographics df - no changes  were performed')

    # Change dtype to int for the following columns

    demographics_int_cols = ['Median Age', 'Male Population', 'Female Population', 'Total Population', 'Number of Veterans', 'Foreign-born', 'Count']
    
    for col in demographics_int_cols:
        
        try:
            df = df.withColumn(col, df[col].cast("integer"))
        except:
            print('Column: ' + col +' not in demographics df - no changes  were performed')
    
    df = df.select(F.col("City").alias("us_city_name"),
                   F.col("State").alias("us_state_name"),
                   F.col("Median Age").alias("median_age"),
                   F.col("Male Population").alias("male_pop"),
                   F.col("Female Population").alias('female_pop'),
                   F.col("Total Population").alias('total_pop'),
                   F.col("Number of Veterans").alias('veterans_pop'),
                   F.col("Foreign-born").alias('foreign_born_pop'),
                   F.col("Average Household Size").alias('mean_hhold_size'),
                   F.col("State Code").alias('us_state'),
                   F.col("Race").alias('ethno_group'),
                   F.col("Count").alias('total_count')
                  )

    return df



def clean_up_airports_df(df):
    '''
    A method to clean up the airports data set by filtering out any non-US airports and dropping the prefix "US-" from "iso_region". The method also drops all airports that have shut down.
    In addition, coordinates are split to latitude and longitude, and data types for elevation feet as well as lat/lon are converted to double.
    
    Arguments:
    airports_df: Spark dataframe that includes airports data
    
    Returns:
    clean airports_df
    '''
    
    # A. Filter for US airports only
    df = df.filter(df.iso_country == "US")

    # B. Drop the prefix "US-" from "iso_region"
    df = df.withColumn("iso_region", F.regexp_replace("iso_region", r'US-', ''))
    
    # C. Drop rows of airports that  have "closed"
    df = df.where(F.col('type') != 'closed')

    # D. Split longitude and latitude
    df = df.withColumn("split", F.split(F.col("coordinates"), ","))
    df = df.withColumn('lat',F.col('split')[0].alias('lat'))
    df = df.withColumn('lon',F.col('split')[1].alias('lon'))

    # E. Drop redundant columns
    df = df.drop(*['continent','coordinates','split'])

    # F. Convert dtypes to double
    airports_double_cols = ['elevation_ft','lat','lon']
    
    for col in airports_double_cols:
        
        try:
            df = df.withColumn(col, df[col].cast("double"))
        except:
            print('Column: ' + col +' not in airports df - no changes  were performed')
            
            
    # Change column ids
    df = df.select(F.col("ident"),
                   F.col("type").alias("airport_type"),
                   F.col("name").alias("airport_name"),
                   F.col("iso_country"),
                   F.col("iso_region").alias('us_state'),
                   F.col("municipality").alias('us_city_name'),
                   F.col("gps_code"),
                   F.col("iata_code"),
                   F.col("local_code"),
                   F.col("lat"),
                   F.col("lon")
                  )

    return df