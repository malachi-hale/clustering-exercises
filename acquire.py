#Disable warnings
import warnings
warnings.filterwarnings("ignore")

#Libraries for processing data
import pandas as pd
import numpy as np

#Import libraries for graphing
import matplotlib.pyplot as plt
import seaborn as sns

#Libraries for obtaining data from SQL databse
import env
import os

#First we establish a connection to the SQL server
def get_connection(db, user=env.user, host=env.host, password=env.password):
    '''
     We establish a connection to the SQL database, using my information stored in the env file.
    '''
    return f'mysql+pymysql://{user}:{password}@{host}/{db}'

#Now we will make our DataFrame with the relevant Zillow data
def get_zillow_data():
    
    filename = "zillow.csv"
  
    sql = ''' 
    SELECT *
    FROM properties_2017
    LEFT OUTER JOIN airconditioningtype 
    USING(airconditioningtypeid)
    LEFT OUTER JOIN architecturalstyletype
    USING(architecturalstyletypeid)
    LEFT OUTER JOIN buildingclasstype 
    USING(buildingclasstypeid)
    LEFT OUTER JOIN heatingorsystemtype
    USING(heatingorsystemtypeid)
    LEFT OUTER JOIN predictions_2017
    USING(id)
    INNER JOIN (
	SELECT id, MAX(transactiondate) as last_trans_date 
	FROM predictions_2017
	GROUP BY id
    ) predictions ON predictions.id = properties_2017.id AND predictions_2017.transactiondate = predictions.last_trans_date
    LEFT OUTER JOIN propertylandusetype
    USING(propertylandusetypeid)
    LEFT OUTER JOIN storytype
    USING(storytypeid)
    LEFT OUTER JOIN typeconstructiontype
    USING(typeconstructiontypeid)
    JOIN unique_properties
    ON unique_properties.parcelid = properties_2017.parcelid
    WHERE latitude IS NOT NULL and longitude IS NOT NULL;
    '''
    if os.path.isfile(filename):
        return pd.read_csv(filename)
    else:
        df = pd.read_sql(sql, get_connection('zillow'))
        #eliminate duplicate columns
        df = df.loc[:,~df.columns.duplicated()]
        return df
    df = pd.read_sql(sql, get_connection('zillow'))
    return df