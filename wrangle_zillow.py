#Disable warnings
import warnings
warnings.filterwarnings("ignore")

#Libraries for processing data
import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split

#Import libraries for graphing
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import seaborn as sns

#Libraries for obtaining data from SQL databse
import env
import os

#Library for statistical testing
from scipy import stats

##Acquire function
from acquire import get_zillow_data

#Library for dealing with NA values
from sklearn.impute import SimpleImputer

#First we establish a connection to the SQL server
def get_connection(db, user=env.user, host=env.host, password=env.password):
    '''
     We establish a connection to the SQL database, using my information stored in the env file.
    '''
    return f'mysql+pymysql://{user}:{password}@{host}/{db}'


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


def only_single_unit(df):
    df = df[df.propertylandusetypeid.isin([261.0, 266.0, 263.0, 269.0, 275.0, 264.0])]
    return df

def handle_missing_values(df, prop_required_column = .5, prop_required_row = .7):
    threshold = int(round(prop_required_column*len(df.index),0))
    df.dropna(axis=1, thresh=threshold, inplace=True)
    threshold = int(round(prop_required_row*len(df.columns),0))
    df.dropna(axis=0, thresh=threshold, inplace=True)
    return df


def wrangle_zillow():
    df = get_zillow_data()
    
    df = only_single_unit(df)

    df = handle_missing_values(df, prop_required_column = .5, prop_required_row = .75)

    return df

def split_data(df):
    '''
    take in a DataFrame and return train, validate, and test DataFrames.
    return train, validate, test DataFrames.
    '''
    
    # splits df into train_validate and test using train_test_split() stratifying on churn to get an even mix of each churn, yes or no
    train_validate, test = train_test_split(df, test_size=.2, random_state=123)
    
    # splits train_validate into train and validate using train_test_split() stratifying on churn to get an even mix of each churn
    train, validate = train_test_split(train_validate, 
                                       test_size=.3, 
                                       random_state=123)
    return train, validate, test

