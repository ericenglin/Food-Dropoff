# -*- coding: utf-8 -*-
"""
Spyder Editor

This is a temporary script file.
"""

import pandas as pd
import geopandas as gpd
from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter
import numpy as np
import geopandas
from datetime import date, timedelta
import warnings
warnings.filterwarnings("ignore")

number_drivers = 35

tomorrow = date.today()+ timedelta(days=1)
tomorrow = tomorrow.strftime("%m_%d")

filename ='./data/' + str(tomorrow) + "_deliveries.csv"
df = pd.read_csv(filename)
good_points = pd.read_csv("point_table.csv")

smaller_df = good_points[['ADDRESS','location', 'point', 
                          'latitude', 'longitude','altitude', 
                          'point_location']].drop_duplicates()



def cleaning(df):
    
    #data cleaning
    df['Street_clean']=df['Street Name']
    for address in df['Street_clean']:
        try:
            df.loc[df.Street_clean==address,'Street_clean']=address.replace(".","")
            df.loc[df.Street_clean==address,'Street_clean']=address.replace(",","")
            df.loc[df.Street_clean==address,'Street_clean']=address.replace(";","")
            df.loc[df.Street_clean==address,'Street_clean']=address.replace("/","")
            df.loc[df.Street_clean==address,'Street_clean']=address.rstrip()
        except:
            print(address)
    
    for address in df['Street_clean']:
        try:
            x=address.lower()
            
            if x[:2].isdigit():
                df.loc[df.Street_clean==address,'Street_clean']=x[x.find(" "):]
    
            if "-" in x:
                df.loc[df.Street_clean==address,'Street_clean']=x[:x.find("-")]
            if "(" in x:
                df.loc[df.Street_clean==address,'Street_clean']=x[:x.find("(")]
            if "calendar stret" in x:
                df.loc[df.Street_clean==address,'Street_clean']="Callender Street"
            if "alston street" in x:
                df.loc[df.Street_clean==address,'Street_clean']="Allston Street"
            if "auburn" in x:
                df.loc[df.Street_clean==address,'Street_clean']="Auburn Street"
            if "linwood pl" in x:
                df.loc[df.Street_clean==address,'Street_clean']="Broadway"
            if "mass ave" in x or "":
                df.loc[df.Street_clean==address,'Street_clean']="Massachusetts Avenue"
            if  "aveue" in x:
                df.loc[df.Street_clean==address,'Street_clean']=x[:x.find("aveue")] + "Avenue"
            if "stret" in x:
                df.loc[df.Street_clean==address,'Street_clean']=x[:x.find("Stret")] + "Street"
                df.loc[df.Street_clean==address,'Street_clean']=x[:x.find("stret")] + "Street"
            if  "cambridge park" in x:
                df.loc[df.Street_clean==address,'Street_clean']="Cambridgepark Drive"
            if "lansdown" in x or "lansdowne" in x:
                df.loc[df.Street_clean==address,'Street_clean']="Landsdowne St"
            if " apt " in x :
                df.loc[df.Street_clean==address,'Street_clean']=x[:x.find("apt")]
            if " bldg " in x :
                df.loc[df.Street_clean==address,'Street_clean']=x[:x.find("bldg")]
            if x[-4:]=='bldg':
                df.loc[df.Street_clean==address,'Street_clean']=x[:x.find("bldg")]
            if x[-3:]=='apt':
                df.loc[df.Street_clean==address,'Street_clean']=x[:x.find("apt")]
            if "frankln" in x or 'franlin' in x or 'frankin' in x or "frtanklin" in x:
                df.loc[df.Street_clean==address,'Street_clean']="Franklin Street"
            if "jackon" in x:
                df.loc[df.Street_clean==address,'Street_clean']="Jackson Pl"
            if "muesum" in x:
                df.loc[df.Street_clean==address,'Street_clean']="Museum Way"
            if "newtown" in x or "newton" in x:
                df.loc[df.Street_clean==address,'Street_clean']="Main Street"
                df.loc[df.Street_clean==address,'Street #']="637"
            if "eerie" in x:
                df.loc[df.Street_clean==address,'Street_clean']="Erie Street"
            if "8th" in x:
                df.loc[df.Street_clean==address,'Street_clean']="Eighth Street"
            if "cliffton" in x:
                df.loc[df.Street_clean==address,'Street_clean']="Clifton Place"
        except:
            print(address)
            
    df['ADDRESS']=""
    for address in df['ADDRESS']:
        if address == "":
            df['ADDRESS'] = df['Street #'].astype(str) + ' ' + \
                            df['Street_clean'] + ',' + \
                            "Cambridge" + ',' + \
                            "Massachusetts" + ',' + ' USA'   
    
    return df

def geocode(df):
    locator = Nominatim(user_agent="myGeocoder")
    geocode = RateLimiter(locator.geocode, min_delay_seconds=1)
    df['location'] = df['ADDRESS'].apply(geocode)
    df['point'] = df['location'].apply(lambda loc: tuple(loc.point) if loc else None)
    
    df[['latitude', 'longitude', 'altitude']] = pd.DataFrame(df['point'].tolist(), index=bad_table.index)
    df['point_location'] = bad_table["longitude"].astype(str) + "," + df["latitude"].astype(str)

    null_df = df.loc[df['point'].isnull()]
    df=df.dropna(subset=['point'], how='all')
    return df, null_df

def neighborhood_clean(df):
    neighborhood_list_temp = []
    
    neighborhood_list = ['The Port','Neighborhood Nine','Area 2/MIT',
                    'Cambridgeport','Riverside','Mid-Cambridge',
                    'Wellington-Harrington','East Cambridge','Agassiz','Cambridge Highlands',
                    'Strawberry Hill','West Cambridge','North Cambridge']
    
    for x in df_new['NAME']:
        z=0
        for y in neighborhood_list:
            if y in str(x):
                if y in ["Cambridge Highlands","West Cambridge", "Strawberry Hill"]:
                    neighborhood_list_temp.append('North Cambridge')
                elif y in ["Agassiz","Neighborhood Nine","Riverside"]:
                    neighborhood_list_temp.append('Mid-Cambridge')
                elif y == "Area 2/MIT":
                    neighborhood_list_temp.append('Cambridgeport')
                elif y in ["Wellington-Harrington","The Port"]:
                    neighborhood_list_temp.append('East Cambridge')
                else: 
                    neighborhood_list_temp.append(y)
                z=1
                break
        if z==0:
            neighborhood_list_temp.append('Problem Location')
        
    return neighborhood_list_temp

def run_kmeans(df, n_clusters, min_size, max_size):
    from k_means_constrained import KMeansConstrained
    df_temp = df[['latitude', 'longitude']]
    # Convert DataFrame to matrix
    mat = df_temp.values
    # Using sklearn
    km = KMeansConstrained(
         n_clusters=n_clusters,
         size_min=min_size,
         size_max=max_size,
         random_state=0
    ).fit(mat)
    # Get cluster assignment labels
    labels = km.labels_
    return labels

def find_cambridge_addresses(df):
    
    # Filepath
    fp = "./Cambridge_Neighborhoods.shp"
    
    # Read the data
    pop = gpd.read_file(fp)
    pop = pop[['NAME','geometry']]
    pop=pop.to_crs(epsg=4326)
    gdf = geopandas.GeoDataFrame(
        df, geometry=geopandas.points_from_xy(df.longitude, df.latitude))

    df = gpd.sjoin(gdf, pop, how="left", op="within")
    return df

def add_dummies(df):
    # Get one hot encoding of columns B
    one_hot = pd.get_dummies(df['neighborhood'])
    # Drop column B as it is now encoded
    df = df.drop('neighborhood',axis = 1)
    # Join the encoded df
    df = df.join(one_hot)
    return df



df = cleaning(df)

point_table= df.merge(smaller_df,on = ['ADDRESS'],how='left')
bad_table = point_table.loc[point_table['point'].isnull()]
good_table = point_table.dropna(subset=['point'], how='all')
geocoded_table, null_table = geocode(bad_table)
print("Number of addresses not found: " + str(len(null_table)))

df_new = pd.concat([good_table,geocoded_table])

df_new = df_new.reset_index()
df_new = find_cambridge_addresses(df_new)
print("Number of non-Cambridge addresses:" + str(len(df_new.loc[df_new['NAME'].isnull()])))
null_table = pd.concat([null_table,df_new.loc[df_new['NAME'].isnull()]])
df_new = df_new.dropna(subset=['NAME'], how='all')
df_new['neighborhood']=neighborhood_clean(df_new)
df_new = add_dummies(df_new)

df_new = df_new.sort_values(['Senior'])
df_seniors = df_new.loc[df_new['Senior']==1]
df_nonseniors = df_new.loc[df_new['Senior']!=1]
senior_clusters = int(round(len(df_seniors)/10,0))
nonsenior_clusters = number_drivers - senior_clusters

df_senior_labels = run_kmeans(df_seniors, int(senior_clusters),9,12)
df_nonsenior_labels = run_kmeans(df_nonseniors, int(nonsenior_clusters),9,12)
df_nonsenior_labels = df_nonsenior_labels + senior_clusters

all_labels = np.concatenate([df_nonsenior_labels, df_senior_labels])
df_new['kmeans_cluster'] = all_labels

df_new['Driver_ID']=df_new['kmeans_cluster']
null_table['Driver_ID']=99

df_new = df_new[['ID','First Name', 'Last Name','Street #', 'Phone',
                 'Notes','Street Name', 'Apt #', 'Senior','New','location', 
                 'latitude','longitude','Driver_ID']]

driver_table_final = pd.concat([null_table,df_new])
driver_table_final=driver_table_final[['ID','First Name', 'Last Name',
                                        'Street #','Street Name','Apt #',
                                        'Driver_ID', 'New','Senior','Phone',
                                        'Notes','location','latitude','longitude']].sort_values(by = ['Driver_ID', 'Street Name', 
                                                       'Street #', 'Apt #'],ascending=False)

file_name_final = "./delivery routes/Driver_Table_" + str(tomorrow) + ".xlsx"
driver_table_final.to_excel(file_name_final, index=False)





