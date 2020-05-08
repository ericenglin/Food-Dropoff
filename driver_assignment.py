# -*- coding: utf-8 -*-
"""
Spyder Editor

This is a temporary script file.
"""


def cleaning(df):
    """
    steps to clean our dataframe from F4F
    Steps included:
        -if no flag for senior, add a non-senior flag
        -create a new column "Street_clean" for fixed addresses
        -remove all punctuation from address
        -make all addresses lower case
        -fix specific issues in case-by-case basis
        -combine into one column called ADDRESS for matching and geocoding
    """
    import pandas as pd

    #data cleaning
    df.loc[df['Senior'].isnull(),'Senior']='NO'
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

    import pandas as pd
    from geopy.geocoders import Nominatim
    from geopy.extra.rate_limiter import RateLimiter
    locator = Nominatim(user_agent="myGeocoder")
    geocode = RateLimiter(locator.geocode, min_delay_seconds=1)
    df['location'] = df['ADDRESS'].apply(geocode)
    df['point'] = df['location'].apply(lambda loc: tuple(loc.point) if loc else None)
    null_df = df.loc[df['point'].isnull()]
    null_df[['latitude', 'longitude', 'point_location']] = None
    df=df.dropna(subset=['point'], how='all')

    print(len(df), len(df.index), len(df['point']))
    try:
        df[['latitude', 'longitude', 'altitude']] = pd.DataFrame(df['point'].tolist(), index=df.index)
        df['point_location'] = df["longitude"].astype(str) + "," + df["latitude"].astype(str)
        df = df.drop(columns = ['point','altitude'])
    except:
        df[['latitude', 'longitude','point_location']]=None,None,None
    null_df = null_df.drop(columns = ['point'])

    return df, null_df

def neighborhood_clean(df):
    """
    Cambridge divided into ~10 neighborhoods that are mapped from shapefile
    Many of these have few people which messes up kmeans clustering model, so want to group neighborhoods that are close to eachother
    All grouped into 4 somewhat arbitrary groups:
        -North Cambridge
        -Mid-Cambridge
        -Cambridgeport
        -East Cambridge
    """
    neighborhood_list_temp = []

    neighborhood_list = ['The Port','Neighborhood Nine','Area 2/MIT',
                    'Cambridgeport','Riverside','Mid-Cambridge',
                    'Wellington-Harrington','East Cambridge','Agassiz','Cambridge Highlands',
                    'Strawberry Hill','West Cambridge','North Cambridge']

    for x in df['NAME']:
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
    """
    Inputs:
        -df: original dataframe
        -n_clusters: number of clusters that kmeans should make
        -min_size: minimum people in clusters
        -max_size: maximum people in clusters

    Returns: labels as array
    """
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
    """
    join our addresses with a shapefile from  Cambridge GIS site.
        https://www.cambridgema.gov/GIS/gisdatadictionary/Boundary/BOUNDARY_CDDNeighborhoods
    Returns: df with our new neighborhood NAME column
    """
    import geopandas
    # Filepath
    fp = "/projects/f3deliveries/app/Cambridge_Neighborhoods.shp" #full path needed for app
    #fp = "./Cambridge_Neighborhoods.shp"

    # Read the data
    pop = geopandas.read_file(fp)
    pop = pop[['NAME','geometry']]
    pop=pop.to_crs(epsg=4326)
    #pop.to_csv("/projects/f3deliveries/app/temp_pop.csv")

    #pandas.set_option('display.max_columns', None)
    #df.to_csv("/projects/f3deliveries/app/temp_df.csv")

    gdf = geopandas.GeoDataFrame(
        df, geometry=geopandas.points_from_xy(df.longitude.astype(float), df.latitude.astype(float)))

    df = geopandas.sjoin(gdf, pop, how="left", op="within")
    return df

def add_dummies(df):
    """
    Make dummy flags from our neighborhood columns
    """
    import pandas as pd

    # Get one hot encoding of columns B
    one_hot = pd.get_dummies(df['neighborhood'])
    # Drop column B as it is now encoded
    df = df.drop('neighborhood',axis = 1)
    # Join the encoded df
    df = df.join(one_hot)
    return df


def update_point_table(point_table, full_df):
    #update points in point table
    import pandas as pd
    full_df = full_df[['ADDRESS','location',
                              'latitude', 'longitude',
                              'point_location']]


    new_point_table = pd.concat([full_df, point_table])
    new_point_table = new_point_table.drop_duplicates(subset=['ADDRESS']).dropna(subset=['ADDRESS'],
                                                                                 how='all').sort_values(by = ['ADDRESS'])
    return new_point_table


def map_routes(df):
    import folium
    color_dict = {-1:'darkgreen',0:'darkpurple',1:'green',2:'purple',3:'red',4:'gray',5:'lightblue',
             6:'beige',7:'cadetblue',8:'black',9:'pink',10:'darkred',11:'lightred',
             12:'darkblue',13:'lightgreen',14:'darkgreen',15:'darkpurple',16:'green',
              17:'purple',18:'red',19:'gray',20:'lightblue',
             21:'beige',22:'cadetblue',23:'black',24:'pink',25:'darkred',26:'lightred',
             27:'darkblue',28:'lightgreen',29:'white',30:'gray',31:'cadetblue',32:'orange',33:'green',
              34:'white',35:'darkgreen'}

    df['kmeans_color']=df['kmeans_cluster'].map(color_dict)

    locations = df[['latitude', 'longitude']]
    locationlist = locations.values.tolist()
    df = df.reset_index()

    map1 = folium.Map(location=[42.379750, -71.101182], zoom_start=13)
    for point in range(0, len(locationlist)):
        folium.Marker(locationlist[point], popup=('Driver Group: '+str(df['kmeans_cluster'][point])),
                     icon=folium.Icon(color=df["kmeans_color"][point])).add_to(map1)

    return map1
    #map1.save('driver routes.html')


def assign_drivers_function(initial_dataframe, num_drivers, initial_point_table):
    """
    Inputs:
        -data: pandas dataframe with addresses and normal column headers in F4F tables
        -num_drivers: number of driver ids to create
    Returns:
        -creates a csv file in same folder with driver assignments for all people
        -driver_table_final: pandas dataframe with all new columns
    Notes:
        -Uses saved lat/longs in point_table.csv
        -Creates kmeans clusters for seniors and nonseniors
        -Pulls out non-Cambridge addresses and unidentified addresses as '99' Driver_ID
    """
    #from assignment_scripts import cleaning, geocode, neighborhood_clean, run_kmeans, find_cambridge_addresses, add_dummies
    import pandas as pd
    import numpy as np
    from datetime import date, timedelta
    import warnings
    warnings.filterwarnings("ignore")

    number_drivers = num_drivers

    tomorrow = date.today()+ timedelta(days=1)
    tomorrow = tomorrow.strftime("%m_%d")

    df = cleaning(initial_dataframe)

    point_table= df.merge(initial_point_table,on = ['ADDRESS'],how='left')
    bad_table = point_table.loc[point_table['latitude'].isnull()]
    good_table = point_table.dropna(subset=['latitude'], how='all')
    geocoded_table, null_table = geocode(bad_table)
    df_new = pd.concat([good_table,geocoded_table])
    print("df_new: ", len(df_new))

    df_new = df_new.reset_index()

    #assigns addresses to cambridge neighborhoods
    #df_new = find_cambridge_addresses(df_new)
    #null_table = pd.concat([null_table,df_new.loc[df_new['NAME'].isnull()]])
    #df_new = df_new.dropna(subset=['NAME'], how='all')
    #df_new['neighborhood']=neighborhood_clean(df_new)
    #df_new = add_dummies(df_new)

    df_new = df_new.sort_values(['Senior'])
    df_seniors = df_new.loc[df_new['Senior']=='YES']
    df_nonseniors = df_new.loc[df_new['Senior']!='YES']
    print("df_seniors: ", len(df_seniors))
    print("df_nonseniors: ", len(df_nonseniors))
    senior_clusters = int(round(len(df_seniors)/10,0))
    nonsenior_clusters = number_drivers - senior_clusters

    df_senior_labels = run_kmeans(df_seniors, int(senior_clusters),9,12)
    df_nonsenior_labels = run_kmeans(df_nonseniors, int(nonsenior_clusters),9,12)
    df_nonsenior_labels = df_nonsenior_labels + senior_clusters

    all_labels = np.concatenate([df_nonsenior_labels, df_senior_labels])
    df_new['kmeans_cluster'] = all_labels
    driver_map = map_routes(df_new)

    df_new['Driver_ID']=df_new['kmeans_cluster']
    null_table['Driver_ID']=99

    driver_table_final = pd.concat([null_table,df_new])

    #for col in driver_table_final.columns:
    #    print(">" + col + "<")

    driver_table_final=driver_table_final[['ID','First Name', 'Last Name',
                                            'Street #','Street Name','Apt #',
                                            'Driver_ID', 'Gift Card','Senior','Phone',
                                            'Notes','ADDRESS','location','latitude','longitude']].sort_values(by = ['Driver_ID', 'Street Name',
                                                           'Street #', 'Apt #'],ascending=False)

    #file_name_final = "Driver_Table_" + str(tomorrow) + ".csv"
    #driver_table_final.to_csv(file_name_final, index=False)



    updated_point_table = update_point_table(initial_point_table, df_new)

    return driver_table_final, updated_point_table, driver_map
