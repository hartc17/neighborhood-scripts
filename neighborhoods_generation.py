import os
import pandas as pd
import geopandas as gpd
import argparse
import requests
import json
from shapely.geometry import Point
from shapely.ops import nearest_points

CENSUS_API_KEY = 'fe249528ca2dd381dd33844ec7cc31efe438c549'


def get_args():
    """use argparse to get command line arguments

    Returns:
        args (argparse.Namespace): the arguments passed in from the command line
    """
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--zhvi_directory', type=str, default='zhvi_assets',
        help='Path to the directory containing ZHVI data'
    )
    parser.add_argument(
        '--zori_directory', type=str, default='zori_assets',
        help='Path to the directory containing ZORI data'
    )
    parser.add_argument(
        '--spatial_directory', type=str, default='spatial_assets',
        help='Path to the directory containing spatial data'
    )
    parser.add_argument(
        '--geojson_directory', type=str, default='geojsons',
        help='Path to the directory where GeoJSON files will be saved'
    )
    parser.add_argument(
        '--csv_directory', type=str, default='csvs',
        help='Path to the directory where CSV files will be saved'
    )
    parser.add_argument(
        '--census_geography', type=str, default='block',
        help='Path to the directory where CSV files will be saved'
    )
    args = parser.parse_args()
    return args



def get_recent_file(directory, identifier):
    """Get the file path for the most recently edited CSV file in the directory which has in its name the string passed as parameter identifier

    Args:
        directory (str): the directory path
        identifier (str): the string to be searched in the file name

    Returns:
        str: the file path for the most recently edited CSV file in the directory which has in its name the string passed as parameter identifier
    """
    return max([os.path.join(directory, f) for f in os.listdir(directory) if f.endswith('.csv') and identifier in f], key=os.path.getmtime)


def convert_to_geojson(gdf, directory, filename):
    """Convert the GeoDataFrame to a GeoJSON file in the directory

    Args:
        gdf (GeoDataFrame): the GeoDataFrame to be converted
        directory (str): the directory path
        filename (str): the file name
    """
    gdf.to_file(os.path.join(directory, filename), driver='GeoJSON')
    
def convert_to_csv(gdf, directory, filename):
    """Convert the GeoDataFrame to a CSV file in the directory

    Args:
        gdf (GeoDataFrame): the GeoDataFrame to be converted
        directory (str): the directory path
        filename (str): the file name
    """
    gdf.to_csv(os.path.join(directory, filename))


def merge_dataframes(left_df, right_df, left_on, right_on, suffixes, rename_last_col=None):
    """Merge two DataFrames and drop the right_on column and the last column (which is the one with the same name as the right_on column) and rename the last column with the value of the parameter rename_last_col

    Args:
        left_df (dataframe): the left DataFrame
        right_df (dataframe): the right DataFrame
        left_on (str): the column name of the left DataFrame to be used as key (left_on='neighborhood')
        right_on (str): the column name of the right DataFrame to be used as key (right_on='RegionName')
        suffixes (tuple): the suffixes to be added to the columns with the same name in the two DataFrames (suffixes=('_left', '_right'))
        rename_last_col (str, optional): the new name for the last column. Defaults to None.

    Returns:
        dataframe: the merged DataFrame
    """
    merged_df = pd.merge(left_df, right_df, how='left', left_on=left_on, right_on=right_on, suffixes=suffixes)
    merged_df.drop(columns=right_on, inplace=True)
    if rename_last_col is not None:
        merged_df.rename(columns={merged_df.columns[-1]: rename_last_col}, inplace=True)
    merged_df.drop_duplicates(subset=['neighborhood'], inplace=True)
    return merged_df



def query_census_by_geography(county, geography, out_format='geojson', fields='*'):
    """Query the Census API for the given counties at the given geography level

    Args:
        county (str): the county FIPS code (e.g. '06037')
        geography (str): the geography level to be queried (either 'tract' or 'block_group')
        out_format (str, optional): the format of the response. Defaults to 'geojson'.
        fields (str, optional): the fields to be returned by the API. Defaults to '*'.

    Returns:
        dict: the response JSON
    """
    sub_layer = {'tract': 0, 'block_group': 1, 'block': 2}
    
    #last 3 digits of the county fips 
    county_str = county[2:] 
    state_str = county[0:2]
    where_clause = f"COUNTY='{county_str}' and STATE='{state_str}'"
        
    #set the base URL for the API request
    base_url = 'https://tigerweb.geo.census.gov/arcgis/rest/services/TIGERweb/Tracts_Blocks/MapServer/'
    
    
    print (f'Querying Census API for county_id :{county}')
    url = f'{base_url}{sub_layer[geography]}/query?where={where_clause}&outFields={fields}&f={out_format}'
    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
    except requests.exceptions.HTTPError as http_err:
        print(f'HTTP error occurred: {http_err}')
        data = None
    except requests.exceptions.RequestException as err:
        print(f'Other error occurred: {err}')
        data = None
        
    print (f'Number of {geography}s returned: {len(data["features"])}')
    return data


def split_counties_and_query_census(county_list, geography):
    """Split the counties into groups and query the Census API for the given counties

    Args:
        county_list (list[str]): the list of county fips to be queried
        geography (str): the geography level to be queried (either 'tract' or 'block_group')

    Returns:
        dict: the response JSON
    """
    census_json = {}
        
     #for each group of counties, query the Census API and append the results to the census_json dict
    for county in county_list:
        data = query_census_by_geography(county, geography)
        if data is not None:
            if not census_json:
                census_json = data
            else:
                census_json['features'].extend(data['features'])
   
                
    print (f'Total number of {geography}s returned: {len(census_json["features"])}')
    
    return census_json



def nearest_poly(point, polygons):
    """Find the nearest polygon to the given point

    Args:
        point (Point): the point to be used as reference
        polygons (GeoDataFrame): the GeoDataFrame containing the polygons

    Returns:
        int: the index of the nearest polygon
    """
    distances = polygons.distance(point)
    nearest_poly = distances.idxmin()
    return nearest_poly


def rename_nh_id(row, point_gdf):
    """Rename the nh_id column with the id of the nearest neighborhood
    
    Args:
        row (GeoDataFrame): the GeoDataFrame row to be renamed
        point_gdf (GeoDataFrame): the GeoDataFrame containing the neighborhood points
        
    Returns:
        GeoDataFrame: the GeoDataFrame row with the renamed nh_id column
    """
    distances = point_gdf.distance(row.geometry.centroid)
    nearest_point_idx = distances.idxmin()
    nh_id = point_gdf.loc[nearest_point_idx, 'id']
    row['nh_id'] = nh_id
    return row



def group_and_union(poly_gdf):
    """Group the polygons by nh_id and union them
        Args:
            poly_gdf (GeoDataFrame): the GeoDataFrame containing the polygons
        Returns:
            GeoDataFrame: the GeoDataFrame containing the unioned polygons
    """
    grouped = poly_gdf.groupby('nh_id')
    polygons = []
    for name, group in grouped:
        polygon = group.unary_union
        polygons.append({'geometry': polygon, 'nh_id': name})
    union_gdf = gpd.GeoDataFrame(polygons, crs=poly_gdf.crs)
    return union_gdf




#main method
if __name__ == '__main__':
    args = get_args()
    
    # Read the CSV files into DataFrames
    neighborhood_zhvi_df = pd.read_csv(get_recent_file(args.zhvi_directory, 'Neighborhood'))
    city_zhvi_df = pd.read_csv(get_recent_file(args.zhvi_directory, 'City'))
    city_zori_df = pd.read_csv(get_recent_file(args.zori_directory, 'City'))
    neighborhoods_points_df = pd.read_csv(get_recent_file(args.spatial_directory, 'neighborhoods'), dtype={'county_fips': str})
    
    
    #Merge neighborhood_zhvi into neighborhoods_points_df
    merged_df = merge_dataframes(
        neighborhoods_points_df, 
        neighborhood_zhvi_df[['RegionName', 'State', 'City', neighborhood_zhvi_df.columns[-1]]],
        left_on=['neighborhood', 'state_id', 'city_name'], 
        right_on=['RegionName', 'State', 'City'], 
        suffixes=('_left', '_right'),
        rename_last_col='neighborhood_ZHVI'
    )

    # Merge merged_df and city_zori_df
    merged_df = merge_dataframes(
        merged_df,
        city_zori_df[['RegionName',city_zori_df.columns[-1]]],
        left_on=['city_name'], 
        right_on=['RegionName'], 
        suffixes=('_left', '_right'),
        rename_last_col='city_ZORI'
    )

    # Merge merged_df and city_zhvi_df
    merged_df = merge_dataframes(
        merged_df,
        city_zhvi_df[['RegionName',city_zori_df.columns[-1]]],
        left_on=['city_name'], 
        right_on=['RegionName'], 
        suffixes=('_left', '_right'),
        rename_last_col='city_ZHVI'
    )

    # Convert the merged DataFrame to a GeoDataFrame
    neighborhood_rtv_gdf = gpd.GeoDataFrame(
        merged_df.drop(columns=['neighborhood_ascii', 'city_id', 'timezone', 'source']),  # drop the RegionName column
        crs='EPSG:4326',  # set the CRS to WGS84
        geometry=gpd.points_from_xy(merged_df.lng, merged_df.lat)  # create a Point geometry from latitude and longitude columns
    )
    
    #Add a column to the GeoDataFrame that calculates the ZHVI to ZORI ratio
    neighborhood_rtv_gdf.loc[:, 'city_RTV'] = neighborhood_rtv_gdf['city_ZORI'] / neighborhood_rtv_gdf['city_ZHVI'] * 100
       
    #get a distinct list of county fips from gdf
    counties = neighborhood_rtv_gdf['county_fips'].unique()
    
    #query the Census API for the given geographies in the given counties
    census_json = split_counties_and_query_census(counties, args.census_geography)
        
    #convert the census json to a geo data frame
    census_gdf = gpd.GeoDataFrame.from_features(census_json['features'], crs='EPSG:4326')
    
    #transform the census_gdf and neighborhood_rtv_gdf to 3857
    census_gdf = census_gdf.to_crs(3857)
    neighborhood_rtv_gdf = neighborhood_rtv_gdf.to_crs(3857)
    
    # Find the nearest polygon for each point in neighborhood_rtv_gdf
    neighborhood_rtv_gdf['nearest_poly'] = neighborhood_rtv_gdf.apply(lambda row: nearest_poly(row.geometry, census_gdf), axis=1)
    
    #rename the nh_id column with the id of the nearest neighborhood
    census_gdf = census_gdf.apply(rename_nh_id, axis=1, args=(neighborhood_rtv_gdf,))
    
    # Group and union the polygons by nh_id
    union_gdf = group_and_union(census_gdf)
    
    #transform the union_gdf to 4326
    union_gdf = union_gdf.to_crs(4326)
    
    #drop lat and long columns from neighborhood_rtv_gdf
    neighborhood_rtv_gdf = neighborhood_rtv_gdf.drop(columns=['lat', 'lng'])
    
    #join the union_gdf to the neighborhood_rtv_gdf where the id column of neighborhood_rtv_gdf is equal to the nh_id column of union_gdf
    neighborhood_rtv_gdf = neighborhood_rtv_gdf.merge(union_gdf, left_on='id', right_on='nh_id')
    
    neighborhood_rtv_gdf = neighborhood_rtv_gdf.drop(columns=['nh_id', 'nearest_poly'])
    neighborhood_rtv_gdf = neighborhood_rtv_gdf.drop(columns=['geometry_x'])
    #rename the geometry_y column to geometry
    neighborhood_rtv_gdf = neighborhood_rtv_gdf.rename(columns={'geometry_y': 'geometry'})
    
    #convert the neighborhood_rtv_gdf to geodataframe
    neighborhood_rtv_gdf = gpd.GeoDataFrame(neighborhood_rtv_gdf, crs='EPSG:4326', geometry='geometry')
   
   
    walkscore_df = pd.read_csv(get_recent_file('./csvs', 'walkscores'))
    
    #join walkscore_df to neighborhood_rtv_gdf but retain rows in neighborhood_rtv_gdf if there is no match in walkscore_df
    neighborhood_rtv_gdf = neighborhood_rtv_gdf.merge(walkscore_df, left_on='neighborhood', right_on='neighborhood', how='left')
        
    #remove columns city_name_y, state_id_y
    neighborhood_rtv_gdf = neighborhood_rtv_gdf.drop(columns=['city_name_y', 'state_id_y'])
    
    #rename city_name_x to city_name and state_id_x to state_id
    neighborhood_rtv_gdf = neighborhood_rtv_gdf.rename(columns={'city_name_x': 'city_name', 'state_id_x': 'state_id'})
    
    #add a column to the GeoDataFrame that calculates the area in square miles
    neighborhood_rtv_gdf.loc[:, 'area_sq_mi'] = neighborhood_rtv_gdf['geometry'].to_crs(3857).area / 2.59e+6
    
    #remove the commas from the population column
    neighborhood_rtv_gdf.loc[:, 'population'] = neighborhood_rtv_gdf['population'].str.replace(',', '')
    
    #if a string and not null, convert the population column to an integer
    neighborhood_rtv_gdf.loc[:, 'population'] = neighborhood_rtv_gdf['population'].apply(lambda x: int(x) if isinstance(x, str) else x)
    
    #add a column to the GeoDataFrame that calculates the population density
    neighborhood_rtv_gdf.loc[:, 'pop_density'] = neighborhood_rtv_gdf['population'] / neighborhood_rtv_gdf['area_sq_mi']
    
    #convert the neighborhood_rtv_gdf to a csv file in the directory
    print(f'Converting {len(union_gdf)} neighborhoods to CSV...')
    neighborhood_rtv_gdf.to_csv(os.path.join(args.csv_directory, f'{args.census_geography}_neighborhoods.csv'), index=False)
      
    #convert the neighborhood_rtv_gdf geo data frame to a geojson file in the directory
    print(f'Converting {len(union_gdf)} neighborhoods to GeoJson...')
    convert_to_geojson(neighborhood_rtv_gdf, args.geojson_directory, f'{args.census_geography}_neighborhoods.geojson')
    
