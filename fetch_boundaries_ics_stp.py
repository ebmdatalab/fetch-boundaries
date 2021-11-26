
# Imports
# -------------------------------------------------------------------------
# Python:
from datetime import datetime
import json
import regex as re

# 3rd party:
import pandas as pd
from pathlib import Path
from urllib.request import urlopen
from urllib import request as urlreq
from bs4 import BeautifulSoup
import geopandas as gpd
from shapely.geometry import Point, Polygon, shape
from shapely import wkb, wkt
import shapely.speedups
shapely.speedups.enable()

#Functions
# ------------------------------------------------------------------------
def geo_json_download(input_num):
    full_url = url_start + data_url + '%s' %input_num  + url_end_base
    with urlopen(full_url) as response:
        geodf_map = gpd.read_file(response)
    return geodf_map

# Ingestion
# -------------------------------------------------------------------------
# Ingest ICS/STP boundary GeoJSON

current_year = datetime.now().strftime('%Y')
last_year = str(datetime.now().year -1)
url_start = "https://ons-inspire.esriuk.com"
search_url = url_start + "/arcgis/rest/services/Health_Boundaries/"
url_end_base = '/query?where=1%3D1&outFields=*&outSR=4326&f=json'
string_filter_base = "Sustainability_and_Transformation_Partnerships_[A-Za-z]+_" 
try:
  response = urlreq.urlopen(search_url)
  soup = BeautifulSoup(response.read(), "lxml")
  data_url = soup.find_all('a', href=re.compile(string_filter_base + current_year))
  if not data_url:
    data_url = soup.find_all('a', href=re.compile(string_filter_base + last_year))
  data_url = data_url[-1].get('href')
  try:
    input_num = '/0'
    df_stp_map = geo_json_download(input_num)
  except:
    input_num = '/1'
    df_stp_map = geo_json_download(input_num)
except:
    print('HTTP error')

# -------------------------------------------------------------------------
# Ingest ICS/STP ONS to ODS code mapping table

url_start_cde_map = "https://services1.arcgis.com"
search_url_cde_map = url_start_cde_map + "/ESMARspQHYMw9BZ9/arcgis/rest/services/"
string_filter_cde_map_base = "STP_[A-Za-z]+_"
string_filter_cde_map_end = '_EN_NC'
url_2_cde_map = '/0/query?where=1%3D1&outFields=*&outSR=4326&f=json'
try:
  response_cde_map = urlreq.urlopen(search_url_cde_map)
  soup_cde_map = BeautifulSoup(response_cde_map.read(), "lxml")
  data_url_cde_map = soup_cde_map.find_all('a', href=re.compile(string_filter_cde_map_base + current_year + string_filter_cde_map_end))
  if not data_url_cde_map:
    data_url_cde_map = soup_cde_map.find_all('a', href=re.compile(string_filter_cde_map_base + last_year + string_filter_cde_map_end))
  data_url_cde_map = data_url_cde_map[-1].get('href')
  full_url_cde_map = url_start_cde_map + data_url_cde_map + url_2_cde_map
  with urlopen(full_url_cde_map) as response:
    stp_code_map_json = json.load(response)
    stp_code_map_df = pd.json_normalize(stp_code_map_json['features'])
except:
  print('HTTP error')

# Processing
# -------------------------------------------------------------------------
# Prepare ICS/STP geopandas dataframe

column_mapping = {df_stp_map.columns[0]: 'Index', df_stp_map.columns[1]: 'ONS ICS/STP code', df_stp_map.columns[2]: 'ICS/STP name'}
df_stp_map_1 = df_stp_map.rename(columns=column_mapping)
df_stp_map_2 = df_stp_map_1.set_index('Index')

# -------------------------------------------------------------------------
# Prepare ICS/STP ONS to ODS code mapping dataframe
column_ons_code = stp_code_map_json['fields'][0]['name'].lower()
column_ods_code = stp_code_map_json['fields'][1]['name'].lower()
stp_code_map_df_1 = stp_code_map_df.iloc[:,:2]
stp_code_map_df_1.columns = stp_code_map_df_1.columns.str.lower()
stp_code_map_df_1.rename(columns={'attributes.%s' %column_ons_code :'ONS ICS/STP code', 'attributes.%s' %column_ods_code: 'ods_code'}, inplace=True)

# -------------------------------------------------------------------------
#Join geometery and code mapping table (for ICS/STP), select relevant columns

final_map_df = stp_code_map_df_1.merge(df_stp_map_2, how = 'outer', on = 'ONS ICS/STP code')
final_map_df_1 = final_map_df[['ods_code', 'geometry']]
gdf = gpd.GeoDataFrame(final_map_df_1)
gdf.to_file("ics_stp_boundaries.TAB", driver="MapInfo File")

