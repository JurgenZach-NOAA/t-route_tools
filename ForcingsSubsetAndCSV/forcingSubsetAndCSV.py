import pandas as pd
import geopandas as gpd
import xarray as xr
import glob

from joblib import delayed, Parallel

def convert_and_write(file, df_x):
    ds = xr.open_dataset(file)[['qSfcLatRunoff','qBucket','q_lateral','crs']]
    ids = list(set(ds.feature_id.values).intersection(set(df_x.hf_id.values)))
    ds_sub = ds.sel(feature_id=ids)
    ds_sub.to_netcdf('subset_nhd/' + file.split('/')[-1])

    df = ds[['qSfcLatRunoff','qBucket']].to_dataframe().drop(['latitude','longitude'], axis=1)
    df['q_lateral'] = df['qSfcLatRunoff'] + df['qBucket']
    df = df.drop(['qSfcLatRunoff','qBucket'], axis=1)

    new_df = df_x.set_index('hf_id').join(df).reset_index()
    temp_df = pd.DataFrame(data=new_df.value_counts('index'))
    temp_df.columns = ['qcount']

    new_df = new_df.set_index('index').join(temp_df)
    new_df['q_lateral_adj'] = new_df.q_lateral/new_df.qcount
    new_df = new_df.drop(['id','q_lateral','qcount'],axis=1).groupby('toid').sum().rename(columns={'q_lateral_adj': file[-28:-16]})
    new_df.index.name = 'feature_id'
    new_df.to_csv('subset_Hy/' + file[-28:-16] + '.CHRTOUT_DOMAIN1.csv')

df2 = gpd.read_file('geopackage/Connecticut_26Jan2024_NGEN201.gpkg', layer='network')

df2 = df2[['id','toid','hf_id']].dropna()
df2['id'] = df2['id'].str.split('-',expand=True).loc[:,1].astype(float).astype(int)
df2['toid'] = df2['toid'].str.split('-',expand=True).loc[:,1].astype(float).astype(int)
df2['hf_id'] = df2.hf_id.astype(int)
df2 = df2.drop_duplicates()

flist = glob.glob('forcingFetch/*.CHRTOUT_DOMAIN1')

print(flist)

with Parallel(n_jobs=4, backend="loky") as parallel:
    jobs = []

    for f in flist:
        jobs.append(
            delayed(convert_and_write)
            (f,
             df2)
            )
        
    parallel(jobs)
