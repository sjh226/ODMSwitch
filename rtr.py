import pandas as pd
import numpy as np
import pyodbc

def fetch():
	connection = pyodbc.connect(r'DRIVER={SQL Server Native Client 11.0};'
								r'SERVER=SQLDW-L48.BP.Com;'
								r'DATABASE=OperationsDataMart;'
								r'Trusted_connection=yes;'
								)
	cursor = connection.cursor()

	SQLCommand = ("""
		SELECT WellFlac
              ,API
		FROM OperationsDataMart.Dimensions.Wells
	""")

	cursor.execute(SQLCommand)
	results = cursor.fetchall()
	df = pd.DataFrame.from_records(results)

	try:
		df.columns = pd.DataFrame(np.matrix(cursor.description))[0]
	except:
		pass

	connection.close()
	return df

def data_link(wells, rtr, meter):
    df = pd.DataFrame(columns=['meter1_id', 'sheet_api', 'sheet_flac', 'true_api', 'true_wellflac'])

    df['meter1_id'] = rtr['Meter1ID']
    df['sheet_api'] = rtr['API']
    df['sheet_flac'] = rtr['WellFlac']

    meter.dropna(inplace=True)
    meter['METER_NUMBER'] = meter['METER_NUMBER'].str.rstrip('_CHK')
    # meter['METER_NUMBER'] = meter['METER_NUMBER'].str.lstrip('ABCDEFGHIJKLMNOPQRSTUVWXYZ')

    api_dic = {}
    for met in df['meter1_id'].str.lstrip('W').unique():
        api = meter[meter['METER_NUMBER'] == met]['API NUMBER'].unique()
        if len(api) == 0:
            api_dic[met] = np.nan
        elif api[0].lower() in ['na', 'n']:
            api_dic[met] = np.nan
        else:
            api = api[0]
            if len(api) > 10:
                api = api[:10]
            api_dic[met] = api

    df['true_api'] = df['meter1_id'].map(api_dic)

    flac_dic = {}
    for api in df['true_api'].unique():
        flac = wells[wells['API'] == str(api)]['WellFlac'].unique()
        if len(flac) == 0:
            flac_dic[api] = np.nan
        else:
            flac_dic[api] = flac[0]

    df['true_wellflac'] = df['true_api'].map(flac_dic)

    df['match'] = np.where(df['sheet_api'].str[:10] == df['true_api'], True, False)

    return df


if __name__ == '__main__':
    well_df = fetch()
    rtr_df = pd.read_csv('data/AllWellsInRTR.csv')
    meter_df = pd.read_csv('data/meter_api.csv')

    df = data_link(well_df, rtr_df, meter_df)
