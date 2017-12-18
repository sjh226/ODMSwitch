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
	# SQLCommand = ("""
	#     SELECT R.EVENT_OBJECTIVE_1
	#     	  ,K.WellFlac
	#     	  ,K.WellName
	#     	  ,G.Diff
	#     	  ,G.EC_Gas
	#     	  ,G.ODM_Gas
	#     	  ,G.Delta
	#     FROM edw.openwells.dm_event r
	#     left join
	#     edw.openwells.cd_well e
	#     on r.well_id=e.well_id
	#     left join
	#     OperationsDataMart.DIMENSIONS.WELLS K
	#     ON LEFT(E.API_NO, 10)=K.API
	#     LEFT JOIN (
	#     	SELECT W.[OBJECT_Code]
	#     			,COUNT(DISTINCT (PA.[ALLOC_GAS_VOL] - AD.GAS)) AS Diff
	#     			,SUM(PA.ALLOC_GAS_VOL) AS EC_Gas
	#     			,SUM(AD.GAS) AS ODM_Gas
	#     			,SUM(PA.ALLOC_GAS_VOL) - SUM(AD.GAS) AS Delta
	#     	FROM [EDW].[EC].[PWEL_DAY_ALLOC] PA
	#     	INNER JOIN [EDW].[EC].[WELL] W
	#     		ON W.[Object_ID] = PA.[Object_ID]
	#     	INNER JOIN OperationsDataMart.Reporting.AllData AD
	#     		ON AD.WellFlac = W.[OBJECT_Code]
	#     		AND AD.DateKey = CAST([DAYTIME] AS DATE)
	#     	WHERE AD.DateKey >= '2017-12-01'
	#     	GROUP BY W.OBJECT_CODE
	#     	HAVING SUM(PA.ALLOC_GAS_VOL) - SUM(AD.GAS) != 0
	#     		AND SUM(PA.ALLOC_GAS_VOL) - SUM(AD.GAS) >= 0.1*SUM(AD.GAS)
	#     		AND SUM(AD.GAS) != 0
	#     	--HAVING COUNT(DISTINCT (PA.[ALLOC_GAS_VOL] - AD.GAS)) = 1
	#     	) AS G
	#     ON G.OBJECT_CODE = K.WellFlac
	#     WHERE (r.event_objective_1 like '%PLUNGER%' 
	#     	OR R.EVENT_OBJECTIVE_1 LIKE '%COMPRESSOR%' 
	#     	OR R.EVENT_OBJECTIVE_1 LIKE '%ESP%' 
	#     	OR R.EVENT_OBJECTIVE_1 LIKE '%GAS LIFT%' 
	#     	OR R.EVENT_OBJECTIVE_1 LIKE '%ROD PUMP%' 
	#     	OR R.EVENT_OBJECTIVE_1 LIKE '%COMPRESSION%' 
	#     	OR R.EVENT_OBJECTIVE_1 LIKE '%ELECTRIC SUBMERSIBLE PUMP%')
	#     	AND G.Diff IS NOT NULL
	#     	AND G.Diff < 10
	#     ORDER BY G.Delta DESC, K.WellFlac;
	# """)

	SQLCommand = ("""
		SELECT AD.API,
			   W.OBJECT_Code,
			   CAST([DAYTIME] AS DATE) AS DateKey,
			   PA.ALLOC_GAS_VOL EC_GAS,
			   AD.GAS ODM_GAS,
			   AD.WellName WellName,
			   PA.ALLOC_GAS_VOL - AD.GAS DeltaGas
		FROM EDW.EC.PWEL_DAY_ALLOC AS PA
		INNER JOIN EDW.EC.WELL AS W
			ON W.Object_ID = PA.Object_ID
		INNER JOIN OperationsDataMart.Reporting.AllData AD
			ON AD.WellFlac = W.OBJECT_Code
			AND AD.DateKey = CAST([DAYTIME] AS DATE)
		WHERE DateKey >= '2017-11-01'
		ORDER BY DateKey Desc
	""")

	cursor.execute(SQLCommand)
	results = cursor.fetchall()
	df = pd.DataFrame.from_records(results)

	try:
		df.columns = pd.DataFrame(np.matrix(cursor.description))[0]
		df.columns = [col.lower().replace(' ', '_') for col in df.columns]
	except:
		pass

	# Close the connection after pulling the data
	connection.close()
	return df

def get_var_wells(df):
	flacs = df[df['deltagas'] > 0.0001]['object_code']
	return df[df['object_code'].isin(flacs)]

def manip(df):
	var_dic = {}
	for flac in df['object_code'].unique():
		ec_var = np.var(df[df['object_code'] == flac]['ec_gas'])
		odm_var = np.var(df[df['object_code'] == flac]['odm_gas'])
		# print('EC: ', ec_var)
		# print('ODM: ', odm_var)
		var_dic[flac] = abs(ec_var - odm_var)

	df['var_dif'] = df['object_code'].map(var_dic)

	df = df[(df['var_dif'] > .001) & (df['deltagas'] > .001)]

	return df

def get_offset(df):
	o_df = df[df['var_dif'] < 1]
	o_df = o_df[['object_code', 'wellname', 'ec_gas', 'odm_gas', 'deltagas', 'var_dif']]
	o_df = o_df.groupby(['object_code', 'wellname'], as_index=False).sum()

	return o_df


if __name__ == '__main__':
	df = fetch()
	df.to_csv('data/well_difs.csv')
	df = pd.read_csv('data/well_difs.csv')
	df = get_var_wells(df)
	var_df = manip(df)

	o_df = get_offset(var_df)
	o_df.to_csv('data/grouped.csv')
