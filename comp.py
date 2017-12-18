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
		WHERE DateKey >= '2017-12-01'
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

	connection.close()
	return df

def dimension_fetch():
	connection = pyodbc.connect(r'DRIVER={SQL Server Native Client 11.0};'
								r'SERVER=SQLDW-L48.BP.Com;'
								r'DATABASE=OperationsDataMart;'
								r'Trusted_connection=yes;'
								)

	cursor = connection.cursor()

	SQLCommand = ("""
		SELECT WellFlac
			  ,API
			  ,NetRevenueInterest
			  ,WorkingInterest
			  ,CurrentWellStatus
			  ,BusinessUnit
			  ,Asset
			  ,Area
			  ,GatheringSite
			  ,BaseOrWedge
			  ,SpudDate
			  ,FirstProductionDate
		FROM OperationsDataMart.Dimensions.Wells
	""")

	cursor.execute(SQLCommand)
	results = cursor.fetchall()
	df = pd.DataFrame.from_records(results)

	try:
		df.columns = pd.DataFrame(np.matrix(cursor.description))[0]
		df.columns = [col.lower().replace(' ', '_') for col in df.columns]
	except:
		pass

	df['wellflac'] = df['wellflac'].astype(int)

	connection.close()
	return df

def List2SQLList(items):
	sqllist = "'{}'".format("\',\'".join(items))
	return sqllist

def plunger_fetch(df):
	# APIs = list(df.api.unique())
	# sql_list = List2SQLList(APIs)
	connection = pyodbc.connect(r'DRIVER={SQL Server Native Client 11.0};'
								r'SERVER=SQLDW-L48.BP.Com;'
								r'trusted_connection=yes;')
	connection.autocommit=True
	cursor = connection.cursor()
	SQLCommandTemp = ("""
		SELECT DISTINCT R.well_id
					   ,R.event_objective_1 as EventObjective
					   ,R.date_ops_start
					   ,MAX(DATE_OPS_START) OVER (PARTITION BY API) AS LATESTDATE
					   ,R.event_code
					   ,R.event_type as EventType
					   ,R.event_objective_2
					   ,left(k.API, 10) as API
					   ,E.well_common_name
					   ,K.WELLNAME
					   ,K.ASSET
		FROM edw.openwells.dm_event R
		LEFT JOIN edw.openwells.cd_well E
			ON R.well_id = E.well_id
		LEFT JOIN OperationsDataMart.DIMENSIONS.WELLS K
			ON LEFT(E.API_NO, 10) = K.API
		WHERE (R.event_objective_1 LIKE '%PLUNGER%'
			OR R.EVENT_OBJECTIVE_1 LIKE '%COMPRESSOR%'
			OR R.EVENT_OBJECTIVE_1 LIKE '%ESP%'
			OR R.EVENT_OBJECTIVE_1 LIKE '%GAS LIFT%'
			OR R.EVENT_OBJECTIVE_1 LIKE '%ROD PUMP%'
			OR R.EVENT_OBJECTIVE_1 LIKE '%COMPRESSION%'
			OR R.EVENT_OBJECTIVE_1 LIKE '%ELECTRIC SUBMERSIBLE PUMP%')
		""")
	cursor.execute(SQLCommandTemp)
	connection.commit()
	# SQLCommandQuery = ("""SELECT DISTINCT *
	#        , CASE WHEN EVENTOBJECTIVE LIKE '%PLUNGER%' THEN 'Plunger'
	#              WHEN EVENTOBJECTIVE LIKE '%GAS LIFT%' THEN 'GasLift'
	#              WHEN EVENTOBJECTIVE LIKE '%ROD PUMP%' THEN 'RodPump'
	#              WHEN EVENTOBJECTIVE LIKE '%ELECTRIC SUBMERSIBLE PUMP%' THEN 'ESP'
	#              WHEN EVENTOBJECTIVE LIKE '%COMPRESSION%' THEN 'Compressor'
	#              END AS PRODUCTIONCATEGORY
	# FROM ##INSTALLSET
	# WHERE API IN (""" + sql_list + """)
	# """)
	# print(SQLCommandQuery)
	# cursor.execute(SQLCommandQuery)
	results = cursor.fetchall()

	df_production = pd.DataFrame.from_records(results)

	try:
		df_production.columns = pd.DataFrame(np.matrix(cursor.description))[0]
		connection.close()
		return df_production
	except:
		connection.close()


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

	# df = df[(df['var_dif'] > .001) & (df['deltagas'] > .001)]

	return df

def get_offset(df):
	o_df = df[['object_code', 'wellname', 'ec_gas', 'odm_gas', 'deltagas', 'var_dif']].copy()
	o_df.rename(columns={'object_code':'wellflac'}, inplace=True)
	o_df = o_df.groupby(['wellflac', 'wellname'], as_index=False).sum()
	o_df = o_df[o_df['var_dif'] < 1]

	return o_df

def dim_link(dims, df):
	linked = df.merge(dims, how='left', on='wellflac')
	return linked

def site_totals(df):
	site_diff = {}
	site_perc = {}
	for site in sorted(df['gatheringsite'].unique()):
		ec = df[df['gatheringsite'] == site]['ec_gas'].sum()
		odm = df[df['gatheringsite'] == site]['odm_gas'].sum()
		# print(site, ' EC: ', ec)
		# print(site, ' ODM: ', odm)
		# print('--------------------------------------------------')
		site_diff[site] = abs(ec - odm)
		site_perc[site] = abs((ec-odm) / ec)
		with open('gat_site.txt', 'a+') as text_file:
			text_file.write('{}\nEC: {}\nODM: {}\n------------------------------------\n'.format(\
							site, ec, odm))

	print('\nAverage site difference: ', np.mean(list(site_diff.values())))
	print('Max site difference: ', np.max(list(site_diff.values())))
	print('----------------------------------------')
	print('Average site percent diff: ', np.mean(list(site_perc.values())))
	return site_diff


if __name__ == '__main__':
	# df = fetch()
	# df.to_csv('data/well_difs.csv')
	# df = pd.read_csv('data/well_difs.csv')
	# df = get_var_wells(df)
	# var_df = manip(df)
	#
	# o_df = get_offset(var_df)
	# o_df.to_csv('data/grouped.csv')

	o_df = pd.read_csv('data/grouped.csv')
	dims = dimension_fetch()

	l_df = dim_link(dims, o_df)
	site_dic = site_totals(l_df)

	# p_df = plunger_fetch(l_df)
	# apis = l_df['api'].unique()
	# df = p_df[p_df['API'].isin(apis)]
