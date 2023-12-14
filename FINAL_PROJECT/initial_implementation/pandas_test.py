# import modin.config as config
import pandas as pd
import psycopg2

import time
import resource

import ray

# ray.init(memory=1000000000) 

# memory_limit = 1 * 1024 * 1024 * 700
# resource.setrlimit(resource.RLIMIT_AS, (memory_limit, memory_limit))

# config.Engine.put("dask")

# Replace with your PostgreSQL database details
db_params = {
    "dbname": "",
    "user": "postgres",
    "password": "databases65830",
    "host": "database-1.cwntxvdkxsdc.us-east-2.rds.amazonaws.com",
    "port": "5432"
}

connection = psycopg2.connect(**db_params)

query_1 = "SELECT * FROM electric_vehicles"
query_2 = "SELECT * FROM vehicle_transactions"

# query_1 = "SELECT * FROM big_transactions"

# query_1 = "SELECT * FROM electric_vehicles WHERE electric_range IS NOT NULL AND electric_range < 80"
# query_1 = "SELECT * FROM electric_vehicles ORDER BY electric_range, legislative_district DESC"
# query_1 = "SELECT COUNT(dol_id) FROM electric_vehicles GROUP BY state"
# query_1 = "SELECT SUM(legislative_district) FROM electric_vehicles"

query_1 = "SELECT * FROM electric_vehicles AS ev JOIN big_transactions AS vt ON ev.dol_id = vt.dol_id"

# query_1 = "EXPLAIN SELECT COUNT(*) FROM electric_vehicles AS ev JOIN vehicle_transactions AS vt ON ev.dol_id = vt.dol_id"

start_time = time.time()

data_frame_1 = pd.read_sql(query_1, connection)
# data_frame_2 = pd.read_sql(query_2, connection)

connection.close()

df1 = pd.DataFrame(data_frame_1)
# df2 = pd.DataFrame(data_frame_2)

# # filter on data frame
# filtered_df = df1[df1["electric_range"] != None]
# filtered_df = filtered_df[df1["electric_range"] < 80]

# # sort on data frame
# df1 = df1.sort_values(by=['electric_range', 'legislative_district'], ascending=[True, False])
# df1 = df1.sort_values(by=['year_start'], ascending=[False])

# # group by and count
# gk = df1.groupby('state')
# gk.count().reset_index()[['state', 'dol_id']].to_csv('state_grouped.csv')

# # sum on data frame
# df1[["legislative_district"]].sum(axis=0).to_csv("table_pandas_example.csv", index=False)

# # join on data frame
# df_joined = df1.join(df2, on="dol_id", rsuffix="_trans")
# df1 = df1.join(df2, on="dol_id", rsuffix="_trans")
# df1 = df1.count()

# write frame to csv file
# query_plan = df1['QUERY PLAN'].iloc[0]
# query_plan_elements = query_plan.split()
# num_rows = 0

# for element in query_plan_elements:
#     print("element: ", element)
#     if "rows" in element:
#         element_els = element.split('=')
#         print("element els: ", element_els[-1])
#         num_rows = element_els[-1]

# print("num rows: ", num_rows)
df1.to_csv("table_pandas_example.csv", index=False)

end_time = time.time()


# Calculate the elapsed time
elapsed_time = end_time - start_time

print(f"Elapsed Time: {elapsed_time} seconds")