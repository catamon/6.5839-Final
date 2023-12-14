# Analyzing and Comparing PostgreSQL, Python Pandas, and Hybrid Performances for Database In-Memory Querying

All commands should be run from FINAL_PROJECT folder.

## calculate_all_table_times.py

### `python3 calculate_all_table_times.py 'table_name'`

Running this command will populate all the raw data of performing 5 iterations of
all possibly queries and all possible hybrid options between Pandas and PostgreSQL for each query in 'table_name'. It will populate analysis_tables/all_times_data/'table_name'\_all.csv with all of the raw data.

### 'table_name' options and adding new options

Initial code and credentials of the database include 'table_name' = 'img1_table', 'electric_vehicles' or 'disease_indicators'. To change the databse, update the psql credentials and edit calculate_all_table_times.py.
to add a new table name, tableParameters - key, value pair in the table_name_to_table_pars dictionary.

## analyze_query_times.py

### `python3 analyze_query_times.py 'table_name'`

Populates analysis_tables/sorted_combinations/'table_name'\_sorted.csv with a file that
gets all the possible combinations from analysis_tables/all_times_data/'table_name'\_all.csv, averages out all the iteration query times for a given query-hybrid combination pair (excluding the first iteration to warm up the cache). And for a given query given in order from best to worst, the hybrid combinations and respective average times for each query.

This assumes that analysis_tables/all_times_data/'table_name'\_all.csv has already been computed using `python3 calculate_all_table_times.py 'table_name'` as specified above.

## compute_results.py

### `python3 compute_results.py`

Computes all of the results for our data analysis part of our investigation for the three databases specifie in the file. It modifies the following files:

- no_single_best_match.csv: All queries that don't have a single best hybrid combination. And specified what the best hybrid combination was for each table.
- resolved_queries_hash: When the second or third best hybrid combination for a query are allowed for a specific query if not all tables converge to same best hybrid combination, populates this file to create a hash between a query and selected hybrid combination that best fits most of the tables.
- unresolved_queries.csv: Populates this file with all queries that are not resolved. i.e are not part of resolved_queries_hash.csv. And maps it to the different hybrid combinations for the different tables.
- not_all_postgresql_queries: Populates this to map all queries which best hybrid combination is not performing everything in Postgresql.

### Change tables to calculate results from

To change tables, edit table_names list in compute_results.py file, with the three corresponding tables you want to base results on. This code assumes that nalysis_tables/sorted_combinations/'table_name'\_sorted.csv has been computed for each 'table_name' in table_names list.
