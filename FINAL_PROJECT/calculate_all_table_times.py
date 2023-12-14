import sys
import pandas as pd
import psycopg2
import time
import resource
import csv
import ray

db_params = {
    "dbname": "",
    "user": "postgres",
    "password": "databases65830",
    "host": "database-1.cwntxvdkxsdc.us-east-2.rds.amazonaws.com",
    "port": "5432"
}

def generate_all_t_f_vals(num_arr):
    if num_arr == 1: return [[True], [False]]
    if num_arr == 0: return []

    prev_opts = generate_all_t_f_vals(num_arr -1)
    all_opts = []
    for new in [True, False]:
        new_opts = [[new] + opt for opt in prev_opts.copy()]
        all_opts.extend(new_opts)
    return all_opts

reverse_direction = {'>': '<=', '>=': '<', '<': ">=", '<=': '>'}

class tableParameters:
    def __init__(self, name, join_par, join_table= None, filter_par= None, sort_par= None, group_par = None, filter_low_value = None, filter_high_value = None):
        self.name = name
        self.join_table = join_table
        self.filter_par = filter_par
        self.join_par = join_par
        self.sort_par = sort_par
        self.group_par = group_par
        # self.low_selectivity_direction = low_selectivity_direction
        # self.selectivity_threshold = selectivity_threshold
        self.filter_low_value = filter_low_value
        self.filter_high_value = filter_high_value


def get_query(filtering_low, filtering_high, joining, sorting, agregating, grouping,
            pandas_filtering_low, pandas_filtering_high, pandas_joining, pandas_sorting, 
            pandas_agregating, pandas_grouping, table):
    query = 'SELECT '
    pandas_query = set()

    # agregation
    if not grouping:
        # no need to be concerned with special returns
        if agregating: 
            if not pandas_agregating:
                #agregation done in postgresql
                query += 'COUNT(*) '
            else:
                #agregation done in pandas, so postgresql should ret everything
                query += '* '  
                pandas_query.add('AGREGGATE')
        else:
            # not agregating so return everything
            query += '* '
    
    else:
        #doing grouping so have to be careful
        if not pandas_grouping:
            #doing grouping in postgresql
            if agregating:
                if not pandas_agregating:
                    query += f'{table.name}.{table.group_par}, COUNT(*) '
                else:
                    query += f'{table.name}.{table.group_par} '
                    pandas_query.add('AGREGGATE')

            else:
                #not aggregating so just ret a line with group par name
                query += f'{table.name}.{table.group_par} '
        else:
            # it is doing pandas grouping
            if agregating: 
                if not pandas_agregating:
                    raise NameError('cannot do grouping in pandas if aggregate is done in postgresql')
                    #agregation done in postgresql
                    query += 'COUNT(*) '
                else:
                    #agregation done in pandas, so postgresql should ret everything
                    query += '* '  
                    pandas_query.add('AGREGGATE')
            else:
                # not agregating so return everything
                query += '* '

    query += f'FROM {table.name} '

    # join
    if joining: 
        if not pandas_joining:
            # query += f'JOIN {table.join_table.name} USING ({table.join_par}) ' 
            query += f'JOIN {table.join_table.name} ON {table.name}.{table.join_par} = {table.join_table.name}.{table.join_table.join_par} '
        else:
            pandas_query.add('JOIN')

    if filtering_low:
        if not pandas_filtering_low:
            # query += f'WHERE {table.name}.{table.filter_par} {table.low_selectivity_direction} {table.selectivity_threshold} '
            query += f'WHERE {table.name}.{table.filter_par} = \'{table.filter_low_value}\' '
        else:
            pandas_query.add('FILTER_LOW')
    
    if filtering_high:
        if not pandas_filtering_high:
            # dir = reverse_direction[table.low_selectivity_direction]
            # query += f'WHERE {table.name}.{table.filter_par} {dir} {table.selectivity_threshold} '
            query += f'WHERE {table.name}.{table.filter_par} != \'{table.filter_low_value}\' '
        else:
            pandas_query.add('FILTER_HIGH')

    if grouping:
        if not pandas_grouping:
            query += f'GROUP BY {table.name}.{table.group_par} '
        else:
            pandas_query.add('GROUPING')
    
    if sorting:
        if not pandas_sorting:
            query += f'ORDER BY {table.name}.{table.sort_par} '
        else:
            pandas_query.add('SORTING')
    
    
    query += ';'

    return query, pandas_query

def perform_query(query, pandas_query, table, entire_df):
    start_time = time.time()

    data_frame_1 = pd.read_sql(query, connection)
    df1 = pd.DataFrame(data_frame_1)



    if 'FILTER_LOW' in pandas_query:
        df1 = df1[df1[table.filter_par] == table.filter_low_value]

    if 'FILTER_HIGH' in pandas_query:
        df1 = df1[df1[table.filter_par] != table.filter_low_value]

    if 'JOIN' in pandas_query:
        query_2 = f"SELECT * FROM {table.join_table.name}"
        data_frame_2 = pd.read_sql(query_2, connection)
        df2 = pd.DataFrame(data_frame_2)
        df1 = df1.join(df2.set_index(table.join_table.join_par), on=table.join_par, rsuffix='_trans')

    
    if 'SORTING' in pandas_query:
        df1 = df1.sort_values(by=[table.sort_par], ascending=[True])

    if 'GROUPING' and 'AGREGGATE' in pandas_query:
        df1 = df1.groupby([table.group_par], as_index=False).count()
    else:
        if 'GROUPING' in pandas_query:
            grouped = df1.groupby([table.group_par], as_index=False)
            df1 = pd.concat([group.reset_index(drop=True) for name, group in grouped], keys=grouped.groups.keys(), names=[table.group_par])

        if 'AGREGGATE' in pandas_query:
            df1 = df1.count()

    if (not df1.equals(entire_df)):
        raise NameError('wrong output') 

    df1.to_csv("overall_rewritten.csv", index=False)

    end_time = time.time()
    elapsed_time = end_time - start_time
    return elapsed_time

def do_overall_queries_on_table(table_pars, doing_grouping = False):
    if not doing_grouping:
        t_f_vals = generate_all_t_f_vals(5)
        for el in t_f_vals:
            el.append(False)
    else:
        t_f_vals = generate_all_t_f_vals(6)
    all_values = []

    query_num = 0
    for i, options in enumerate(t_f_vals):
        if len(options) != 6:
            raise NameError('each option should have 6 args')
        if not doing_grouping and options[5]:
            raise NameError('cannot have grouping par set to true while also not specified')
        # print(f'Processing query {i} of {len(t_f_vals)}')
        if options[0] and options[1]:
            # ignore cause we cant filter in both low and high
            continue
        if options[4] and options[3]:
            # ignore both agregation and sorting
            continue

        active_options = len([op for op in options if op == True])
        
        args_full = options + [False, False, False, False, False, False] + [table_pars]
        entire_query, pandas_query = get_query(*args_full)
        entire_data_frame_1 = pd.read_sql(entire_query, connection)
        entire_df = pd.DataFrame(entire_data_frame_1)
        # for option in options: 
        #     if option: active_options += 1
        
        # print(active_options)
        
        pandas_t_f_vals = generate_all_t_f_vals(active_options)
        for j, pandas in enumerate(pandas_t_f_vals):

            pandas_args = []
            pandas_indx = 0
            pandas_active = 0
            for indx in range(6):
                if (options[indx]):
                    pandas_args.append(pandas[pandas_indx])
                    if (pandas[pandas_indx]):
                        pandas_active += 1
                    pandas_indx += 1
                else:
                    pandas_args.append(False)
            
            if (pandas_active > 0 and options[4] and not pandas_args[4]):
                # ignore pandas extra operands cause postgrest agregated
                continue
            
            if options[5] and not pandas_args[5] and pandas_args[2]:
                #doing groupby in postgresql and joining in pandas
                if table_pars.group_par != table_pars.join_par:
                    #ignore cause table wont have the grouped column name
                    continue
            
            if not doing_grouping and pandas_args[5]: raise NameError('cannot set active to true for grouping if its not being done')
            if len(pandas_args) != 6 or len(options) != 6: raise NameError('wrong lengths of parameters')

            print(f'Processing query {query_num}')

            args = options + pandas_args + [table_pars]
            query, pandas_query = get_query(*args)

            print(query)
            print(pandas_query)

            query_num += 1

            for iteration in range(5):
                try:
                    time = perform_query(query, pandas_query, table_pars, entire_df)
                    new_value = args[:-1] + [query, pandas_query, iteration, time]
                    all_values.append(new_value)
                except NameError as e:
                    continue

    print('Saving to CSV')
    print(all_values)
    save_to_csv(all_values, table_pars.name)
    print('DONE!')


def save_to_csv(rows, table_name):
    all = [['filtering_low', 'filtering_high', 'joining', 'sorting', 'agregating', 'grouping', 'filter_low_pandas', 'filtering_high_pandas', 'join_pandas', 'sort_pandas', 'agregate_pandas', 'grouping_pandas','query', 'pandas_query','iteration', 'time']] + rows
    csv_file_path = f"analysis_tables/all_times_data/{table_name}_all.csv"
    with open(csv_file_path, "w", newline="") as csv_file:
        csv_writer = csv.writer(csv_file)
        csv_writer.writerows(all)



if __name__ == '__main__':


    vehicle_transactions_pars = tableParameters('vehicle_transactions', 'dol_id')
    electric_vehicles_pars = tableParameters(name = 'electric_vehicles', join_par = 'dol_id',  join_table = vehicle_transactions_pars, filter_par = 'state', sort_par = 'state', group_par = 'state', filter_low_value = 'NY', filter_high_value = 'WA')

    national_obesity_pars = tableParameters('national_obesity', 'national_location')
    disease_indicators_pars = tableParameters(name = 'disease_indicators', join_par = 'location',  join_table = national_obesity_pars, filter_par = 'location', sort_par = 'location', group_par = 'location', filter_low_value = 'Texas')
    
    means_pars = tableParameters('positions_means', join_par = 'img_name_means')
    img1_table_pars = tableParameters(name = 'img1_table', join_par = 'img_name',  join_table = means_pars, filter_par = 'img_name', sort_par = 'img_name', group_par = 'img_name', filter_low_value = '0-0-0-0')
    
   #ADD more table pars if adding more datasets
   
   #update dictionary to include new dataframes if added
    table_name_to_table_pars = {
        'electric_vehicles': electric_vehicles_pars, 
        'disease_indicators': disease_indicators_pars, 
        'img1_table': img1_table_pars}
   
    table_name = sys.argv[1]
    table_pars = table_name_to_table_pars[table_name]

    
    
    connection = psycopg2.connect(**db_params)

    do_overall_queries_on_table(table_pars, True)
        
    connection.close()



