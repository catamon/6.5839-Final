import csv
# import decision_tree

ind_to_operator = ['filtering_low', 'filtering_high', 'joining', 'sorting', 'agregating', 'grouping']

ind_to_activation = ['pandas_filtering_low', 'pandas_filtering_high', 'pandas_joining', 'pandas_sorting', 'pandas_agregating', 'pandas_grouping']

def list_encode_to_str(encoding_list):
    encoding_num = ''
    for el in encoding_list:
        if el is None:
            encoding_num += '?'
        else:
            encoding_num += str(el)
    print(len(encoding_num))
    return encoding_num

def str_encode_to_list(str_encode):
    l = [int(val) for val in str_encode]
    return l

class queryStore:
    def __init__(self, query_key):
        self.query_key = query_key
        self.best_act_keys = set()
        self.act_to_best = {}
        self.all = {}
    
    def __setitem__(self, table_name, sorted_key_times):
        if table_name in self.act_to_best or table_name in self.all:
            print(self.all, self.act_to_best, self.best_act_keys, self.query_key)
            # print(table_name)
            raise KeyError('cannot set a table val more than once')
        best_act_key = sorted_key_times[0]
        self.best_act_keys.add(best_act_key)
        self.act_to_best[table_name] = best_act_key
        self.all[table_name] = sorted_key_times


def understand_query_speed(table_names):

    min_accom = [0 for _ in range(6)]
    worst_accom = min_accom.copy()
    total_query = min_accom.copy()

    for table_ind, table_name in enumerate(table_names):
        with open('analysis_tables/average_max_min/'+ table_name + '_sorted.csv','r') as file:
            reader = list(csv.reader(file))
        for row in reader:
            query_operators = row[0]
            best_pandas = row[2]
            worst_pandas = row[4]
            for i, val in enumerate(best_pandas):
                min_accom[i] += int(val)
                worst_accom[i] += int(worst_pandas[i])
                total_query[i] += int(query_operators[i])
                if i == 5 and int(val) == 1:
                    print(table_name, query_operators, best_pandas)
                # if i == 4 and int(val) == 1:
                #     print(query_operators, best_pandas, table_ind)
                # if i == 4 and int(val) == 0 and int(query_operators[5]) == 0:
                #     print('here')

    
    print(f'{min_accom}')
    print(f'{worst_accom}')
    print(f'{total_query}')

def get_estimated_table_output(postgres_query):

    data_frame_1 = pd.read_sql(query, connection)
    df1 = pd.DataFrame(data_frame_1)

    query_plan = df1['QUERY PLAN'].iloc[0]
    query_plan_elements = query_plan.split()
    num_rows = 0

    for element in query_plan_elements:
        if "rows" in element:
            element_els = element.split('=')
            num_rows = element_els[-1]

    return num_rows


def dict_from_file_names(file_names):
    best_mappings = {}
    for i, table_name in enumerate(file_names):
        with open('analysis_tables/sorted_combinations/'+ table_name + '_sorted.csv','r') as file:
            reader = list(csv.reader(file))
            for row in reader:
                query_key = row[0]
                if i == 0:
                    best_mappings[query_key] = queryStore(query_key)
                best_mappings[query_key][table_name] = row[1:]
    return best_mappings


def find_odd(exception):
    bests = exception.best_act_keys
    if len(bests) != 2:
        return False, None
    mappings = exception.act_to_best

    counts = {best: 0 for best in bests}
    for i, act in enumerate(mappings.values()):
        counts[act] += 1
        if counts[act] == 2:
            best = act
    

    for table_name, act_key in mappings.items():
        if act_key != best:
            sorted_table = exception.all[table_name]
            if sorted_table[2] == best:
                if float(sorted_table[3]) - float(sorted_table[1]) < 0.1:
                    return True, best
            if sorted_table[4] == best:
                if float(sorted_table[5]) - float(sorted_table[1]) < 0.1:
                    return True, best
            return False, None

def get_exceptions(file_names):
    best_mappings = dict_from_file_names(file_names)
    all_exceptions = [q_store for q_store in best_mappings.values() if len(q_store.best_act_keys) != 1]
    save_data = []
    restricted_exceptions = []
    figured_exceptions = []
    for exception in all_exceptions:
        save_data.append([[exception.query_key] + list(exception.act_to_best.items())])
    
    save_data = [['query_key', 'combination by table']] + save_data
    csv_file_path = f"analysis_tables/results/no_single_best_match.csv"
    with open(csv_file_path, "w", newline="") as csv_file:
        csv_writer = csv.writer(csv_file)
        csv_writer.writerows(save_data)


def query_to_act(query_key, best_mappings):
    if query_key not in best_mappings:
        return
    q_store = best_mappings[query_key]
    if len(q_store.best_act_keys) == 1:
        for el in q_store.best_act_keys:
            return el
    can_delete, chosen_act = find_odd(q_store)
    if can_delete: return chosen_act
    return q_store.act_to_best



def generate_all_t_f_vals(num_arr):
    if num_arr == 1: return [[True], [False]]
    if num_arr == 0: return []

    prev_opts = generate_all_t_f_vals(num_arr -1)
    all_opts = []
    for new in [True, False]:
        new_opts = [[new] + opt for opt in prev_opts.copy()]
        all_opts.extend(new_opts)
    return all_opts

def turn_tf_to_key(tf):
    key = ''
    for el in tf:
        if el: key += '1'
        else: key += '0'
    return key

def run_all(file_names):
    resolved = []
    invalid_queries = []
    unresolved = []
    all_t_f = generate_all_t_f_vals(6)
    best_mappings = dict_from_file_names(file_names)
    for q_list in all_t_f:
        q = turn_tf_to_key(q_list)
        act_key = query_to_act(q, best_mappings)
        if type(act_key) == str:
            resolved.append([q, act_key])
        elif act_key is None:
            invalid_queries.append(q)
        else:
            unresolved.append([q, act_key])
    
    resolved_save = [['query', 'hybrid_combination']] + resolved
    csv_file_path = f"analysis_tables/results/resolved_queries_hash.csv"
    with open(csv_file_path, "w", newline="") as csv_file:
        csv_writer = csv.writer(csv_file)
        csv_writer.writerows(resolved_save)

    unresolved_save = [['query', 'possible_combinations']] + unresolved
    csv_file_path = f"analysis_tables/results/unresolved_queries.csv"
    with open(csv_file_path, "w", newline="") as csv_file:
        csv_writer = csv.writer(csv_file)
        csv_writer.writerows(unresolved_save)
    
    return resolved, unresolved

    

def get_not_all_postgresql(unresolved):
    csv_file_path = 'analysis_tables/results/resolved_queries_hash.csv'
    none_postgresql_queries = unresolved.copy()
    with open(csv_file_path,'r') as file:
            reader = list(csv.reader(file))
            for i, row in enumerate(reader):
                if not i:
                    continue
                if row[1] != '000000':
                    none_postgresql_queries.append([row[0], row[1]])

    none_postgresql_queries = [['query', 'best_combination']] + none_postgresql_queries
    csv_file_path = f"analysis_tables/results/not_all_postgresql_queries.csv"
    with open(csv_file_path, "w", newline="") as csv_file:
        csv_writer = csv.writer(csv_file)
        csv_writer.writerows(none_postgresql_queries)



if __name__ == '__main__':

    #UPDATE TO ADD/REMOVE TABLES TO INCLUDE IN ANALYSIS
    table_names = ['img1_table', 'electric_vehicles', 'disease_indicators']
    
    get_exceptions(table_names)
    resolved, unresolved = run_all(table_names)
    get_not_all_postgresql(unresolved)

    


    ''''
    - no best query has any group by or filter low done by pandas
    - 

    Pseudo code for decision tree
    if group by or filter low in query:
        do all query on psql

    '''