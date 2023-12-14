import csv
import sys

class generalQueryStore:
    def __init__(self, query_key):
        self.activation_data = {}
        self.query_key = query_key

    def __setitem__(self, active_key, active_store):
        if active_key in self.activation_data: raise KeyError('there shouldnt be more than one data for each activation key')
        self.activation_data[active_key] = active_store

    def __getitem__(self, active_key):
        if active_key not in self.activation_data: raise KeyError('no active key in activation data')
        return self.activation_data[active_key]

    def __contains__(self, active_key):
        if active_key in self.activation_data: return True
        return False

    def __iter__(self):
        for q_store in self.activation_data.values():
            yield q_store

    def calculate_min_max(self):
        min_time = float('inf')
        max_time = -float('inf')
        min_q_store = None
        max_q_store = None
        for q_store in self:
            q_store_ave = q_store.average
            if min_time >= q_store_ave:
                min_time = q_store_ave
                min_q_store = q_store
            if max_time <= q_store_ave:
                max_time = q_store_ave
                max_q_store = q_store
        self.min_time = min_time
        self.max_time = max_time
        self.min_q_store = min_q_store
        self.max_q_store = max_q_store
        return min_time, max_time, min_q_store, max_q_store
    
    def create_average_order_array(self):
        all_q_stores = self.activation_data.values()
        sorted_q_stores = sorted(all_q_stores, key=lambda q_store: q_store.average)
        ret_val = [[q_store.activation_key, q_store.average] for q_store in sorted_q_stores]
        self.all_activations_in_order = ret_val
        return ret_val

class queryActivatedStore:
    def __init__(self, activation_key, psql_query, pandas_query):
        self.activation_key = activation_key
        self.iteration_times = {}
        self.psql_query = psql_query
        self.pandas_query = pandas_query
        self.average = None
    
    def __setitem__(self, iteration, time):
        # print(self.activation_key, iteration, time)
        if iteration in self.iteration_times: raise KeyError('multiple iterations should not be set')
        self.iteration_times[iteration] = time

    def __getitem__(self, iteration):
        if iteration not in self.iteration_times: raise KeyError('iteration not in activation store')
        return self.iteration_times[iteration]
    
    def set_average(self):
        if len(self.iteration_times) != 5: print("WARNING: setting average for uncomplete iteration")
        total = 0
        for iteration, time in self.iteration_times.items():
            if iteration != 0: total += time
        
        self.average = total/float((len(self.iteration_times) - 1))


def unpack_row(row):
    query_key_list = row[:6]
    active_key_list = row[6:12]
    psql_query, pandas_query, iteration, time = row[12:]

    query_key = ''
    active_key = ''
    for qk, ak in zip(query_key_list, active_key_list):
        if qk == 'True': query_key += '1'
        else: query_key += '0'
        if ak == 'True': active_key += '1'
        else: active_key += '0'
    
    
    return query_key, active_key, psql_query, pandas_query, int(iteration), float(time)


def create_data_dictionary(reader):
    all_dict = {}
    is_head = True
    for row in reader:
        if is_head:
            is_head = False
            continue
            
        query_key, active_key, psql_query, pandas_query, iteration, time = unpack_row(row)
        is_grouping = int(query_key[5])
        is_agregating = int(query_key[4])
        if int(is_grouping) == 1 and int(is_agregating) == 1 and active_key[4] != active_key[5]:
                continue
        if int(active_key[2]) == 1 and is_grouping == 1 and int(active_key[5]) == 0:
            #group by join exception
            continue
        
        if int(active_key[3]) == 1 and is_grouping == 1 and int(active_key[5]) == 0:
            #group by join exception
            continue
        
        if int(active_key[2]) == 1 and int(query_key[3]) == 1 and int(active_key[3]) == 0:
            # sort join exception
            continue
        if query_key not in all_dict:
            all_dict[query_key] = generalQueryStore(query_key)
        if active_key not in all_dict[query_key]:
            all_dict[query_key][active_key] = queryActivatedStore(active_key, psql_query, pandas_query)
        all_dict[query_key][active_key][iteration] = time
    
    for query, general_query_store in all_dict.items():
        for q_store in general_query_store:
            q_store.set_average()

    
    return all_dict


def flaten(l):
    flat_l = []
    for el in l:
        flat_l.extend(el)
    return flat_l


def generate_averages_sorted(reader, table_name):
    all_dict = create_data_dictionary(reader)
    data_to_save = []
    for general_query_store in all_dict.values():
        averages = general_query_store.create_average_order_array()
        flat_av = flaten(averages)
        new_row = [general_query_store.query_key] + flat_av
        data_to_save.append(new_row)
    save_to_csv(data_to_save, table_name)


def save_to_csv(rows, table_name):
    all = rows
    csv_file_path = f"analysis_tables/{table_name}.csv"
    with open(csv_file_path, "w", newline="") as csv_file:
        csv_writer = csv.writer(csv_file)
        csv_writer.writerows(all)

    

if __name__ == '__main__':
    table_name = sys.argv[1]
    file_name = f'analysis_tables/all_times_data/{table_name}_all.csv'
    with open(file_name,'r') as file:
        csv_reader = csv.reader(file)
        generate_averages_sorted(csv_reader, f'sorted_combinations/{table_name}_sorted')
