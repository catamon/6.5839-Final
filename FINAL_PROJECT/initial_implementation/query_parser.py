import sqlparse
import pandas as pd

# import psycopg2

# from pandas_profiling import ProfileReport


def is_pandas_div(div):
    better_with_pandas = {"SORT BY"}
    for q in better_with_pandas:
        if q in div:
            return True
    return False


def parse_sql_query(sql_query):
    parsed = sqlparse.parse(sql_query)

    py_Q = []
    psql_Q = []
    for statement in parsed:
        python_query = ""
        psql_query = ""
        formatted_sql = sqlparse.format(
            str(statement), reindent=True, keyword_case="upper"
        )
        query_divided = formatted_sql.split("\n")
        for div in query_divided:
            if not is_pandas_div(div):
                psql_query += " " + div
            else:
                python_query += " " + div
        py_Q.append(python_query)
        psql_Q.append(psql_query)
    return py_Q, psql_Q


# def do_psql_query(query):
#     db_params = {
#         "dbname": "",
#         "user": "postgres",
#         "password": "databases65830",
#         "host": "database-1.cwntxvdkxsdc.us-east-2.rds.amazonaws.com",
#         "port": "5432",
#     }

#     connection = psycopg2.connect(**db_params)
#     data_frame = pd.read_sql(query, connection)
#     connection.close()

#     df = pd.DataFrame(data_frame)
#     return df


def create_db(query):
    df = pd.read_csv("tables_65830/k_table.csv")
    df.columns = df.columns.astype(str)
    return df


def post_processing(df, pandas_query):
    parameters_parsed = get_parameters_to_sort(pandas_query)[::-1]
    new_df = df
    for p, is_desc in parameters_parsed:
        new_df = new_df.sort_values(by=p, ascending=not is_desc)
    return new_df


def get_parameters_to_sort(raw_query):
    modified_query = raw_query.replace(" ", "")
    modified_query = modified_query.replace("SORTBY", "")
    modified_query = modified_query.replace(";", "")
    parameter_list = modified_query.split(",")
    print(parameter_list)
    parameter_parsed = []
    for p in parameter_list:
        new_p = p
        is_desc = False
        if p[-4:].lower() == "desc":
            is_desc = True
            new_p = p[:-4]
        elif p[-3:].lower() == "asc":
            new_p = p[:-3]
        parameter_parsed.append((new_p, is_desc))
    print(parameter_parsed)
    return parameter_parsed


if __name__ == "__main__":
    # Example SQL query
    sql_query = (
        "SELECT * FROM employees WHERE department = 'IT' SORT BY k_val DESC, k_key;"
    )

    # Parse the SQL query
    py_Q, psql_Q = parse_sql_query(sql_query)

    df = create_db(psql_Q[0])
    df = post_processing(df, py_Q[0])

    df.to_csv("k_table_query.csv", index=False)
