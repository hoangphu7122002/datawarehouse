import csv

def convert(value):
    for type in [int, float]:
        try:
            return type(value)
        except ValueError:
            continue
    # All other types failed it is a string
    return value


def construct_string_sql(file_path, table_name):
    string_SQL = ''
    try:
        with open(file_path, 'r') as file:            
            reader = csv.reader(file)
            headers = ','.join(next(reader))
            for row in reader:
                row = [convert(x) for x in row].__str__()[1:-1]
                string_SQL += f'INSERT INTO {table_name}({headers}) VALUES ({row});\n'
    except:
        return ''

    return string_SQL

with open('insert.sql','a') as f:
	f.write(construct_string_sql('inventory.csv','inventory'))
	f.write(construct_string_sql('order_detail.csv','order_detail'))
	f.write(construct_string_sql('order.csv','orders'))
	f.write(construct_string_sql('product.csv','product'))
	f.write(construct_string_sql('users.csv','user'))
	f.write(construct_string_sql('user_detail.csv','user_details'))