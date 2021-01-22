import glom

#
def divide_chunks(l, n): 
      
    # looping till length l 
    for i in range(0, len(l), n):  
        yield l[i:i + n]    

def find_key(input_dict, value):
    return next((k for k, v in input_dict.items() if v == value), None)

def get_table_id(input_dict, value):
    keys = input_dict.keys()
    id = "Not Found"
    for k in keys:
        path = k + ".name"
        #print(path)
        table_name = glom.glom(input_dict, path)
        #print(table_name)
        if(table_name == value):
            id = k
            #path = k +".description.table.steps"
    return id

def convert_list_of_dicts(input_list):
    result = []
    for x in input_list:
        columns = list(x.keys())
        values = list(x.values())
        values = ["NA" if x == '' else x for x in values]
        temp = [str(x) for x in values]
        result.append(temp)
    return columns, result

def convert_dicts_list(input_dict):
    columns = list(input_dict.keys())
    values = list(input_dict.values())
    values = ["NA" if x == '' else x for x in values]
    temp = [str(x) for x in values]
    return columns, temp
            
def convert_df_list(input_df):
    columns = list(input_df.columns)
    values = list(input_df.values)
    values = ["NA" if x == '' else x for x in values]
    temp = [str(x) for x in values]
    return columns, temp