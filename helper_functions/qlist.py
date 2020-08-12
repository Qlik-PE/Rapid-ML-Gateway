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