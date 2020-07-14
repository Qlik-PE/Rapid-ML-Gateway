
#
def divide_chunks(l, n): 
      
    # looping till length l 
    for i in range(0, len(l), n):  
        yield l[i:i + n]    

def find_key(input_dict, value):
    return next((k for k, v in input_dict.items() if v == value), None)