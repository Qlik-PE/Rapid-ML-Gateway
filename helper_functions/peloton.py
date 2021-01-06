from datetime import datetime
from dateutil import tz
import requests, json



# Anonymous Endpoints
#intructors = "https://api.onepeloton.com/api/instructor"
# instructor_ = https://api.onepeloton.com/api/instructor/<instructor id>
metadata_mapping = "https://api.onepeloton.com/api/ride/metadata_mappings"

# Privileged Endpoints
# Essential information about the logged in user
my_detail = "https://api.onepeloton.com/api/me"

# User details and workouts completed
user_detail = "https://api.onepeloton.com/api/user/"
# "<user id>


# Workout specific info
# https://api.onepeloton.com/api/user/<user id>/workouts
# https://api.onepeloton.com/api/user/<user id>/workouts?joins=ride,ride.instructor&limit=10&page=0

# Note that workouts, in this context, are what a user did. So, a workout can either be a tread or bike workout.

workout = "https://api.onepeloton.com/api/workout/"
#    <workout id>
# https://api.onepeloton.com/api/workout/<workout id>?joins=ride,ride.instructor&limit=1&page=0
# https://api.onepeloton.com/api/workout/<workout id>/performance_graph?every_n=5

def get_instructors(url):
    s = requests.get(url)
    result = json.loads(s.text)
    instructors = result["data"]
    return_val = []
    for x in instructors:
        instructor_list_elem = []
        id = x['id']
        name = x['name']
        fitness_disciplines =  ', '.join([str(elem) for elem in x['fitness_disciplines']]) 
        instructor_list_elem = [id, name, fitness_disciplines]
        return_val.append((instructor_list_elem))
    return return_val

def get_instructor(url, instructor_id):
    s = requests.get(url+"/"+instructor_id)
    result = json.loads(s.text)
    return result
    
def get_session(user_name, password):
    s = requests.Session()
    payload = {'username_or_email': user_name, 'password': password}
    r = s.post('https://api.onepeloton.com/auth/login', json=payload)
    return s, r


def user_workout(s, user_id, option):
    url = user_detail + user_id + '/workouts' + option
    r = s.get(url)
    return r


def workout_detail(s, workout_id, option):
    url = workout + workout_id + option
    print(url)
    r = s.get(url)
    return r
