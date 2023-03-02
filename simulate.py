import sys
import datetime as dt
from src.simulator import Simulator
import json

def read_params(file):
    # Read execution parameters
    with open(file) as json_params:
        params = json.load(json_params)
    return params

if __name__ == "__main__":
    
    params = read_params('params.json')
    default_input_date = params.get('default_input_date')
    minimum_input_date = params.get('minimum_input_date')
    maximum_input_date = params.get('maximum_input_date')
    
    if len(sys.argv) == 1:
        input_date = default_input_date
    else:
        input_date = str(sys.argv[1])
        
    input_date = dt.datetime.strptime(input_date, '%d/%m/%Y').date()
    if input_date.year != 2022:
        print(f'Invalid date. Please input a date between {minimum_input_date} and {maximum_input_date}')
    else:
        simulator = Simulator()
        simulator.get_cases(input_date)