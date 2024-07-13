import json
import pandas as pd
from time import sleep
from datetime import datetime
from random import uniform


def gerar_registro() -> dict[str,str]:
    power_factor = uniform(0.7,1)
    hydraulic_pressure = uniform(70,80)
    temperature = uniform(20,25)
    register = {
        'powerfactor': str(power_factor),
        'hydraulicpressure': str(hydraulic_pressure),
        'temperature': str(temperature),
        'timestamp': str(datetime.now())
    }
    return register

temperature_id = 1
while True:
    # Gera uma lista com todos os windturbines
    windturbines_list = ['json_windturbine','csv_windturbine','parquet_windturbine']
    # Gera o registro
    register = gerar_registro() 
    
    count = temperature_id
    for current_windturbine_name in windturbines_list:

        register['idtemp'] = str(count)
        register['name'] = current_windturbine_name
        
        match current_windturbine_name:
            case 'json_windturbine':
                with open('data/data.json','w') as json_file:
                    json_file.write(json.dumps(register))
            case 'csv_windturbine':
                df = pd.DataFrame([register])
                df.to_csv('data/data.csv',index=False)
            case 'parquet_windturbine':
                df = pd.DataFrame([register])
                df.to_parquet('data/data.parquet',index=False)

        count+=1

    sleep(5)
    temperature_id += 3
    