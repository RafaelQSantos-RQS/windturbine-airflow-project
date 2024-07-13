import json
import pandas as pd
from time import sleep
from datetime import datetime
from random import uniform,randint


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
    # Gera uma lista com todos os windmills
    windmills_list = ['json_windmill','csv_windmill','parquet_windmill']
    # Gera o registro
    register = gerar_registro() 
    
    count = temperature_id
    for current_windmill_name in windmills_list:

        register['idtemp'] = str(count)
        register['name'] = current_windmill_name
        
        match current_windmill_name:
            case 'json_windmill':
                with open('data/data.json','w') as json_file:
                    json_file.write(json.dumps(register))
            case 'csv_windmill':
                df = pd.DataFrame([register])
                df.to_csv('data/data.csv',index=False)
            case 'parquet_windmill':
                df = pd.DataFrame([register])
                df.to_parquet('data/data.parquet',index=False)

        count+=1

    sleep(5)
    temperature_id += 3
    