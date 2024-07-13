import json
import pandas as pd
from time import sleep
from datetime import datetime
from random import uniform,randint

id = {
    'json_windmill':0,
    'csv_windmill':0,
    'parquet_windmill':0
}

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

while True:
    windmills = id.keys() # Gera uma lista com todos os windmills
    current_windmill = windmills[randint(0,len(windmills)-1)] # Escolhe um windmill aleatório da lista
    register = gerar_registro() # Gera o registro
    id = windmills[current_windmill] + 1
    windmills[current_windmill] = id
    register['idtemp'] = str(id)
    register['name'] = current_windmill
    
    print(register)
    sleep(15)
    