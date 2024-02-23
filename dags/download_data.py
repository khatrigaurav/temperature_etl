import pandas as pd
import requests


def get_temperature():
    ''' 
    This function can be replaced with any API that returns temperature data 
    '''   

    url = 'https://www.metoffice.gov.uk/pub/data/weather/uk/climate/stationdata/bradforddata.txt'
    response = requests.get(url,timeout=5)
    with open('bradforddata.txt', 'wb') as f:
        f.write(response.content)
   
    with open('bradforddata.txt', 'r', encoding='utf-8') as f:
        lines = f.readlines()
   
    data = lines[7:]
    data = [x.split() for x in data]
    df = pd.DataFrame(data, columns=['Year', 'Month', 'Tmax', 'Tmin', 'AF', 'Rain', 'Sun','as'])
    
    df =  df[['Year', 'Month', 'Tmax', 'Tmin']]
    df.to_csv('/opt/ariflow/data/bradford.csv', index=False)

    return 'Data downloaded and saved to data/bradford.csv'

def put_to_s3():
    ''' 
    This function can be replaced with any API that returns temperature data 
    '''   
    
    df = pd.read_csv('/opt/ariflow/data/bradford.csv')
    df.to_csv('s3://weatherdata-bucket1/bradford.csv', index=False)

    return 'Data saved to S3'