from multiprocessing import Pool
import json
import requests
import time
import pandas as pd

def query_to_df_local(kwargs, last_sort=None):
    # query with search_after
    word = kwargs['word']
    start = kwargs['start']
    end = kwargs['end']
    q_size = kwargs['q_size']
    url = kwargs['url']

    id, nombre, urls, region, country, size, anyo = [], [], [], [], [], [], []
    start-=1
    flag = True
    while True:
        if flag:
            query = {
                    'size': q_size,
                    'query': {
                                'match': {
                                            'content': word
                                        }
                            },
                    "search_after": [start+1],
                    "sort": [
                                {"_doc": {"order": "asc"}}
                            ],
                    "size": q_size
                    }
            
            flag = False
        else:
            query = {
                'size': q_size,
                'query': {
                            'match': {
                                        'content': word
                                    }
                        },
                "search_after": [last_sort+1],
                "sort": [
                            {"_doc": {"order": "asc"}}
                        ],
                "size": q_size
                }      
        query = json.dumps(query) 
        r = requests.get(url, 
                        data=query,
                        headers={'Content-type': 'application/json'})
        result = r.json()
        last_sort = int(result['hits']['hits'][-1]['sort'][0])
        if last_sort < end:
             p = True
        else: p = False
        print(start, end, last_sort, p)
        
        n_results = len(result['hits']['hits'])
        if n_results == 0 or last_sort > end:
            break

        for i in range(n_results):
            try: #Se comprueba que el año sea un número
                    year = result['hits']['hits'][i]['_source']['year'] #Se obtiene el año de la empresa
                    year = int(year) #Se comprueba que el año sea un número
                    if year > 1900:
                        id.append(result['hits']['hits'][i]['_id']) #Se obtiene el id de la empresa
                        nombre.append(result['hits']['hits'][i]['_source']['name']) #Se obtiene el nombre de la empresa
                        urls.append(result['hits']['hits'][i]['_source']['url']) #Se obtiene la url de la empresa
                        region.append(result['hits']['hits'][i]['_source']['region']) #Se obtiene la región de la empresa
                        country.append(result['hits']['hits'][i]['_source']['country']) #Se obtiene el país de la empresa
                        size.append(result['hits']['hits'][i]['_source']['size']) #Se obtiene el tamaño de la empresa
                        anyo.append(year) #Se obtiene el año de la empresa
                    else:pass
            except:
                    pass
        if n_results < q_size: #Si el número de resultados es menor que 10000, se sale del bucle, ya que 10000 es el número máximo de resultados que se pueden obtener en una query
            break

    data = pd.DataFrame({'id': id, 'nombre': nombre, 'url': urls, 'region': region, 'country': country, 'size': size, 'anyo': anyo})
    data['anyo'] = pd.to_numeric(data['anyo'], downcast ='signed')
    return data

if __name__ == '__main__':
    word = 'salud'
    n_jobs = 4
    url = 'http://localhost:9200/indice_prueba/_search'
    q_size = 10000
    search_size = 300000/n_jobs
    start = [i*search_size for i in range(n_jobs)]
    end = [(i+1)*search_size for i in range(n_jobs)]
    p = [(word, url, start[i], end[i], q_size) for i in range(n_jobs)]
    variables = ['word', 'url', 'start', 'end', 'q_size']
    params = [dict(zip(variables, p[i])) for i in range(n_jobs)]

    start_time = time.time()
    with Pool(processes=n_jobs) as pool:
        results = pool.map(query_to_df_local, params)
    end_time = time.time()
    print('Time: ', round(end_time-start_time,2))
    
    # Concat results
    data = pd.concat(results, ignore_index=True)
    print(len(data))