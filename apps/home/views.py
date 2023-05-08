# -*- encoding: utf-8 -*-
"""
Copyright (c) 2019 - present AppSeed.us
"""

from django import template
from django.contrib.auth.models import User
from django.contrib.auth import authenticate, login, logout
from django.contrib.auth.decorators import login_required
from django.http import HttpResponse, HttpResponseRedirect, JsonResponse
from django.apps import apps
from django.conf import settings
from django.template import loader
from django.urls import reverse
import plotly.express as px
from django.shortcuts import render, get_object_or_404
import pandas as pd
import json
import requests
import sqlite3
from datetime import date
from urllib.request import urlopen
from django.views.decorators.csrf import csrf_exempt,csrf_protect
import time
from multiprocessing import Pool





# @login_required(login_url="/login/")
def index(request):
    context = {'segment': 'index'}

    html_template = loader.get_template('home/home.html')
    return HttpResponse(html_template.render(context, request))


@login_required(login_url="/login/")
def pages(request):
    context = {}
    # All resource paths end in .html.
    # Pick out the html file name from the url. And load that template.
    try:

        load_template = request.path.split('/')[-1]

        if load_template == 'admin':
            return HttpResponseRedirect(reverse('admin:home'))
        context['segment'] = load_template

        html_template = loader.get_template('home/' + load_template)
        return HttpResponse(html_template.render(context, request))

    except template.TemplateDoesNotExist:

        html_template = loader.get_template('home/page-404.html')
        return HttpResponse(html_template.render(context, request))

    except:
        html_template = loader.get_template('home/page-500.html')
        return HttpResponse(html_template.render(context, request))
    
def paises(pais):
    lista_paises_excel = ['España', 'Italia', 'Francia', 'Alemania']
    lista_paises = ['Spain', 'Italy', 'France', 'Germany']
    if pais in lista_paises:
        return lista_paises_excel[lista_paises.index(pais)]

def query_to_df(request, word, q_size = 10000):
    endpoint = 'https://search-btrends-elastic-nydmwkmzsz7dlvneik4e5t5nle.eu-west-1.es.amazonaws.com/'
    username = 'btrends_elastic'
    password = 'P4HKg3QE6tXNgFq!'

    id, nombre, urls, region, country, size, anyo, sector = [], [], [], [], [], [], [], []
    flag = True

    while True:
        """
        Si es la primera vez que se ejecuta el bucle, se ejecuta la query sin el search_after ya que no hay ningún valor, 
        esto se hace porque para que funcione search_after hay que tener un sort, que se le da a partir de la siguiente ejecucion de la query
        """
        if flag:
            query = {
                'size': q_size,
                'query': {
                    'match': {
                        'content': word
                    }
                },
                "sort": [
                {"_doc": {"order": "asc"}}
                ],
                "size": q_size
            }
            
            flag = False
    
        else: #Si no es la primera vez que se ejecuta el bucle, se ejecuta la query con el search_after
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

        url = endpoint + 'btrends_index/_search'
        r = requests.get(url, 
                        auth=(username, password),
                        data=query,
                        headers={'Content-type': 'application/json'})
        result = r.json()

        last_sort = int(result['hits']['hits'][-1]['sort'][0])
        n_results = result['hits']['total']['value']
        if n_results == 0:
                break

        l_sectores = ['B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'L', 'M', 'N']
        for i in range(n_results):
            try: #Se comprueba que el año sea un número y que el sector esté en la lista de sectores
                    year = result['hits']['hits'][i]['_source']['year'] #Se obtiene el año de la empresa
                    year = int(year) #Se comprueba que el año sea un número
                    sectores = result['hits']['hits'][i]['_source']['section'] #Se obtiene el sector de la empresa
                    if year > 1900 and sectores in l_sectores:
                        id.append(result['hits']['hits'][i]['_id']) #Se obtiene el id de la empresa
                        nombre.append(result['hits']['hits'][i]['_source']['name']) #Se obtiene el nombre de la empresa
                        urls.append(result['hits']['hits'][i]['_source']['url']) #Se obtiene la url de la empresa
                        region.append(result['hits']['hits'][i]['_source']['region']) #Se obtiene la región de la empresa
                        country.append(paises(result['hits']['hits'][i]['_source']['country'])) #Se obtiene el país de la empresa
                        size.append(result['hits']['hits'][i]['_source']['size']) #Se obtiene el tamaño de la empresa
                        anyo.append(year) #Se obtiene el año de la empresa
                        sector.append(sectores) #Se obtiene el sector de la empresa
                    else:pass
            except:
                    pass
        if n_results < q_size: #Si el número de resultados es menor que 10000, se sale del bucle, ya que 10000 es el número máximo de resultados que se pueden obtener en una query
            break

    data = pd.DataFrame({'id': id, 'nombre': nombre, 'url': urls, 'region': region, 'country': country, 'sector':sector, 'size': size, 'anyo': anyo})
    data['anyo'] = pd.to_numeric(data['anyo'], downcast ='signed')
    # if request.user.is_authenticated:
    #     historial(request, data, word.lower())
    return data

def query_to_df_local(request, word, url = 'http://localhost:9200/indice_prueba/_search', q_size = 10000):
    # # query with search_after
    id, nombre, urls, region, country, sector, size, anyo = [], [], [], [], [], [], [], []
    flag = True

    while True:
        """
        Si es la primera vez que se ejecuta el bucle, se ejecuta la query sin el search_after ya que no hay ningún valor, 
        esto se hace porque para que funcione search_after hay que tener un sort, que se le da a partir de la siguiente ejecucion de la query
        """
        if flag:
            query = {
                'size': q_size,
                'query': {
                    'match': {
                        'content': word
                    }
                },
                "sort": [
                {"_doc": {"order": "asc"}}
                ],
                "size": q_size
            }
            
            flag = False
    
        else: #Si no es la primera vez que se ejecuta el bucle, se ejecuta la query con el search_after
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
        n_results = len(result['hits']['hits'])

        if n_results == 0:
            break
        
        l_sectores = ['B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'L', 'M', 'N']
        for i in range(n_results):
            try: #Se comprueba que el año sea un número y que el sector esté en la lista de sectores
                    year = result['hits']['hits'][i]['_source']['year'] #Se obtiene el año de la empresa
                    year = int(year) #Se comprueba que el año sea un número
                    sectores = result['hits']['hits'][i]['_source']['section'] #Se obtiene el sector de la empresa
                    if year > 1900 and sectores in l_sectores:
                        id.append(result['hits']['hits'][i]['_id']) #Se obtiene el id de la empresa
                        nombre.append(result['hits']['hits'][i]['_source']['name']) #Se obtiene el nombre de la empresa
                        urls.append(result['hits']['hits'][i]['_source']['url']) #Se obtiene la url de la empresa
                        region.append(result['hits']['hits'][i]['_source']['region']) #Se obtiene la región de la empresa
                        country.append(paises(result['hits']['hits'][i]['_source']['country'])) #Se obtiene el país de la empresa
                        size.append(result['hits']['hits'][i]['_source']['size']) #Se obtiene el tamaño de la empresa
                        anyo.append(year) #Se obtiene el año de la empresa
                        sector.append(sectores) #Se obtiene el sector de la empresa
                    else:pass
            except:
                    pass
        if n_results < q_size: #Si el número de resultados es menor que 10000, se sale del bucle, ya que 10000 es el número máximo de resultados que se pueden obtener en una query
            break

    data = pd.DataFrame({'id': id, 'nombre': nombre, 'url': urls, 'region': region, 'country': country, 'sector':sector, 'size': size, 'year': anyo})
    data['year'] = pd.to_numeric(data['year'], downcast ='signed')
    if request.user.is_authenticated:
        historial(request, data, word.lower())
    return data
    

def historial(request, data, word):
    connection = sqlite3.connect('db.sqlite3')
    cursor = connection.cursor()
    # Compruebo si el usuario ya ha buscado esa palabra
    cursor.execute(f"SELECT search FROM historial WHERE user_id={request.user.id}")
    d = cursor.fetchall()
    for i in d:
        if i[0] == word:
            return
    date_ = date.today().strftime("%d/%m/%Y")
    # Introduzco el usuario, la palabra, el año, el país y la fecha en la tabla historial
    insertData = f"INSERT INTO historial VALUES ('{request.user.id}','{word}', '{str(data['year'])}', '{str(data['country'])}', '{str(date_)}')"
    cursor.execute(insertData)
    connection.commit()
    connection.close()

def extract_historial(request):
    connection = sqlite3.connect('db.sqlite3')
    cursor = connection.cursor()
    # Extraigo los datos de la tabla historial
    cursor.execute(f"SELECT search, date FROM historial WHERE user_id={request.user.id}")
    data = cursor.fetchall()
    l_data = [i for i in data]
    connection.close()
    return l_data

def prepare_data(data, dimensiones):

    sector_dic={'Sector': ['B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'L', 'M', 'N'],
                    'España': [0.0007261846208517991,0.0643924163370203,0.004264403305427586,0.0025788015157908366,0.15455077535107198,0.26728781080352293,
                    0.07538524756310613,0.10397205329621233,0.025519098765890806,0.06843315183622808,0.1575629332414137,0.0753271233634635],
                    'Italia': [0.0005386136232901886,0.10174987941995876,0.0034002888455605365,0.0026228379281274636,0.1331547974460259,0.2920928186216574,
                    0.03266933468510505,0.09158344480440073,0.029658181202083952,0.06462188421856618,0.20498388394802966,0.042924035257194154],
                    'Francia': [0.0006767511643633294,0.08386318853023743,0.011248898473071941,0.003137761451942984,0.1431978021449178,0.26229364373156616,
                    0.049951011253440876,0.09541013353785333,0.03454466290487431,0.06504887385122192,0.1845025540917925,0.06612471886471742],
                    'Alemania': [0.0007654552489480005,0.0854472698337332,0.0260820032682277,0.004211644911910652,0.14188783363765559,0.2275003017695181,
                    0.041798451512111444,0.09067490251294691,0.04845670874664817,0.06209158549887151,0.1909608450859341,0.0801229979734946]}
    size_dic = {'Size': ['micro', 'small', 'medium', 'large'],
                    'España': [0.9537956757802714,0.03886485645345999,0.005985321064467463,0.0013541467018011511],
                    'Italia': [0.9428353593968402,0.05029683868508319,0.0058807535125341755,0.0009870484055424463],
                    'Francia': [0.9301303416748424,0.057868458731412296,0.00994212506714554,0.0020590745265996867],
                    'Alemania': [0.8937599898474159,0.0844436810556937,0.01796030062443498,0.003836028472455463]}
    country_dic = {'España': [0.13274381643009436],
                   'Italia': [0.1960125004509701],
                   'Francia': [0.27507745686306084],
                   'Alemania': [0.3961662262558747]}

    df_sector = pd.DataFrame(sector_dic)
    index_sector = ['B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'L', 'M', 'N']
    df_sector.index = index_sector
    df_sector = df_sector.drop(columns=['Sector'])

    df_size = pd.DataFrame(size_dic)
    index_size = ['micro', 'small', 'medium', 'large']
    df_size.index = index_size
    df_size = df_size.drop(columns=['Size'])

    df_country = pd.DataFrame(country_dic)
    index_country = ['Weight']
    df_country.index = index_country

    dimensiones_aceptadas = ['country', 'sector', 'size']
    dimensiones = [x.lower() for x in dimensiones]
    dimensiones.sort()

    for i in dimensiones:
        if i not in dimensiones_aceptadas:
            return 'Error of dimensions. Accepted dimensions: country, sector, size'
    
    if len(dimensiones) == 2:
        if dimensiones[0] == 'country' and dimensiones[1] == 'size':
            data['country_size'] = data['country'] + '_' + data['size']
            data_grouped = data.groupby(by=["anyo","country","sector","country_size"]).size().reset_index(name="counts")
            data_grouped['weight'] = [df_sector.loc[data_grouped.iloc[row, 2], data_grouped.iloc[row, 1]]*data_grouped.iloc[row, 4] for row in range(data_grouped.shape[0])]
            data_grouped = data_grouped.groupby(['anyo','country_size']).sum(numeric_only=True).reset_index()

        elif dimensiones[0] == 'country' and dimensiones[1] == 'sector':
            data['country_sector'] = data['country'] + '_' + data['sector']
            data_grouped = data.groupby(by=["anyo","country","size","country_sector"]).size().reset_index(name="counts")
            data_grouped['weight'] = [df_size.loc[data_grouped.iloc[row, 2], data_grouped.iloc[row, 1]]*data_grouped.iloc[row, 4] for row in range(data_grouped.shape[0])]
            data_grouped = data_grouped.groupby(['anyo','country_sector']).sum(numeric_only=True).reset_index()
        
        elif dimensiones[0] == 'sector' and dimensiones[1] == 'size':
            data['size_sector'] = data['size'] + '_' + data['sector']
            data_grouped = data.groupby(by=["anyo","country","size","size_sector"]).size().reset_index(name="counts")
            data_grouped['weight'] = [df_country.loc['Weight', data_grouped.iloc[row, 1]]*data_grouped.iloc[row, 4] for row in range(data_grouped.shape[0])]
            data_grouped = data_grouped.groupby(['anyo','size_sector']).sum(numeric_only=True).reset_index()

    elif len(dimensiones) == 1:
        if dimensiones[0] == 'size':
            data_grouped = data.groupby(by=["anyo","country","sector","size"]).size().reset_index(name="counts")
            data_grouped['weight'] = [df_sector.loc[data_grouped.iloc[row, 2], data_grouped.iloc[row, 1]]*data_grouped.iloc[row, 4] for row in range(data_grouped.shape[0])]
            data_grouped = data_grouped.groupby(['anyo','country','size']).sum(numeric_only=True).reset_index()
            data_grouped['weight'] = [df_country.loc['Weight', data_grouped.iloc[row, 1]]*data_grouped.iloc[row, 4] for row in range(data_grouped.shape[0])]
            data_grouped = data_grouped.groupby(['anyo','size']).sum(numeric_only=True).reset_index()
        elif dimensiones[0] == 'sector':
            data_grouped = data.groupby(by=["anyo","country","sector","size"]).size().reset_index(name="counts")
            data_grouped['weight'] = [df_size.loc[data_grouped.iloc[row, 3], data_grouped.iloc[row, 1]]*data_grouped.iloc[row, 4] for row in range(data_grouped.shape[0])]
            data_grouped = data_grouped.groupby(['anyo','country','sector']).sum(numeric_only=True).reset_index()
            data_grouped['weight'] = [df_country.loc['Weight', data_grouped.iloc[row, 1]]*data_grouped.iloc[row, 4] for row in range(data_grouped.shape[0])]
            data_grouped = data_grouped.groupby(['anyo','sector']).sum(numeric_only=True).reset_index()
        elif dimensiones[0] == 'country':
            data_grouped = data.groupby(by=["anyo","country","sector","size"]).size().reset_index(name="counts")
            data_grouped['weight'] = [df_sector.loc[data_grouped.iloc[row, 2], data_grouped.iloc[row, 1]]*data_grouped.iloc[row, 4] for row in range(data_grouped.shape[0])]
            data_grouped = data_grouped.groupby(['anyo','country','size']).sum(numeric_only=True).reset_index()
            data_grouped['weight'] = [df_size.loc[data_grouped.iloc[row, 2], data_grouped.iloc[row, 1]]*data_grouped.iloc[row, 4] for row in range(data_grouped.shape[0])]
            data_grouped = data_grouped.groupby(['anyo','country']).sum(numeric_only=True).reset_index()
    else: return 'Error of len of dimensions. Accepted dimensions: country, sector, size and 1 >= len <= 2'

    return data_grouped
    

def f_lineplot(data):

    data_grouped = data.groupby(by=["anyo"]).size().reset_index(name="counts")
    fig = px.line(data_grouped, x = 'anyo', y = 'counts', title='Número de empresas por año', markers = True, hover_data = {'anyo': True, 'counts': True}, labels = {'anyo': 'Año', 'counts': 'Número de empresas'})
    # fig.update_layout(width=800, height=400)
    fig.update_xaxes(title = "Año", showgrid=False)
    fig.update_yaxes(title = "Número de empresas", showgrid=False)
    # fig.update_traces(line=dict(color='red'),textposition="bottom right")
    fig.update_layout(plot_bgcolor='rgba(0,0,0,0)', paper_bgcolor='rgba(0,0,0,0)', font=dict(color='#04a9f5'))
    fig.update_traces(line=dict(width = 4), marker = dict(size = 10))

    chart = fig.to_html()

    return chart

def f_multilineplot(data):
    dimensiones = ['country']
    data_grouped = prepare_data(data,dimensiones)
    fig = px.line(data_grouped, x = 'anyo', y = 'weight',color = 'country',title='Número de empresas por países', markers = True, hover_data = {'anyo':False}, labels = {'country':'País','weight': 'Número de empresas'})
    fig.update_xaxes(title = "Año", showgrid=False)
    fig.update_yaxes(title = "Número de empresas", showgrid=False)
    # fig.update_traces(line=dict(color='red'),textposition="bottom right")
    fig.update_traces(line=dict(width = 4), marker = dict(size = 10)) 
    fig.update_layout(plot_bgcolor='rgba(0,0,0,0)', paper_bgcolor='rgba(0,0,0,0)', font=dict(color='#04a9f5'),hovermode="x unified")

    chart = fig.to_html()

    return chart

def f_map(data):
    with urlopen('https://raw.githubusercontent.com/leakyMirror/map-of-europe/master/GeoJSON/europe.geojson') as response:
        counties = json.load(response)  

    dimensiones = ['country']
    data_grouped = prepare_data(data,dimensiones)
    data_grouped = data_grouped.groupby(by=["country"]).size().reset_index(name="counts")
    data_grouped['country'] = data_grouped['country'].replace(['España','Francia','Italia','Alemania'],['Spain','France','Italy','Germany'])
    
    # data_grouped = data_grouped.sort_values('anyo')

    # totals_by_country = data_grouped.groupby("country")["counts"].sum().reset_index(name="total_counts")

    # # Filtrar los datos por país y asignar los valores a variables
    # total_counts_spain = totals_by_country.loc[totals_by_country["country"] == "Spain", "total_counts"].iloc[0]
    # total_counts_italy = totals_by_country.loc[totals_by_country["country"] == "Italy", "total_counts"].iloc[0]
    # total_counts_france = totals_by_country.loc[totals_by_country["country"] == "France", "total_counts"].iloc[0]
    # total_counts_germany = totals_by_country.loc[totals_by_country["country"] == "Germany", "total_counts"].iloc[0]

    # # Quiero añadir a data_grouped 4 filas mas con la suma de los valores de los años por cada país
    # data_grouped = data_grouped.append({'country': 'Spain', 'anyo': 'All', 'counts': total_counts_spain}, ignore_index=True)
    # data_grouped = data_grouped.append({'country': 'Italy', 'anyo': 'All', 'counts': total_counts_italy}, ignore_index=True)
    # data_grouped = data_grouped.append({'country': 'France', 'anyo': 'All', 'counts': total_counts_france}, ignore_index=True)
    # data_grouped = data_grouped.append({'country': 'Germany', 'anyo': 'All', 'counts': total_counts_germany}, ignore_index=True)

    fig = px.choropleth_mapbox(
                        data_grouped,
                        color='counts',
                        geojson=counties,
                        featureidkey = 'properties.NAME',
                        locations='country', 
                        # animation_frame="anyo",
                        mapbox_style="carto-positron",
                        color_continuous_scale=px.colors.sequential.Plasma,
                        hover_data = {'country': False, 'counts': True}, #'anyo': False
                        hover_name = 'country',
                        labels={'counts':'Número de empresas', 'country':'País'},
                        zoom=2.5, center = {"lat": 45.455677, "lon": 6.822616},
                        opacity=0.8,
                        )
    fig.update_layout(plot_bgcolor='rgba(0,0,0,0)', paper_bgcolor='rgba(0,0,0,0)', font=dict(color='#04a9f5'))
    
    chart = fig.to_html()

    return chart
    
# @login_required(login_url="/login/")
def chart(request):
    word = request.GET.get('searched')
    # year = request.GET.get('selected_year')
    if word:
        # df = run_multiprocess(word, 4, 'http://localhost:9200/indice_prueba/_search', 10000)
        df = query_to_df(request, word)
        length = len(df)
        pct = round((length*100)/300000,0)
        pct = int(pct)
        if pct < 1: pct=1
        # df = query_to_df(request, word)
        # if year:
        #     df = df[df['anyo'] == int(year)]
        lineplot = f_lineplot(df)
        multilineplot = f_multilineplot(df)
        mapa = f_map(df)

        context = {'linechart': lineplot, 'multilinechart':multilineplot, 'map':mapa, 'word':word, 'pct':pct}
        html_template = loader.get_template('home/chart.html')
        return HttpResponse(html_template.render(context, request))
    else:
        # context = {'chart': None, 'chart2':None}
        return render(request, 'home/home.html')

@login_required(login_url="/login/")  
def historial_view(request):
    palabras = extract_historial(request)
    context = {'palabras': palabras}
    html_template = loader.get_template('home/historial.html')
    return HttpResponse(html_template.render(context, request))