from fastapi import FastAPI, File, UploadFile
from fastapi.responses import HTMLResponse, StreamingResponse
from deta import Deta, Drive
import pandas as pd

app = FastAPI()

drive = Drive("archivos")

ds1 = drive.get("dataset_final.csv")
ds2 = drive.get("dataset_listo.csv")



@app.get('/get_max_duration/{year}/{platform}/{duration_type}')
def get_max_duration(year: int, platform: str, duration_type: str):
    df = pd.read_csv(ds1)
    film = df.loc[(df.duration_type == duration_type) & (df.plataforma == platform) & (df.release_year == year)]
    film2 = film.sort_values(['duration_int'], ascending=[False])
    film3 = film2[['title']]
    lista = film3.iloc[0].to_list()
    return lista

@app.get('/get_count_plataform/{platform}')
def get_count_plataform(platform: str):
    df = pd.read_csv(ds2)
    cant = df.loc[(df.type == 'movie') & (df.plataforma == platform)]
    conteo = cant.shape[0]
    return conteo




def listaPalabrasDicFrec(listaPalabras):
    frecuenciaPalab = [listaPalabras.count(p) for p in listaPalabras]
    return dict(list(zip(listaPalabras, frecuenciaPalab)))


def ordenaDicFrec(dicFrec):
    aux = [(dicFrec[key], key) for key in dicFrec]
    aux.sort()
    aux.reverse()
    return aux


@app.get('/get_actor/{platform}/{year}')
def get_actor(platform: str, year: int):
    df = pd.read_csv(ds2)
    act = df[(df['plataforma'] == platform)
              & (df['release_year'] == year)].cast.str.split(',')
    act = act.dropna()

    actores_año = []
    for actores in act:
        for actor in actores:
            actor = actor.rstrip()
            actor = actor.lstrip()
            actores_año.append(actor)

    actor = listaPalabrasDicFrec(actores_año)
    actor = ordenaDicFrec(actor)

    return {'plataforma':platform,
             'año': year,
            'actor':actor[0][1],
            'apariciones': actor[0][0]}


@app.get('/get_score_count/{platform}/{scored}/{year}')
def get_score_count(platform: str, scored: float, year: int):
    df = pd.read_csv(ds1)
    cant = df.loc[(df.score_review > scored) & (df.plataforma == platform) &
                  (df.release_year == year)]
    conteo = cant.shape[0]
    return {
        'plataforma': platform,
        'catidad': conteo,
        'year': year,
        'score': scored
    }
