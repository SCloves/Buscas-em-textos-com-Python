from bs4 import BeautifulSoup
import urllib3
from urllib.parse import urljoin
import re
import nltk
import os
import psycopg2
import sys

'''
DB = os.environ["NOME_DO_MEU_DB"] 
HOST = os.environ["HOST_DO_MEU_DB"] 
USER = os.environ["USUARIO_DO_MEU_DB"]
PASS = os.environ["SENHA_DO_MEU_DB"]
'''
DB = 'indice'
HOST = 'localhost'
USER = 'postgres'
PASS = 'c1l2o3v4'

def getTexto(sopa):
    for tag in sopa(['script', 'style']):
        tag.decompose()
    
    return ' '.join(sopa.stripped_strings)
    
def separaPalavras(texto):
    splitter = re.compile('\\W*')
    stop = nltk.corpus.stopwords.words('portuguese')
    stemmer = nltk.stem.RSLPStemmer()
    lista_palavras = []
    lista = [p for p in splitter.split(texto) if p != '' ] 
    for p in lista:
        if p.lower() not in stop and len(p) > 1:
            lista_palavras.append(stemmer.stem(p).lower())
            
    return lista_palavras
    
def conectar():
    conn_string = "host='%s' dbname='%s' user='%s' password='%s'" %(HOST, DB, USER, PASS)
    try:
        # get a connection, if a connect cannot be made an exception will be raised here
        conn = psycopg2.connect(conn_string)
        conn.set_client_encoding('UNICODE')
        return conn
    except:
        print ("Can't connected to database!")
        
        
def inserPagina(url):
    con  = conectar()
    cursor = con.cursor()
    cursor.execute("select idurl from buscas_em_textos.urls\
                    order by idurl desc limit 1")
    try:
        idpagina = cursor.fetchall()[0][0] + 1
    except:
        idpagina = 0
    cursor.execute("insert into buscas_em_textos.urls (idurl, url) values(%d,'%s')" %(idpagina,url))
    con.commit()
    cursor.close()
    con.close()
    return idpagina
    
def palavraIndexada(palavra):
    retorno = -1 # não existe a palavra no índice
    con  = conectar()
    cursor = con.cursor()
    cursor.execute("select idpalavra from buscas_em_textos.palavras where palavra = '%s'" %palavra)
    if cursor.rowcount > 0:
        #print('palavra já cadastrada')
        retorno = cursor.fetchone()[0]
    #else:
        #print('palavra não cadastrada')
    cursor.close()
    con.close()
    return retorno
    
def paginaIndexada(url):
    retorno = -1 # não exite página
    con  = conectar()
    cursorURL = con.cursor()
    cursorURL.execute("select idurl from buscas_em_textos.urls where url = '%s'" %url)
    if cursorURL.rowcount > 0:
        #print('URL já cadastrada!')
        idurl = cursorURL.fetchone()[0]
        cursorPalava = con.cursor()
        cursorPalava.execute("select idurl from buscas_em_textos.palavra_localizacao \
                             where idurl = '%s'" %idurl)
        if cursorPalava.rowcount > 0:
            #print('Url com palavras')
            retorno = -2 # existe a página com palavras cadastradas
        else:
            #print('URL sem palavras')
            retorno = idurl # existe a página sem palavras
        cursorPalava.close()
    #else:
       # print ('URL não cadastrada!')
        
    cursorURL.close()
    con.close()
    
    return retorno
    
def inserePalavra(palavra):
    con  = conectar()
    cursor = con.cursor()
    cursor.execute("select idpalavra from buscas_em_textos.palavras \
                    order by idpalavra desc limit 1")
    try:
        idpalavra = cursor.fetchall()[0][0] + 1
    except:
        idpalavra = 0
        
    cursor.execute("insert into buscas_em_textos.palavras (idpalavra, palavra) \
                    values(%d, '%s')" %(idpalavra, palavra))
    
    con.commit()
    cursor.close()
    con.close()
    return idpalavra
    
def inserPalavraLocalizacao(idurl, idpalavra, localizacao):
    con  = conectar()
    cursor = con.cursor()
    cursor.execute("select idpalavra_localizacao from buscas_em_textos.palavra_localizacao \
                    order by idpalavra_localizacao desc limit 1")
    try:
        idpalavra_localizacao = cursor.fetchall()[0][0] + 1
    except:
        idpalavra_localizacao = 0
        
    query = "insert into buscas_em_textos.palavra_localizacao \
            (idpalavra_localizacao, idurl, idpalavra, localizacao) \
            values(%d, %d, %d, %d)" %(idpalavra_localizacao,idurl,idpalavra,localizacao)
    
    cursor.execute(query)
    con.commit()
    cursor.close()
    con.close()
    return idpalavra_localizacao
    
def indexador(url, sopa):
    indexada = paginaIndexada(url)
    if indexada == -2:
        print("URL já indexada!")
        return
    elif indexada == -1:
        idnova_pagina = inserPagina(url)
    elif indexada > 0:
        idnova_pagina = indexada
    
    print("Indexando " + url)
    
    texto = getTexto(sopa)
    palavras = separaPalavras(texto)
    for i in range(len(palavras)):
        palavra = palavras[i]
        idpalavra = palavraIndexada(palavra)
        if idpalavra == -1:
            idpalavra = inserePalavra(palavra)
        inserPalavraLocalizacao(idnova_pagina, idpalavra, i)
        

def crawl(paginas, profundidade):
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    for i in range(profundidade):
        novas_paginas = set()
        for pagina in paginas:
            http = urllib3.PoolManager()
            try:
                dados_pagina = http.request('GET', pagina)
            except:
                print('Erro na página ' + pagina)
                continue
            
            sopa = BeautifulSoup(dados_pagina.data, 'html')
            indexador(pagina, sopa)
            links = sopa.find_all('a')
            for link in links:
                if 'href' in link.attrs:
                    url = urljoin(pagina, str(link.get('href')))
                    if url.find("'") != -1:
                        continue
                    url = url.split('#')[0]
                    if url[0:4] == 'http':
                        novas_paginas.add(url)
                    
            paginas = novas_paginas
    return paginas
    
if __name__=='__main__':
    crawl([sys.argv[1]], int(sys.argv[2]))