import mincemeat
import glob
import csv
import os, sys
import time
import operator

dir_base = os.getcwd()
dir_files_trab = dir_base + '\\trab2.3\\'

print(dir_files_trab)

text_files = glob.glob(dir_files_trab+'*')

def file_contents(file_name):
    f = open(file_name)
    try:
        return f.read()
    finally:
        f.close

source = dict((file_name, file_contents(file_name))for file_name in text_files)

def mapfn(k, v):
    print 'map ' +k
    from stopwords import allStopWords
    for line in v.splitlines():
        autores = line.split(":::")[1]
        titulo =  line.split(":::")[2].replace(".", "")
        for autor in autores.split("::"):
            for palavra in titulo.split():
                if (palavra not in allStopWords and len(palavra) > 1):
                    yield autor.lower(),palavra.lower()
                            
    
def reducefn(k, v):
    print 'reduce ' + k
    dictPalavras = dict()

    for palavra in v:
        total = dictPalavras.get(palavra,0) + 1
        dictPalavras[palavra] = total       
    
    return dictPalavras

def stringLinhaPalavras(dictPalavras):
    ordenada = sorted(dictPalavras.items(), key=lambda x:x[1], reverse=True)
    linhas = list()
    for palavra in ordenada:
        linha = str(palavra).replace("('","").replace(")","").replace(",",":").replace(" ","").replace("'","")
        linhas.append(linha)
    return str(linhas).replace("'","").replace("[","").replace("]","").replace(" ", "")

s = mincemeat.Server()

s.datasource = source
s.mapfn = mapfn
s.reducefn = reducefn

tempoIni = time.time()
results = s.run_server(password="changeme")

w = csv.writer(open(dir_base +'\\result_23.csv', "w"))
for k, v in results.items():
    linha = str(k).upper()+':'+stringLinhaPalavras(v)
    w.writerow([linha])
    
print 'Processo finalizado em ', time.time() - tempoIni, 'segundos'
print 'digite exit() para sair'
