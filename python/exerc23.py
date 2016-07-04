import mincemeat
import glob
import csv
import os

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
                    yield autor.lower() + ';' + palavra.lower(),1
                            
    
def reducefn(k, v):
    print 'reduce ' + k
    return sum(v)

s = mincemeat.Server()

s.datasource = source
s.mapfn = mapfn
s.reducefn = reducefn

results = s.run_server(password="changeme")

w = csv.writer(open(dir_base +'\\result_23.csv', "w"))
for k, v in results.items():
    autor = str(k).split(";")[0].upper()
    palavra = str(k).split(";")[1].lower().capitalize()
    w.writerow([autor, palavra, v])


