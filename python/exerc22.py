import mincemeat
import glob
import csv
import os

dir_base = os.getcwd()
dir_files_join = dir_base + '\\join\\'

text_files = glob.glob(dir_files_join + '*')

print('base dir')
print(dir_base)
print('files joins dir')
print(dir_files_join)

def file_contents(file_name):
    f = open(file_name)
    try:
        return f.read()
    finally:
        f.close

source = dict((file_name, file_contents(file_name))for file_name in text_files)

vendas_csv = str(dir_files_join + '2.2-vendas.csv')
filiais_csv = str(dir_files_join + '2.2-filiais.csv')

print(vendas_csv)
print(filiais_csv)

def mapfn(k, v):
    print 'map ' +k
    for line in v.splitlines():
        if k == 'C:\\Users\\Renato\\Documents\\GitHub\\bigdata\\python\\join\\2.2-vendas.csv':
            yield line.split(';')[0], 'vendas' + ':'+line.split(';')[5]
        if k == 'C:\\Users\\Renato\\Documents\\GitHub\\bigdata\\python\\join\\2.2-filiais.csv':
            yield line.split(';')[0], 'filial' + ':'+line.split(';')[1]

def reducefn(k, v):
    print 'reduce ' + k
    total = 0
    for index, item in enumerate(v):
        if item.split(":")[0] == 'vendas':
            total = int(item.split(":")[1]) +total
        if item.split(":")[0] == 'filial':
            nomeFilial = item.split(":")[1]
    L = list()
    L.append(nomeFilial + " , " + str(total))
    return L

s = mincemeat.Server()

s.datasource = source
s.mapfn = mapfn
s.reducefn = reducefn

results = s.run_server(password="changeme")

w = csv.writer(open(dir_base + "\\result_22.csv", "w"))
for k, v in results.items():
    w.writerow([k, str(v).replace("[","").replace("]","").replace("'","").replace(' ', '')])
w.close
