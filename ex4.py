#################################################
# Descricao: Resolucao Exercicio 4 - DataFrame  #
# Autor: Wellington C. Faria '                  #
# Data: 27/01/18                                #
#################################################

from pyspark import SparkContext
from pyspark.sql import SQLContext, Row
from datetime import datetime


sc = SparkContext("local", "Exercicio4_DataFrame")
sqlContext = SQLContext(sc)

# Carregando Arquivos
arquivo_final = sc.textFile("/ANO/*/dataset.csv") #Carrega todos os arquivos


arquivo_final = arquivo_final.map(lambda l : l.replace(';;', ';')) #Tratamento do Arquivo para retirar o erro no formato do mesmo.
arquivo_final = arquivo_final.filter(lambda l : l.find('data')<0) #Tira o Cabecalho dos arquivos
arquivo_final = arquivo_final.map(lambda l : l.replace(',','.')) # Troca a , por . para converter o value para float abaixo

linhas = arquivo_final.map(lambda l: l.split(";")) # Ponto e virgula porque eh um aquivo CSV
linhas = linhas.map(lambda l: [l[0],l[1],'0'] if l[2] == '' else [l[0],l[1],l[2]] ) #Tranforma o Vazio do value em Zero
linhas = linhas.map(lambda l: [l[0]+':00',l[1],l[2]] ) # Ajustando o Dado de Data para conversao
linhas  = linhas.map(lambda p: Row(data=datetime.strptime(p[0], '%d-%b-%Y %H:%M:%S'), timestamp=int(p[1]), value=float(p[2]), quadrado = (float(p[2])**2) ))
linhas = linhas.sortBy(lambda p: p['data']) #Ordena por Data Crecente
valores = linhas.map(lambda l: Row(data=l['data'].strftime('%m-%d-%Y %H:%M'), timestamp=l['timestamp'], value=l['value'], quadrado=l['quadrado']) ) #Formatar data como pedido MM-DD-YYYY HH:MM



df1 = sqlContext.createDataFrame(valores) #Criando DataFrame com um RDD

#df1.show(n=20,truncate=False)
df1.show(n=8300,truncate=False)

