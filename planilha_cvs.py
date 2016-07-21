#!env/bin/python
import sys
import csv
import pymysql
import gspread
import htmlmin
from collections import OrderedDict
from itertools import repeat
from oauth2client.service_account import ServiceAccountCredentials


class Magento_csv_google():
   def __init__(self,fconfig):
         self.wks = ""
         self.head_value = []
         self.body_value = []
         self.w_sku      = []
         self.w_desc     = []
         self.params={}
         try:
            fconfig = open(fconfig)
         except:
            print("Nao foi possivel abrir o arquivo de configuracao")
            sys.exit(1)

         for linha in fconfig.readlines():
            (index,valor) = linha.split("=")
            valor = valor.replace('\n','')
            valor = valor.replace(' ','')
            self.params[index]=valor

   def connect_google(self):
       #conectando no google
       try:
          scope = ['https://spreadsheets.google.com/feeds']
          credentials = ServiceAccountCredentials.from_json_keyfile_name(self.params['json_file'], scope)
          gc = gspread.authorize(credentials)
          self.wks = gc.open(self.params['planilha_name']).sheet1
       except:
          print("Erro ao conectar no Google")
          sys.exit(1)

   def connect_mysql(self):
       #conectando mysql
       try:
         conn = pymysql.connect(host=self.params['db_ip'],unix_socket= '/var/run/mysqld/mysqld.sock',user=self.params['db_user'], passwd=self.params['db_password'],db=self.params['db_name'])
         pointer = conn.cursor()
         pointer.execute("SELECT  * FROM wp_posts")
         for response in pointer:
             if response[7] == "publish":
                self.w_sku.append(response[5])
                self.w_desc.append(response[4])
         pointer.close()
         conn.close()
       except:
         print("Erro ao conectar no Mysql")
         sys.exit(1)
 
         #unique_list = list(OrderedDict(zip(list_sku, repeat(None))))
         #for x in unique_list:
         #if x in w_sku:
         #   print(x)

          
   
   def load_data(self,line):
       try:
          self.head_value = self.wks.row_values(line)
       except:
          print("Erro ao ler dados da planilha do google")
          sys.exit(1)
       next_line=line + 1
       next=True
       while next:
          try:
             tmp_body =  self.wks.row_values(next_line)
          except:
             print("Erro ao ler dados da planilha do google")
             sys.exit(1)
          next_line=next_line+1
          if tmp_body[0]:
             self.body_value.append(tmp_body)
          else:
             next=False

   def join_mysql_google(self):
       for g_line in self.body_value:
           #Verificando se SKU da planilha foi feito no wordpress
           print(g_line)
           if g_line[0] in self.w_sku:
              index = self.w_sku.index(g_line[0])
              r_desc,desc_comp = self.w_desc[index].split("<!--more-->")
              
              #adicionando os conteudos na lista final
              #verificar se existe a descricao resumida
              if desc_comp:
                 g_line[-2]= htmlmin.minify(desc_comp, remove_comments=True, remove_empty_space=True)
                 g_line[-1]= htmlmin.minify(r_desc, remove_comments=True, remove_empty_space=True)
              else:
                 f_line[-2]=r_desc
       

   def generate_csv(self,linha_head):
       # carregando os dados da plinha
       self.load_data(linha_head)  

       # join google com WordPress
       self.join_mysql_google()
        
       #criando um csv
       out = csv.writer(open(file_csv,"w"), delimiter=';',quoting=csv.QUOTE_ALL)
       #escrevendo o head
       out.writerow(self.head_value)
       for line in self.body_value:
           out.writerow(line) 

cvs_teste = Magento_csv_google('config.json')
cvs_teste.connect_google()
cvs_teste.connect_mysql()
cvs_teste.generate_csv(1)
print("arquivo gerado.")
