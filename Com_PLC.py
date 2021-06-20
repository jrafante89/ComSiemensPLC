# -*- coding: utf-8 -*-

import snap7
import os
#from influxdb import InfluxDBClient
import datetime
import requests
import pandas as pd 
class plc(object):
	#função Conecção com o plc
	'''def conexao_plc(self):
		client =snap7.client.Client()
		client.connect(ip_plc,0,0)	
		return client.get_connected()
	'''	 
	#Função leitura PLC
	def ler_plc(self, list_db=[1,3], ult_byte_entrada=80, ult_byte_saida=80):
		'''
		list_db lista de banco de dados a ser lido, Padrão db1 e db2
		ult_byte_entrada e ult_byte_saida: int ultimo byte a ser lido na entrada ou saida. Padrão 80
			
		'''
		#Leitura das Entradas e saidas[

		Entradas_plc=client.read_area(0x81, 0, 0, ult_byte_entrada)
		Saidas_plc=client.read_area(0x82, 0, 0, ult_byte_saida)

		#Leitura DBs

		Dados_db={}
		for index_db in list_db:
			print index_db
			Dados_db[index_db]=client.db_get(index_db)

		#print Dados_db, Entradas_plc, Saidas_plc

		return(Dados_db, Entradas_plc, Saidas_plc)
	def ler_tags(self, tags={}, dados_plc, temp):
		dados=''
		for index in tags:
			if tags[index]['formato'] == 'bool':
				bol=snap7.util.get_bool(dados_plc, tags[index]['byte'], int(tags[index]['bit']))
				if bol:
					tags[index]['valor']= 1
				else: 
					tags[index]['valor']= 0			
				
			elif tags[index]['formato'] == 'real':
				tags[index]['valor']=snap7.util.get_real(dados_plc, tags[index]['byte'])

			#elif tags[index]['formato'] == 'int':
			#	tags[index]['valor']=snap7.util.get_int(dados_plc, tags[index]['byte'])

			#elif tags[index]['formato'] == 'string':
			#	tags[index]['valor']=snap7.util.get_string(dados_plc, tags[index]['byte'])

			dados=dados+tags[index]['medidas']+',Equipamento='+tags[index]['equipamento']+' value='+str(tags[index]['valor'])+' '+str(temp)+'\n'	
		return dados


#Configuração do banco de dados

url='http://localhost:8086/write'
params={'db':'mydb'}
s=requests.Session()

#Configuração do PLC

ip_plc = '172.26.18.39'
client =snap7.client.Client()

#Configuração dos tags

df = pd.read_csv('tags.csv', sep=':')
Entradas_df=df[df['area']=='input']
Saidas_df=df[df['area']=='output']
Banco_df=df[df['area']=='db']

Entradas = Entradas_df.to_dict(orient='index')
Saidas = Saidas_df.to_dict(orient='index')
Banco = Banco_df.to_dict(orient='index')


Index_ent=Entradas.index.T
Index_sai=Saidas.index.T
Index_db=Banco.index.T
Lista_db=df['numero_db'].unique()
Ent_byte_max=Entradas['byte'].max()+1
print Ent_byte_max
Sai_byte_max=Saidas['byte'].max()+1
print Sai_byte_max

#programa principal
while 1:
	#Conexão com o plc
	plc_dados=plc()
	client =snap7.client.Client()
	client.connect(ip_plc,0,0)	
	com=client.get_connected()
	
	if com:
		print 'Conexão com o PLC efetuada'
	else:
		print 'Não foi possivel conectar com o PLC'	

	
	while com:
		#captura horario
		tempo = datetime.datetime.today()
		print tempo
		tempo1=int(tempo.strftime('%s' '%f'))*1000	
		#coleta os dados do PLC
		df_db, df_entradas, df_saidas=plc_dados.ler_plc(Lista_db, Ent_byte_max, Sai_byte_max)
		

		# Manipulação dos dados para ser gravados no influxDB
		dados_grava =plc_dados.ler_tags(Entradas, df_entradas, tempo1)+plc_dados.ler_tags(Saidas, df_saidas, tempo1)+plc_dados.ler_tags(Banco, df_db, tempo1)


		'''print 'Entrada'
		print datetime.datetime.today()
		for index in Entradas:
			print Entradas[index]['bit'], type(Entradas.loc[(index),('bit')])
			bol=snap7.util.get_bool(df_entradas, Entradas.loc[(index),('byte')], int(Entradas.loc[(index),('bit')]))
			if bol:
				Entradas.loc[(index),('valor')]= 1
			else: 
				Entradas.loc[(index),('valor')]= 0			
			dados=dados+Entradas.loc[(index),('medidas')]+',Equipamento='+Entradas.loc[(index),('equipamento')]+' value='+str(Entradas.loc[(index),('valor')])+' '+str(tempo1)+'\n'
		
		print 'saida'
		print datetime.datetime.today()

		for index in Index_sai:
			bol=snap7.util.get_bool(df_saidas, Saidas.loc[(index),('byte')], int(Saidas.loc[(index),('bit')]))
			if bol:
				Saidas.loc[(index),('valor')]= 1
			else: 
				Saidas.loc[(index),('valor')]= 0			
			dados=dados+Saidas.loc[(index),('medidas')]+',Equipamento='+Saidas.loc[(index),('equipamento')]+' value='+str(Saidas.loc[(index),('valor')])+' '+str(tempo1)+'\n'
		
		print 'db'
		print datetime.datetime.today()
		for index in Index_db:
			if Banco.loc[(index),('formato')]=='real':
				print 'corrente'
				print datetime.datetime.today()
				Banco.loc[(index),('valor')]=snap7.util.get_real(df_db[Banco.loc[(index),('numero_db')]], Banco.loc[(index),('byte')])
				dados=dados+Banco.loc[(index),('medidas')]+',Equipamento='+Banco.loc[(index),('equipamento')]+' value='+str(Banco.loc[(index),('valor')])+' '+str(tempo1)+'\n'
			else:

				bol=snap7.util.get_bool(df_db[Banco.loc[(index),('numero_db')]], Banco.loc[(index),('byte')], int(Banco.loc[(index),('bit')]))
				if bol:
					Banco.loc[(index),('valor')]= 1
				else: 
					Banco.loc[(index),('valor')]= 0
				dados=dados+Banco.loc[(index),('medidas')]+',Equipamento='+Banco.loc[(index),('equipamento')]+' value='+str(Banco.loc[(index),('valor')])+' '+str(tempo1)+'\n'
		
'''
		print 'grava db'
		print datetime.datetime.today()
		# Grava os dados no InfluxDB
		try:
			envia=s.request(method='POST', url=url, params=params, data=dados_grava)
		
		except Exception as e:
			print 'Não foi possivel conectar ao banco de dados'
		
		else: 
			if envia.status_code == 204:
				pass
			else:
				print envia






#print Entradas
#for b in a:	
#	print b





'''
url='http://localhost:8086/write'
params={'db':'mydb'}

Equip = {'TC01' : {'byte_corrente': 0, 'byte_status':0 },'TC02':{ 'byte_corrente': 4, 'byte_status':16 }, 'TC03':{ 'byte_corrente': 8, 'byte_status':32 }, 'TR03':{ 'byte_corrente': 12, 'byte_status':48 }, 'TC04':{ 'byte_corrente': 16, 'byte_status':64 }, 'TR04':{ 'byte_corrente': 20, 'byte_status':80 }, 'TC05':{ 'byte_corrente': 24, 'byte_status':96  }, 'TC06':{ 'byte_corrente': 28, 'byte_status':112  }, 'TC07':{ 'byte_corrente': 32, 'byte_status':128  }, 'TC08':{ 'byte_corrente': 36, 'byte_status':144  }, 'EL01':{ 'byte_corrente': 40, 'byte_status':160  }, 'EL02':{ 'byte_corrente': 44, 'byte_status':176  }, 'RD01':{ 'byte_corrente': 48, 'byte_status':192 }, 'RD02':{'byte_corrente': 52, 'byte_status':208  }, 'RD03':{ 'byte_corrente': 56, 'byte_status':224 }, 'FM01/M1':{ 'byte_corrente': 60, 'byte_status':240 }, 'FM01/M2':{ 'byte_corrente': 64, 'byte_status':256 }, 'FM01/M3':{'byte_corrente': 68, 'byte_status':272  }, 'FM02/M1':{ 'byte_corrente': 72, 'byte_status':288 }, 'FM02/M2':{ 'byte_corrente': 76, 'byte_status':304 }, 'FP01':{ 'byte_corrente': 80, 'byte_status':320 }, 'FP02':{ 'byte_corrente': 84, 'byte_status':336 }, 'FP03':{ 'byte_corrente': 88, 'byte_status':352 }, 'FP04':{ 'byte_corrente': 92, 'byte_status':368 }, 'FP05':{ 'byte_corrente': 96, 'byte_status':384 }, 'FP06':{ 'byte_corrente': 100, 'byte_status':400 }, 'FP07':{ 'byte_corrente': 104, 'byte_status':416 }, 'FP08':{ 'byte_corrente': 108, 'byte_status':432 }, 'FP09':{ 'byte_corrente': 112, 'byte_status':448 }, 'FP10':{ 'byte_corrente': 116, 'byte_status':464 },  'FP11':{ 'byte_corrente': 120, 'byte_status':480 }, 'FP12':{ 'byte_corrente': 124, 'byte_status':496 }, 'FP13':{ 'byte_corrente': 128, 'byte_status':512 }, 'FP14':{ 'byte_corrente': 132, 'byte_status':528 }, 'FP15':{ 'byte_corrente': 136, 'byte_status':544 }, 'FP16':{ 'byte_corrente': 140, 'byte_status':560 }, 'FP17':{ 'byte_corrente': 144, 'byte_status':576 }, 'FP18':{ 'byte_corrente': 148, 'byte_status':592 }, 'FP19':{ 'byte_corrente': 152, 'byte_status':608 }, 'FP20':{ 'byte_corrente': 156, 'byte_status':624 }, 'VA01/M1':{ 'byte_corrente': 160, 'byte_status':640 }, 'VA02/M1':{ 'byte_corrente': 164, 'byte_status':656 }, 'VA03/M1':{ 'byte_corrente': 168, 'byte_status':672}, 'RES01':{ 'byte_corrente': 172, 'byte_status':688 }, 'VA05/M1':{ 'byte_corrente': 176, 'byte_status':704 }, 'VA06/M1':{ 'byte_corrente': 180, 'byte_status':720 }, 'VA07/M1':{ 'byte_corrente': 184, 'byte_status':736 }, 'VA08/M1':{ 'byte_corrente': 188, 'byte_status':752 }, 'VA09/M1':{ 'byte_corrente': 192, 'byte_status':768 }, 'VA12/M1':{'byte_corrente': 204, 'byte_status':816  }, 'VA13/M1':{ 'byte_corrente': 208, 'byte_status':832 }, 'PA01/P1':{ 'byte_corrente': 212, 'byte_status':848 }, 'PA01/P2':{ 'byte_corrente': 216, 'byte_status':864 }, 'PA02/P1':{ 'byte_corrente': 220, 'byte_status':880 }, 'PA02/P2':{ 'byte_corrente': 224, 'byte_status':896 }, 'PA03/P1':{ 'byte_corrente': 228, 'byte_status':912 }, 'PA03/P2':{ 'byte_corrente': 232, 'byte_status':928 }, 'VTAD/01':{ 'byte_corrente': 236, 'byte_status':944 }, 'BOAD/01':{ 'byte_corrente': 240, 'byte_status':960 }}
Medidas = {'CORRENTE': 12, 'DEF_GERAL': {'byte':0, 'bit':0, 'valor':0 }, 'DEF_DISJUNTOR': {'byte':0, 'bit':1, 'valor':0 }, 'DEF_CONF_LIGADO': {'byte':0, 'bit':2, 'valor':0 }, 'DEF_SOFT': {'byte':0, 'bit':3, 'valor':0 }, 'DEF_COM_REDE': {'byte':0, 'bit':4, 'valor':0 }, 'DEF_RESERVA': {'byte':0, 'bit':5, 'valor':0 }, 'DEF_BOTAO_EMERGENCIA': {'byte':0, 'bit':6, 'valor':0 }, 'DEF_CHAVE_EMERGENCIA': {'byte':0, 'bit':7, 'valor':0 }, 'DEF_TAMPA_INSP': {'byte':1, 'bit':0, 'valor':0 }, 'DEF_CHAVE_SECCIONADORA_ABERTA': {'byte':1, 'bit':1, 'valor':0 }, 'DEF_SEN_MOV': {'byte':1, 'bit':2, 'valor':0 }, 'DEF_SEN_EMB': {'byte':1, 'bit':3, 'valor':0 }, 'DEF_SERSOR_DES': {'byte':1, 'bit':4, 'valor':0 }, 'DEF_TEMP_MAN': {'byte':1, 'bit':5, 'valor':0 }, 'DEF_TEMP_MOTOR': {'byte':1, 'bit':6, 'valor':0 }, 'DEF_SEN_RES': {'byte':1, 'bit':7, 'valor':0 }, 'IND_LIG_DIR': {'byte':2, 'bit':0, 'valor':0 }, 'IND_LIG_ESQ': {'byte':2, 'bit':1, 'valor':0 }, 'CH_MAN_AUTO': {'byte':2, 'bit':2, 'valor':0 }, 'COND_AUTO': {'byte':2, 'bit':3, 'valor':0 }, 'COND_AUTO_REV': {'byte':2, 'bit':4, 'valor':0 }, 'STATUS_SAIDA_MOTOR': {'byte':2, 'bit':5, 'valor':0 }, 'STATUS_SAIDA_MOTOR_INV': {'byte':2, 'bit':6, 'valor':0 }, 'STATUS_LIB': {'byte':2, 'bit':7, 'valor':0 }, 'STATUS_LIB_REV': {'byte':3, 'bit':0, 'valor':0 }, 'EQUIP_OCUPADO_ROTA': {'byte':3, 'bit':1, 'valor':0 }, 'SIRENE': {'byte':3, 'bit':2, 'valor':0 }, 'BLOQ_PARTIDA_MULT': {'byte':3, 'bit':3, 'valor':0 }, 'INTERT_EXT1': {'byte':3, 'bit':4, 'valor':0 }, 'INTERT_EXT2': {'byte':3, 'bit':5, 'valor':0 }, 'LOCAL_MANUAL': {'byte':3, 'bit':6, 'valor':0 }, 'DEF_RESERVA_2': {'byte':3, 'bit':7, 'valor':0 }, 'COMAND_AUTO_TESTE': {'byte':4, 'bit':0, 'valor':0 }, 'COMAND_LIGA_TESTE': {'byte':4, 'bit':1, 'valor':0 }, 'COMAND_LIGA_TESTE_REV': {'byte':4, 'bit':2, 'valor':0 }, 'COMANDO_MANUT': {'byte':4, 'bit':3, 'valor':0 }, 'COMAND_REARME': {'byte':4, 'bit':4, 'valor':0 }, 'COMAND_REARME_SEG': {'byte':4, 'bit':5, 'valor':0 }, 'COMAND_OPER_LOCAL': {'byte':4, 'bit':6, 'valor':0 }, 'COMAND_RES_2': {'byte':4, 'bit':7, 'valor':0 }, 'COMAND_RES_3': {'byte':5, 'bit':0, 'valor':0 }, 'COMAND_RES_4': {'byte':5, 'bit':1, 'valor':0 }, 'COMAND_INIB_SMOV': {'byte':5, 'bit':2, 'valor':0 }, 'COMAND_INIB_SEMB': {'byte':5, 'bit':3, 'valor':0 }, 'COMAND_INIB_SDES': {'byte':5, 'bit':4, 'valor':0 }, 'COMAND_INIB_STEMP_MAN': {'byte':5, 'bit':5, 'valor':0 }, 'COMAND_INIB_STEMP_MOT': {'byte':5, 'bit':6, 'valor':0 }, 'COMAND_INIB_RES': {'byte':5, 'bit':7, 'valor':0 }, 'COMAND_INIB_INTER_EXT1':{'byte':6, 'bit':0, 'valor':0 }, 'COMAND_INIB_INTER_EXT2': {'byte':6, 'bit':1, 'valor':0 }, 'SINAL_BOTAO_EMERGENCIA': {'byte':6, 'bit':2, 'valor':0 }, 'SINAL_CHAVE_EMERGENCIA': {'byte':6, 'bit':3, 'valor':0 }, 'SINAL_TAMPA_INSP': {'byte':6, 'bit':4, 'valor':0 }, 'SINAL_CHAVE_SECCIONADORA': {'byte':6, 'bit':5, 'valor':0 }, 'SINAL_SENSOR_MOV': {'byte':6, 'bit':6, 'valor':0 }, 'SINAL_SENSOR_EMB': {'byte':0, 'bit':7, 'valor':0 }, 'SINAL_SENSOR_DES': {'byte':7, 'bit':0, 'valor':0 }, 'SINAL_TEMP_MAN': {'byte':7, 'bit':1, 'valor':0 }, 'SINAL_TEMP_MOTOR': {'byte':7, 'bit':2, 'valor':0 }, 'SINAL_RES': {'byte':7, 'bit':3, 'valor':0 }, 'SINAL_FC_MOTOR': {'byte':7, 'bit':4, 'valor':0 }, 'SINAL_FC_MOTOR_REV': {'byte':7, 'bit':5, 'valor':0 }, 'COMAND_MOVIMENTACAO_AUTO': {'byte':7, 'bit':6, 'valor':0 }, 'SINAL_RESERVA_2': {'byte':7, 'bit':7, 'valor':0 }}
dados=''


client =snap7.client.Client()
client.connect('172.26.18.39',0,0)
com=client.get_connected()
print com
s=requests.Session()


'''
'''def grava(medicao, equipamento, temp, valor):
	json_body = [
	    {
		"measurement": medicao,
		"tags": {
		    "Equipamento": equipamento,
		},
		#"time": int(tempo.strftime('%f')),
		"time": temp,		
		"fields": {
		    "value": valor
		}
	    }
	]
	cliente.write_points(json_body)'''
'''
#cliente = InfluxDBClient('localhost', 8086, 'root', 'root', 'TESTE')

if com == True:
		

	while 1:
		tempo = datetime.datetime.today()
		print tempo
		tempo1=int(tempo.strftime('%s' '%f'))*1000
		data_corrente = client.db_get(3)
		data_status = client.db_get(1)
		data_input = client.read_area(0x81, 0, 0, 5)
		data_output = client.read_area(0x82, 0, 0, 5)
		data_registros = client.db_get(1)
		dados = ''

		for k in Equip:
			
			for l in Medidas:
				
				if l == 'CORRENTE':
					Medidas['CORRENTE']= snap7.util.get_real(data_corrente,Equip[k]['byte_corrente'])
					dados=dados+l+',Equipamento='+k+' value='+str(Medidas['CORRENTE'])+' '+str(tempo1)+'\n'
					#print dados
				else:
					byte=Medidas[l]['byte']+Equip[k]['byte_status']
					valor_bool=snap7.util.get_bool(data_status, byte , Medidas[l]['bit'])
					if valor_bool:
						Medidas[l]['valor'] = 1
					else:
						Medidas[l]['valor'] = 0
					#print l
					dados=dados+l+',Equipamento='+k+' value='+str(Medidas[l]['valor'])+' '+str(tempo1)+'\n'
					#print dados

		print '1 '+ str(datetime.datetime.today())
		try:
			a=s.request(method='POST', url=url, params=params, data=dados)
		
		except Exception as e:
			print 'Não foi possivel conectar ao banco de dados'
		
		else: 
			if a.status_code == 204:
				pass
			else:
				print a
		print '2 '+ str(datetime.datetime.today())

			#grava('CORRENTE', k, tempo1, int(Equip[k]['CORRENTE']))
		#client.close()
		print tempo1
		#ele1_curr = snap7.util.get_real(data,40)
		#ele2_curr = snap7.util.get_real(data,44)
		#os.system('clear')
		#print ele1_curr, ele2_curr
			
'''

