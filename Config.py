import shelve
import pandas as pd


train_df = pd.read_csv('tags.csv', sep=':')
print train_df
'''
medida:
	tag:
		onde:
		byte:
		bit:






while 1:
	
	config=shelve.open('config.conf')
	print '1: para configurar CLP\n2: para add local de busca do PLC (inputs, output e DB)\n3: para adicionar tags\n4: para remover tags\n'
	num1=raw_input('entre com o valor: ')
	if num1 == '1':
		print ('\n1\n')
	elif num1 == '2':
		print ('\n2\n')
	elif num1 == '3':
		ent = raw_input('digita nesse formato: "MEDIDA" "EQUIPAMENTO" "BD OU INPUT OU OUTPUT" "BYTE" "BIT": ')
		ent_aux=ent.split()
		config['medidas']= {ent_aux[0]:{ent_aux[1]:{'local':ent_aux[2], 'byte':ent_aux[3], 'bit':ent_aux[4]}}}	
		print ent_aux[1], len(ent_aux)
		print config['medidas']
		print config['medidas']['med1']['tc1']
		print config['medidas']['med1']['tc1']['local']
		print config['medidas']['med1']['tc1']['byte']
		print config['medidas']['med1']['tc1']['bit']

		print ('\n3\n')
	elif num1 == '4':
		print ('\n4\n')
	else:
		print('numero incorreto tente novamente\n')
'''
