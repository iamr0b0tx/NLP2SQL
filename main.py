# import from std lib
import os, sys, re
from itertools import combinations

# from third party
import mysql.connector

class SQL:
	def __init__(self, database=None, log_state=False, password="", user="root", host="localhost"):
		self.log_state = log_state
		self.database = database
		if database == None:
			self.connection = mysql.connector.connect(
				host = host,
				user = user,
				passwd = password
			)

		else:
			self.connection = mysql.connector.connect(
				host = host,
				user = user,
				passwd = password,
				database = database
			)

		self.cursor = self.connection.cursor()
		
		# load the entire database 
		self.loadDatabase()

	def executeAndReturn(self, query):
		self.execute(query)
		return self.cursor.fetchall()

	def execute(self, query):
		self.cursor.execute(query)
		return

	def loadDatabase(self):
		# holds the database data structure
		self.databases, self.tables = {}, {}

		# holds all the databases, and tables in the SQL
		self.all_databases, self.all_tables = [], []

		# load all databases
		databases = self.executeAndReturn("SHOW DATABASES") if self.database == None else (self.database,)

		# iterate all the databases to load database
		for db in databases:
			database = db[0]
			self.databases[database] = []
			self.all_databases.append(database)

			# prepare the qury to get all tables of the databases
			query = "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE' AND TABLE_SCHEMA='{}' ".format(database)

			# load all tables
			tables = self.executeAndReturn(query)
			
			# log all loaded database to console
			self.log('loading tables from database={}'.format(database))

			for tab in tables:
				table = tab[0]
				self.databases[database].append(table)

				if table not in self.tables:
					self.tables[table] = []

				self.tables[table].append(database)
				self.all_tables.append(table)

				self.log("  loading table={}".format(table))

				# for ob in objs:
				# 	self.objects[obj].append(database)

		return

	def log(self, output=''):
		if self.log_state:
			print(output)

class NLP2SQLParser:
	def __init__(self, database=None, password="", user="root", host="localhost", log_state=False):
		# if to log on concole or not
		self.log_state = log_state

		# the identity of the table and database in statement
		self.DATA = "<class 'data'>"
		self.COLUMN = "<class 'column'>"
		self.TABLE = "<class 'table'>"
		self.DATABASE = "<class 'database'>"
		
		# the models will save here
		self.models = {}

		# get the sql object to manipulate the db
		self.sql = SQL(database, log_state, password, user, host)

		# train the model from the train.txt
		self.generateTraining()
		self.train()

	def execute(self, code, model, template_map):
		varz = model['var_type']
		dtc = {'database':[], 'table':[], 'column':[]}

		def getDatabase(datatype, data_dict):
			var_index = varz.index(datatype)
			data = template_map['[var]{}'.format(var_index)]
			return data, data_dict[data]

		def update_dtc(databases=None, tables=None, columns=None):
			def addValues(key, li):
				for x in li:
					dtc[key].append(x)

			if databases != None:
				addValues('database', databases)

			elif tables != None:
				addValues('table', tables)

			else:
				addValues('column', columns)
			
			return
		
		def getMax(li):
			print(li)
			s = set(li)
			freq = [li.count(x) for x in s]
			value = freq.index(max(freq)) if len(freq) > 0 else None
			return value
			
		print(varz)

		if self.DATA in varz:
			data, databases, tables, columns = getDatabase(self.DATA, self.sql.data)
			update_dtc(database, table, column)

		if self.TABLE in varz:
			table, databases = getDatabase(self.TABLE, self.sql.tables)
			update_dtc(databases, table)

		if self.COLUMN in varz:
			column, databases, tables = getDatabase(self.DATA, self.sql.data)
			update_dtc(databases, tables, column)

		if self.DATABASE not in varz:
			database, table, column = getMax(dtc['database']), getMax(dtc['table']), getMax(dtc['column'])

			if database == None:
				return False

		else:
			database = template_map['[var]{}'.format(varz.index(self.DATABASE))]
			
		

		print(database, model, template_map)

	def generateTraining(self):
		'''
		generate the training file for the std lib of pai
		'''
		training_data = []

		# for assignment
		executions = ['SELECT * FROM {}.{}']

		variables = [
			[
				[("what", "which"), ("users", "places", "companies"), ("america", "usa", "qlover")],
			],
			[
				[("what", "which"), ("states", "mountains"), ("africa", "north america")],
			]
		]

		similar_codes = ["{} {} are in {}"]
		
		for ci, code in enumerate(similar_codes):
			for ei, execution in enumerate(executions):
				a, b, c = variables[ei][ci]
				for ax in a:
					for bx in b:
						for cx in c:
							td = '{} ~ {}\n'.format(code.format(ax, bx, cx), execution.format(cx, bx))
							training_data.append(td)

		# open the file and read the lines of code
		with open('train.txt', 'w') as file:
			file.writelines(training_data)

		return

	def interpret(self, code):
		self.log('code:{}'.format(code))
		
		template, var_template = self.parse(code)
		self.log(' template:{}, var_template:{}'.format(template, var_template))

		status = True
		while status:
			for model in self.models:
				if model in template:
					self.log('  model:{} found in template'.format(model))

					haystack = " ".join([x[:-1] if x.startswith('[var]') else x for x in var_template.split()])
					pin = self.models[model]['template']

					start = haystack.index(pin)
					span = (start, start + len(pin) + haystack.split().count('[var]'))

					template_map = self.map(code, var_template)
					new_template_map = {x:template_map[x] for x in template_map if x in var_template[span[0]:span[1]]}


					result = self.run(model, self.models[model], new_template_map)

					if result == False:
						status = False
						continue

					template = template.replace(model, str(type(result)), 1)

					if template.startswith('<class '):
						return result

					tm = var_template[span[0]:span[1]]
					for x in new_template_map:
						tm = tm.replace(x, new_template_map[x], 1).strip()

					result = result if type(result) == str else str(result)
					code = code.replace(tm, result, 1)

					template, var_template = self.parse(code)
					self.log(' template:{}, var_template:{}'.format(template, var_template))

					if self.isAssignment(code):
						self.log('  model: [assignment]')
						return

	def isPrimitive(self, code):
		if code in self.sql.all_databases:
			return self.DATABASE

		elif code in self.sql.all_tables:
			return self.TABLE

		else:
			return False

	def log(self, output=''):
		if self.log_state:
			print(output)

	def map(self, s1, s2, delimeter=" "):
		'''
		x and y are strings, the function returns the similarity ratio and
		mapping relation of the strings
		'''
		a, b = s1.split(), s2.split()
		lw = a
		if len(b) < len(a):
			lw = b

		formats = [{"a":a, "b":b, "ca":0, "cb":0}]
		for w in set(lw):
			for f in formats:
				fa, fb, fca, fcb = f["a"], f["b"], f["ca"], f["cb"]
				fac, fbc = fa.count(w), fb.count(w)
				fai = [i for i, xx in enumerate(fa) if xx == w]
				fbi = [i for i, xx in enumerate(fb) if xx == w]
				if fbc < fac:
					lwc = fbc
					fa_combs = list(combinations(fai, lwc))
					fb_combs = [fbi for _ in fa_combs]

				else:
					lwc = fac
					fb_combs = list(combinations(fbi, lwc))
					fa_combs = [fai for _ in fb_combs]
				
				new_formats = []
				for n in range(len(fa_combs)):
					ca, cb = fca, fcb
					fax, fbx = fa.copy(), fb.copy()
					for i in range(lwc):
						fax[fa_combs[n][i]] = "`"
						fbx[fb_combs[n][i]] = "`"

						# self.log(fax, fbx, fa_combs[n][i], fb_combs[n][i], ca, cb)
					new_formats.append({"a":fax, "b":fbx, "ca":ca, "cb":cb})
			formats = new_formats.copy()

		for xx in formats:
			x, y = xx["a"], xx["b"]
			
			x_map, y_map = {}, {}
			xl = delimeter.join(self.trimVars(x)).split("`")
			yl = delimeter.join(self.trimVars(y)).split("`")

			for i in range(len(xl)):
				xl[i], yl[i] = xl[i].strip(), yl[i].strip()
				if xl[i] != '' and yl[i] != '':
					x_map[xl[i]] = yl[i]
					y_map[yl[i]] = xl[i]
		return y_map

	def parse(self, code):
		code = code.split(' ')
		if len(code) == 1:
			return code[0]

		template = []
		var_template = []
		c = 0
		for x in code:
			token = x.strip()
			is_primitive = self.isPrimitive(token)
			
			if is_primitive == False:
				x = self.parse(token)
				template.append(x)
				var_template.append(x)

			else:
				var_template.append('[var]{}'.format(c))
				template.append(is_primitive)
				c += 1
		
		template = " ".join(template)
		var_template = " ".join(var_template)
		return template, var_template

	def parseTraining(self, code, execution):
		model_template, template = execution, code

		execution = execution.split(' ')
		code = code.split(' ')
		
		common_elements = set(code).intersection(set(execution))

		last_index = len(code) - 1
		last_var_index = None

		var, varz = '', []
		model = code.copy()

		for i, e in enumerate(code):
			if (e not in common_elements and last_var_index != None) or (last_var_index == None and i == last_index):
				ix = i + 1 if i == last_index else i
				last_var_index = i if i == last_index and last_var_index == None else last_var_index

				var = " ".join(code[last_var_index:ix]).strip()
				var_is_primitive = self.isPrimitive(var)

				self.log('var = {}, isPrimitive = {}'.format(var, var_is_primitive))
				if var_is_primitive:
					key = var.split()[0]
					varz.append(('{}'.format(var_is_primitive)))
					for index in range(last_var_index, ix):
						model[last_var_index] = var_is_primitive if index == last_var_index else ''
						model_template = model_template.replace(var, '[var]', 1)
						template = template.replace(var, '[var]', 1)

			if e not in common_elements:
				last_var_index = None
				continue
			
			if last_var_index == None:
				last_var_index = i

		model = " ".join(model)
		return {model:{'var_type':varz, 'code':model_template, 'template':template}}

	def prepExecution(self, execution):
		# database reference alongside
		db_ref = re.search("\s*([\w-]+)\s*\.\s*([\w-]+)\s*", execution).group()
		database, table = db_ref.strip().split('.')

		return re.sub("\s*([\w-]+)\s*\.\s*([\w-]+)\s*", ' {} {} '.format(database, table), execution).strip()

	def run(self, code, model, template_map):
		new_code, var_type = model['code'], model['var_type']

		for var in template_map:
			var_val = varx = template_map[var]
			if var.startswith('[var]'):
				var_index = int(var.replace('[var]', '', 1).strip())

			else:
				continue

			new_code = new_code.replace('[var]', varx, 1)

		self.log('    new_code:{}\n'.format(new_code))
		return self.execute(new_code, model, template_map)

	def train(self, filepath='train.txt'):
		'''
		train the model on the train.txt containing the sentence - label pair
		'''

		# open the file and read the lines of code
		with open(filepath, 'r') as file:
			training = file.readlines()

		self.log('========================================training in progress=====================================\n')
		
		# start the training pair by pair
		for training_pair in training:
			code, execution = training_pair.split('~')

			# prep the values
			code, execution = code.strip(), execution.strip()
			execution = self.prepExecution(execution)
			self.log('code = [{}], execution = {}'.format(code, execution))

			# get the parsed model
			model = self.parseTraining(code, execution)

			# print the model found
			self.log('{}\n\n'.format(model))

			# save model
			self.models.update(model)						
			self.log()
		self.log('========================================training is done=====================================\n')
		return

	def trimVars(self, map_model, var="`"):
		'''
		to remove excess <vars> from a map
		'''
		#compress the <var>s
		while '{} {}'.format(var, var) in map_model:
			map_model = map_model.replace('{} {}'.format(var, var), var)

		return map_model


def main():
	# the parser object
	parser = NLP2SQLParser(log_state=True)

	# parse the code
	code = "what currencies are in lechwegr_brollymart"
	parser.interpret(code)

if __name__ == '__main__':
	main()