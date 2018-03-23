#Python script that takes an input sql file, finds the parameters in it and queries the runner about the values for those parameters
#Writes a SQL file ot the output folder with the parameters replaced and notes the name of that file in ~/code/sql/runner/.file.meta
#Stores the most recently used set of parameters for each SQL in the file ~/code/sql/runner/.param.hist

import json
import sys
import re
import psycopg2
import psycopg2.extras
import pandas as pd
import pandas.io.sql as psql

def get_cols(sql):
	rets=[]
	for line in sql.split("\n"):
		if line.strip()=="":
			pass
		elif line.strip().lower()=="select":
			pass
		elif line.strip().lower()[:4]=="from":
			return rets
		else:
			rets.append(line.split(" as ")[-1].strip().replace(",",""))
	return rets

#Takes a SQL statement and returns a list of parameters, no parameters returns an empty list
def find_params(sql):	
	params=list(set(re.findall('\$([^\$]+)\$',sql)))
	params.sort()
	return params

#Takes a SQL statement and dictionary of parameter:value pairs and applies them, returning the modified SQL
def apply_params(sql,params):
	for param in params:
		sql=sql.replace('$'+param+'$',mappings[param])
	return sql
#Runs a SQL statement (with markup) and returns a list of dataframes with the data
def run_query(sql,conn):
	lines = sql.split("\n")
	mode=0
	transformed_sql=""
	pivot_sql=""
	order_sql=""
	cur_sql = ""
	output = []

	for line in lines:
		if line.strip()=="":
			pass
		elif mode==0 and line.strip()=="<pivot>":
			mode=1
		elif mode==0 and line.strip()[-1]==";":		
			#Add line to cur_sql, add cur_sql to transformed_sql
			cur_sql+=line+"\n"
			transformed_sql+=cur_sql
			#Run cur_sql and re-set to "", 
			try:
				df = psql.read_sql(cur_sql,conn)
				output.append(df)
			except TypeError:
				pass
			cur_sql=""
		elif mode==0:
			cur_sql+=line+"\n"
		elif mode==1 and line.strip()=="<by>":
			mode=2
		elif mode==1 and line.strip()=='</pivot>':
			#Execute pivot SQL and use pandas pivot
			pivot_columns = get_cols(pivot_sql)
			try:
				df = psql.read_sql(pivot_sql,conn)
				pivoted = df.pivot(index=pivot_columns[0],columns=pivot_columns[1],values=pivot_columns[2])
				output.append(pivoted)
			except TypeError:
				pass
			#Write pivot_sql to transformed SQL w/ note about pivoting
			transformed_sql+="/*The following SQL was pivoted in its output*/"+"\n"
			transformed_sql+=pivot_sql
			pivot_sql=""
			#Reset mode to 0
			mode=0
		elif mode==1:
			pivot_sql+=line+"\n"
		elif mode==2 and line.strip()=="</pivot>":
			#DOUBLECHECK
			#Run order_sql, Create Custom_sql, run custom_sql, grab output
			#Write custom SQL to transformed SQL w/ note about being pivoted
			#reset mode to 0

			#Execute pivot
			cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
			try:
				df = psql.read_sql(order_sql,conn)
			except TypeError:
				pass
			
			#create custom sql
			pivot_columns = get_cols(pivot_sql)
			created_sql="select"+"\n"
			created_sql+="\t"+pivot_columns[0]+","+"\n"
			column_value = pivot_columns[1]
			fill_value = pivot_columns[2]
			order_value = None
			if len(pivot_columns)>3:
				order_value = pivot_columns[3]
			for i in df[df.columns[0]]:
				created_sql+="\t"+"max(case when "+column_value+"::varchar = '"+i+"' then "+fill_value+" end) as \""+i+"\",\n"
			created_sql=created_sql[0:-2]+"\n"
			created_sql+="from (\n"
			for i in pivot_sql.split("\n"):
				if i.strip() != ";":
					created_sql+=i+"\n"
			created_sql+=") as a"+"\n"
			created_sql+="group by 1\n"
			if order_value is not None:
				created_sql+="order by max("+order_value+")\n"
			created_sql+=";"
			transformed_sql+=created_sql+"\n"
			try:
				df = psql.read_sql(created_sql,conn)
				output.append(df)
			except TypeError:
				pass
			created_sql=""
			pivot_sql=""
			order_sql=""
			mode=0
		elif mode==2:
			order_sql+=line+"\n"
		else:
			transformed_sql+=line+"\n"
	return output

#Prints a dataframe nicely, replacing Nones with empty string and nice column/row separators
def pretty_print(df):
	print(df)



if __name__ == "__main__":
	if len(sys.argv)<=1:
		print("No File Specified",file=sys.stderr)
		parse(None)
		sys.exit(1)

	mode=0
	inparams = None
	infile=None
	i=1
	while i < len(sys.argv):
		if sys.argv[i]=='-i':
			mode=1		
		elif sys.argv[i]=='-f':
			mode=2
			pfile=sys.argv[i+1]
			with open(pfile,'r') as p:
				inparams=json.loads(p.read())
			i+=1
		elif sys.argv[i]=='-l':
			mode=3
		else:
			infile = sys.argv[i]
		i+=1

	with open(infile,'r') as f:
		#Load Parameter History for this File
		param_hist=None
		try:
			param_hist=json.loads(open(".params.json").read())
		except Error:
			param_hist={}
		sql_file=infile.split("/")[-1]
		if sql_file in param_hist:
			last_params=param_hist[sql_file]
		else:
			last_params=None

		#Find Parameters in SQL
		sql = f.read()
		params=find_params(sql)

		mappings = {}
		if mode==0:
			#Query user re: params and create a dict of substitutions to make
			for param in params:
				if last_params is None or param not in last_params:
					value = input("Input a value for "+param+": ")
				else:
					temp_value = input("Input a value for "+param+" or leave blank to use <"+last_params[param]+">:")
					if temp_value is None or temp_value=="":
						value=last_params[param]
					else:
						value=temp_value
				mappings[param]=value
		elif mode==2:
			for param in params:
				if param in inparams:
					mappings[param]=inparams[param]
				else:
					print(param,"not found", file=sys.stderr)
					quit()
		elif mode==3:
			for param in params:
				if param in last_params:
					mappings[param]=last_params[param]
				else:
					print(param,"not found", file=sys.stderr)
					quit()
				
		#Store new Param Values
		param_hist[sql_file]=mappings
		with open(".params.json","w") as new_param_hist:
			new_param_hist.write(json.dumps(param_hist))

		#Generate new SQL and write it to _temp file
		sql=apply_params(sql,mappings)
		out_sql="/home/henry/code/util/runner/output/"+sql_file.replace(".sql","_temp.sql")
		with open(out_sql,'w') as f2:
			f2.write('\\timing\n\n')
			f2.write(sql)
			f2.close()
		
		conn = psycopg2.connect("dbname='scraping' user='henry'")
		data=run_query(sql,conn)

		#with open(out_sql,'w') as f2:
		#	f2.write('\\timing\n\n')
		#	f2.write(transformed_sql)
		#	f2.close()

		for i in data:
			pretty_print(i)

