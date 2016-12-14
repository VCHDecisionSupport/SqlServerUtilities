from sqlalchemy import *
import pyodbc

metadata = MetaData()

mssql_connection_string = r"DRIVER={SQL Server Native Client 11.0};SERVER=PC;DATABASE=Staging;Trusted_Connection=yes;"
#engine = create_engine('mssql+pyodbc://scott:tiger@mydsn')
conn = pyodbc.connect(mssql_connection_string)
print(conn)

cursor = conn.cursor()
cursor.execute("SELECT * FROM Statement")
while 1:
    row = cursor.fetchone()
    if not row:
        break
    print(row.import_str)
cnxn.close()

engine = create_engine('sqlite:///:memory:', echo=True)
print(engine)