"""
	Dataware house structure dependancies

	view|report|cube -> star -> fact+dims -> etl ->  

"""

class ConnectionManager(object):
	"""docstring for ConnectionManager"""
	def __init__(self, arg):
		super(ConnectionManager, self).__init__()
		self.arg = arg
		
class DataSource(ConnectionManager):
	"""DataSourceConnector is like a """
	_name = None

	def __init__(self, arg):
		super(DataSourceConnection, self).__init__()
		self.arg = arg

	def get_staging_table_name(self):
		return NotImplementedError(" error")
		
	def get_columns(self):
		return NotImplementedError(" error")

class SqlTable(object):
	_name = None
	_columns = None
	_connection = None

	def __init__(self):
		super(SqlTable, self).__init__()

	def get_create_script(self):
		return NotImplementedError(" error")

	def create_object(self):
		return NotImplementedError(" error")


class Dimension(object):
	_source_column_name = None
	_

class BusinessProcess(object):

	_data_source_connector = None
	_source_key_columns = None
	_parent_processes = None

	def __init__(self, x):
		super(DataSourceConnector, self).__init__()
		self._x = x

	def __str__(self):
		return 'self._x = {}\nself.y = {}'.format(self._x,self.y)

	def _validate_staging_table(self):
		return NotImplementedError(" error")

	def _create_staging_table(self):
		return NotImplementedError(" error")

	def deminsionalize(self):
		return NotImplementedError(" error")

	def denormalize(self):
		return NotImplementedError(" error")


x = BusinessProcess(123)
print(x)
