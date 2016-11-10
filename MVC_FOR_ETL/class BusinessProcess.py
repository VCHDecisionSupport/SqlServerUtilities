class DataSourceConnector(object):
	"""docstring for DataSourceConnector"""

	_name = None
	_connection_string = None

	def __init__(self, arg):
		super(DataSourceConnector, self).__init__()
		self.arg = arg

	def get_staging_table_name(self):
		return NotImplementedError(" error")
		
	def get_columns(self):
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
