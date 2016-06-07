using Microsoft.SqlServer.Dts.Pipeline.Wrapper;
using Microsoft.SqlServer.Dts.Runtime;
using Microsoft.SqlServer.Management.Smo;
using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

namespace DatabaseUtilities
{
    using connection_dictionary = Dictionary<string, string>;
    public class EtlPackage : IEtlPackage
    {
        private Package _package;
        private connection_dictionary _connection_dict;
        private string _package_file_name;
        //private string _src_database_name;
        //private string _src_server_name;
        //private string _src_cm_name;
        //private string _dst_database_name;
        //private string _dst_server_name;
        //private string _dst_cm_name;
        private Regex connection_string_regex = new Regex(@"Data Source=(?<server_name>\w+);Initial Catalog=(?<database_name>\w+);Provider=SQLOLEDB.1;Integrated Security=SSPI;");
        public bool IsSaved { get; set; }
        public string package_file_name {
            get
            {
                if (_package_file_name == null)
                {
                    return string.Format("autoEtl.{0}.dtsx", DateTime.Today.ToString("YYYY-MM-DD"));
                }
                else
                {
                    return _package_file_name;
                }
            }
            set
            {
                _package_file_name = value;
            }
        }
        public EtlPackage(Package package)
        {
            _package = package;
            _package.DelayValidation = true;
            IsSaved = false;
            _connection_dict = new connection_dictionary();
            throw new NotImplementedException();
        }
        public EtlPackage(string file_name)
        {
            _package = new Package();
            _package.DelayValidation = true;
            IsSaved = false;
            _connection_dict = new connection_dictionary();
            package_file_name = file_name;
        }
        public EtlPackage()
        {
            _package = new Package();
            _package.DelayValidation = true;
            IsSaved = false;
            _connection_dict = new connection_dictionary();
            package_file_name = string.Format("autoEtl.{0}.dtsx",DateTime.Today.ToString("YYYY-MM-DD"));
            //Executable exec = _package.Executables["FLC"];
            //Executable exec2 = (exec as IDTSSequence).Executables.Add("STOCK:ScriptTask");
        }
        public string cleanSsisTaskName(string task_name)
        {
            return Regex.Replace(task_name, @"[\[\]\\\.]", "", RegexOptions.None, TimeSpan.FromSeconds(1.5));
        }
        public string savePackage(string pkg_path)
        {


            if (!IsSaved)
            {

                Application app = new Application();
                _package.Validate(_package.Connections, null, null, null);
                app.SaveToXml(pkg_path, _package, null);
                Console.WriteLine("\r\n\r\npackage saved to:\r\n{0}\r\n{1}", Directory.GetCurrentDirectory(), pkg_path);
                IsSaved = true;
            }
            return Path.Combine(pkg_path);
        }
        public string getPackageName()
        {

            //string package_name = string.Format("auto_package.dtsx");
            return _package_file_name;
        }
        public string savePackage()
        {
            return savePackage(getPackageName());
        }
        public string getConnection(string server, string database)
        {
            string connection_name = string.Format("{0}-{1}", cleanSsisTaskName(server), cleanSsisTaskName(database));
            if (_connection_dict.ContainsKey(connection_name))
            {
                return connection_name;
            }
            else
            {
                ConnectionManager newConnectionManager = _package.Connections.Add("OLEDB");
                newConnectionManager.Name = connection_name;
                string confmt = "Data Source={0};" +
                  "Initial Catalog={1};Provider=SQLOLEDB.1;" +
                  "Integrated Security=SSPI;";
                string connection_string = string.Format(confmt, server, database);
                newConnectionManager.ConnectionString = connection_string;
                _connection_dict.Add(connection_name, connection_string);
                return connection_name;
            }
            ////cleanSsisTaskName(database) + "-" + cleanSsisTaskName(database);

            ////= string.Format("{0}-{1}", cleanSsisTaskName(server), cleanSsisTaskName(database));
            //ConnectionManager newConnectionManager = null;
            //foreach (ConnectionManager connection in _package.Connections)
            //{
            //    //string connection_string = connection.ConnectionString;
            //    //Match regex_match = connection_string_regex.Match(connection_string);
            //    ////Console.WriteLine(string.Format("{0}{1}", regex_match.Groups["server_name"].Value, regex_match.Groups["database_name"].Value));
            //    //if (regex_match.Success && regex_match.Groups["server_name"].Value == server && regex_match.Groups["database_name"].Value == database)
            //    //{
            //    //    connection_name = connection.Name;
            //    //    return connection.Name;
            //    //}
            //    if (connection.Name == connection_name)
            //    {
            //        return connection.Name;
            //    }
            //}
            //if (connection_name == null)
            //{
            //    Console.WriteLine("heelo");
            //    connection_name = string.Format("{0}-{1}", cleanSsisTaskName(server), cleanSsisTaskName(database));
            //    Console.WriteLine(string.Format("loop finished: connection does not exist: {0}", connection_name));
            //    Console.WriteLine(string.Format("attempting to add new connection: {0}", connection_name));
            //    newConnectionManager = _package.Connections.Add("OLEDB");
            //    newConnectionManager.Name = connection_name;
            //    string confmt = "Data Source={0};" +
            //      "Initial Catalog={1};Provider=SQLOLEDB.1;" +
            //      "Integrated Security=SSPI;";
            //    newConnectionManager.ConnectionString = string.Format(confmt, server, database);
            //    return connection_name;
            //}
            //return connection_name;

        }
        public string getConnection(Table table)
        {
            Database database = table.Parent;
            Server server = database.Parent;
            return getConnection(server.Name, database.Name);
        }
        /// <summary>
        /// Root implementation creation of a new Data Flow Task from src_table to dst_table in execs container.  All other addDataFlowTask methods overload this method.
        /// </summary>
        /// <param name="execs">Container for executable tasks.</param>
        /// <param name="src_table"></param>
        /// <param name="dst_table"></param>
        /// <returns></returns>
        public string addDataFlowTask(Executables execs, Table src_table, Table dst_table)
        {
            // initialize required connections
            string _src_cm_name = getConnection(src_table);
            string _dst_cm_name = getConnection(dst_table);
            string _src_server_name = src_table.Parent.Parent.Name;
            string _dst_server_name = dst_table.Parent.Parent.Name;
            //Console.WriteLine("\r\n{0}.{1}.{2}.{3} -->> {4}.{5}.{6}.{7}", src_table.Parent.Parent.ToString(), src_table.Parent.ToString(), src_table.Schema, src_table.Name, dst_table.Parent.Parent.ToString(), dst_table.Parent.ToString(), dst_table.Schema, dst_table.Name);

            /* ADD DATA FLOW TASK */

            Executable e = execs.Add("STOCK:PipelineTask");
            TaskHost thMainPipe = e as TaskHost;
            thMainPipe.Name = cleanSsisTaskName(src_table.Name);
            MainPipe dataFlowTask = thMainPipe.InnerObject as MainPipe;
            // The Application object will be used to obtain the CreationName of a PipelineComponentInfo from its PipelineComponentInfos collection.
            Application app = new Application();
            thMainPipe.DelayValidation = true;
            /* ADD SOURCE COMPONENT */

            // Add an OLE DB source to the data flow the CreationName property requires an Application source_component_wrapper.
            IDTSComponentMetaData100 source_component = dataFlowTask.ComponentMetaDataCollection.New();
            source_component.ComponentClassID = app.PipelineComponentInfos["OLE DB Source"].CreationName;
            // Get the design time source_component_wrapper of the source_component.
            CManagedComponentWrapper source_component_wrapper = source_component.Instantiate();
            // Initialize the source_component
            source_component_wrapper.ProvideComponentProperties();
            // Set name of source_component
            string src_comp_name = string.Format("src {0} {1} {2}", _src_server_name, src_table.Schema, src_table.Name);
            //Console.WriteLine(src_comp_name);
            source_component.Name = cleanSsisTaskName(src_comp_name);
            // Specify the connection manager _package connections
            //Console.WriteLine(_package.Connections[_src_cm_name]);
            if (source_component.RuntimeConnectionCollection.Count > 0)
            {
                source_component.RuntimeConnectionCollection[0].ConnectionManager = DtsConvert.GetExtendedInterface(_package.Connections[_src_cm_name]);
                source_component.RuntimeConnectionCollection[0].ConnectionManagerID = _package.Connections[_src_cm_name].ID;
            }
            //foreach (IDTSRuntimeConnection100 rt_con in source_component.RuntimeConnectionCollection)
            //{
            //    System.Console.WriteLine("connection name: {0}\r\nconnection string: {1}", rt_con.Name, rt_con.ConnectionManager.ConnectionString);
            //}
            //foreach (ConnectionManager con in _package.Connections)
            //{
            //    System.Console.WriteLine("connection manager: {0}", con.Name);
            //}
            // Set the custom properties: source table query and access mode
            source_component_wrapper.SetComponentProperty("AccessMode", 2);
            string src_query = string.Format("SELECT * FROM [{0}].[{1}];", src_table.Schema, src_table.Name);
            Console.WriteLine(src_query);
            source_component_wrapper.SetComponentProperty("SqlCommand", src_query);
            // Connect to the data source table
            source_component_wrapper.AcquireConnections(null);
            // Reinitialize the metadata.
            source_component_wrapper.ReinitializeMetaData();
            source_component_wrapper.ReleaseConnections();

            /* ADD DESTINATION COMPONENT */

            // Add an OLE DB source to the data flow the CreationName property requires an Application source_component_wrapper.
            IDTSComponentMetaData100 destination_component = dataFlowTask.ComponentMetaDataCollection.New();
            destination_component.ComponentClassID = app.PipelineComponentInfos["OLE DB Destination"].CreationName;
            // Create the design-time source_component_wrapper of the destination_component.
            CManagedComponentWrapper destination_component_wrapper = destination_component.Instantiate();
            // The ProvideComponentProperties method creates a default input.
            destination_component_wrapper.ProvideComponentProperties();
            // Set name of destination_component
            string dst_comp_name = string.Format("dst - {0} {1} {2}", _dst_server_name, dst_table.Schema, dst_table.Name);
            //Console.WriteLine(dst_comp_name);
            destination_component.Name = cleanSsisTaskName(dst_comp_name);
            // Specify the connection manager from _package connections
            if (destination_component.RuntimeConnectionCollection.Count > 0)
            {
                destination_component.RuntimeConnectionCollection[0].ConnectionManager = DtsConvert.GetExtendedInterface(_package.Connections[_dst_cm_name]);
                destination_component.RuntimeConnectionCollection[0].ConnectionManagerID = _package.Connections[_dst_cm_name].ID;
            }
            //foreach (IDTSRuntimeConnection100 rt_con in source_component.RuntimeConnectionCollection)
            //{
            //    System.Console.WriteLine("connection name: {0}\r\nconnection string: {1}", rt_con.Name, rt_con.ConnectionManager.ConnectionString);
            //}
            //foreach (ConnectionManager con in _package.Connections)
            //{
            //    System.Console.WriteLine("connection manager: {0}", con.Name);
            //}
            // Set the custom properties: destination table and access mode
            destination_component_wrapper.SetComponentProperty("AccessMode", 3);
            string dst_table_str = string.Format("[{0}].[{1}]", dst_table.Schema, dst_table.Name);
            //Console.WriteLine("{0} dst_table_str", dst_table_str);
            destination_component_wrapper.SetComponentProperty("OpenRowset", dst_table_str);
            // Connect to the data destination table
            destination_component_wrapper.AcquireConnections(null);
            // Reinitialize the metadata.
            destination_component_wrapper.ReinitializeMetaData();
            destination_component_wrapper.ReleaseConnections();

            /* ADD PATH FROM source_component TO destination_component */

            // Create the path object
            IDTSPath100 path = dataFlowTask.PathCollection.New();
            path.AttachPathAndPropagateNotifications(source_component.OutputCollection[0], destination_component.InputCollection[0]);

            /* MAP COLUMNS FROM source_component TO destination_component */

            // Get the destination_component's default input and virtual input.
            IDTSInput100 input = destination_component.InputCollection[0];
            IDTSVirtualInput100 vInput = input.GetVirtualInput();

            // Iterate through the virtual input column collection.
            foreach (IDTSVirtualInputColumn100 vColumn in vInput.VirtualInputColumnCollection)
            {
                // Call the SetUsageType method of the destination_component to add each available virtual input column as an input column.
                IDTSInputColumn100 vCol = destination_component_wrapper.SetUsageType(input.ID, vInput, vColumn.LineageID, DTSUsageType.UT_READONLY);

                // check if the column match exists in the destination_component table 
                string cinputColumnName = vColumn.Name;
                var columnExist = (from item in input.ExternalMetadataColumnCollection.Cast<IDTSExternalMetadataColumn100>()
                                   where item.Name == cinputColumnName && item.DataType == vColumn.DataType
                                   select item).Count();
                // check if the column is an identity column
                var isIdentity = (from Column item in src_table.Columns
                                  where item.Identity == true
                                  && item.Name == cinputColumnName
                                  select item).Count();
                // if match then map
                if (columnExist > 0)
                {
                    if (isIdentity == 1)
                    {
                        Console.WriteLine("{0} is an identity column", src_table.Columns[cinputColumnName]);
                    }
                    destination_component_wrapper.MapInputColumn(input.ID, vCol.ID, input.ExternalMetadataColumnCollection[vColumn.Name].ID);
                    Console.WriteLine("\t{0} ({1}) => {2}", input.ExternalMetadataColumnCollection[vColumn.Name].Name, input.ExternalMetadataColumnCollection[vColumn.Name].DataType.ToString(), vColumn.Name);

                    // confirm component initialized and valid 
                    if (destination_component_wrapper.Validate() == DTSValidationStatus.VS_NEEDSNEWMETADATA)
                    {
                        Console.WriteLine("removing invalid column mapping");
                        // https://msdn.microsoft.com/en-us/library/microsoft.sqlserver.dts.pipeline.pipelinecomponent.reinitializemetadata.aspx
                        //destination_component.RemoveInvalidInputColumns();
                        //destination_component_wrapper.ReinitializeMetaData();
                    }

                }
            }

            // validate _package
            DTSExecResult validation = _package.Validate(_package.Connections, null, null, null);
            return thMainPipe.Name;
        }
        public string addDataFlowTask(Table src_table, Table dst_table)
        {
            return addDataFlowTask(_package.Executables, src_table, dst_table);
        }
        public string addDataFlowTask(string src_server_name, string src_database_name, string src_schema_name, string src_table_name, string dst_server_name, string dst_database_name, string dst_schema_name, string dst_table_name)
        {
            Table src_table = SchemaReader.getTable(src_server_name, src_database_name, src_schema_name, src_table_name);
            Table dst_table = SchemaReader.getTable(dst_server_name, dst_database_name, dst_schema_name, dst_table_name);
            return addDataFlowTask(src_table, dst_table);
        }
        public string addDataFlowTasksBySchema(string database_name, string schema_name, string src_server_name, string dst_server_name)
        {
            Microsoft.SqlServer.Dts.Runtime.Sequence seq = (Microsoft.SqlServer.Dts.Runtime.Sequence)_package.Executables.Add("STOCK:SEQUENCE");
            seq.Name = schema_name;

            Database src_database = SchemaReader.getDatabase(src_server_name, database_name);
            Database dst_database = SchemaReader.getDatabase(dst_server_name, database_name);
            foreach (Table src_table in src_database.Tables)
            {
                if (src_table.Schema == schema_name && dst_database.Tables[src_table.Name, src_table.Schema] != null)
                {
                    addDataFlowTask(seq.Executables, src_table, dst_database.Tables[src_table.Name, src_table.Schema]);
                }
            }
            return seq.Name;

        }
        public void getSequenceContainers()
        {

        }
        public void addDataFlowTasksFromExcel(string excel_path)
        {
            DataTable excel_table = ExcelHandler.readExcel(excel_path);
            
            // get list of sequence containers
            foreach (DataRow row in excel_table.Rows)
            {
                Console.WriteLine(string.Format("container: {0}",row[0]));
            }
        }
    }
}
