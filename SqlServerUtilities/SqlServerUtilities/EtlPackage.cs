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
using Microsoft.SqlServer.Dts.Tasks.BulkInsertTask;
using Microsoft.SqlServer.Dts.Tasks.FileSystemTask;
using Microsoft.SqlServer.Dts.Tasks.ExecuteSQLTask;
using Microsoft.SqlServer.Dts.Tasks.SendMailTask;

namespace SqlServerUtilities
{
    using connection_dictionary = Dictionary<string, string>;
    using Microsoft.SqlServer.Dts.Tasks.ExecuteSQLTask;
    using Sequence = Microsoft.SqlServer.Dts.Runtime.Sequence;

    /// <summary>
    /// Generates SSIS ETL *.dtsx package file.  
    /// To allow for error handling, all implementating methods use SMO objects; 
    /// overloading string based methods create corresponding SMO objects and call SMO based methods.
    /// </summary>
    public class EtlPackage
    {
        private Package _package;
        private connection_dictionary _connection_dict;
        private string _package_file_name;
        private Regex connection_string_regex = new Regex(@"Data Source=(?<server_name>\w+);Initial Catalog=(?<database_name>\w+);Provider=SQLOLEDB.1;Integrated Security=SSPI;");
        public bool IsSaved { get; set; }
        public Application app { get; set; }
        public List<string> DestinationTables;
        public string package_file_name
        {
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
        public string cleanSsisTaskName(string task_name)
        {
            return Regex.Replace(task_name, @"[\[\]\\\.]", "", RegexOptions.None, TimeSpan.FromSeconds(1.5));
        }
        //public EtlPackage(string src_server_name, string dst_server_name, string database_name)
        //{
        //    _package = new Package();
        //    _package.DelayValidation = true;
        //    IsSaved = false;
        //    _connection_dict = new connection_dictionary();
        //    package_file_name = string.Format("{0} {1}-{2}.dtsx", database_name, src_server_name, dst_server_name);
        //    addDataFlowTasksBySchema(database_name, src_server_name, dst_server_name);
        //}
        public EtlPackage(string file_name)
        {
            _package = new Package();
            app = new Application();

            _package.DelayValidation = true;
            IsSaved = false;
            _connection_dict = new connection_dictionary();
            package_file_name = file_name;
        }
        public EtlPackage(string ssisPath, string serverName, IDTSEvents events)
        {
            app = new Application();

            _package = app.LoadFromDtsServer(ssisPath, serverName, events);
            _package.DelayValidation = true;
            IsSaved = false;
            _connection_dict = new connection_dictionary();
            package_file_name = string.Format("{0}.dtsx", _package.Name);
            //Executable exec = _package.Executables["FLC"];
            //Executable exec2 = (exec as IDTSSequence).Executables.Add("STOCK:ScriptTask");
        }
        public EtlPackage()
        {
            app = new Application();
            _package = new Package();
            _package.DelayValidation = true;
            IsSaved = false;
            _connection_dict = new connection_dictionary();
            package_file_name = string.Format("autoEtl.{0}.dtsx", DateTime.Today.ToString("YYYY-MM-DD"));
            //Executable exec = _package.Executables["FLC"];
            //Executable exec2 = (exec as IDTSSequence).Executables.Add("STOCK:ScriptTask");
        }

        public string savePackage()
        {
            return savePackage(package_file_name);
        }
        public string savePackage(string package_name)
        {
            package_file_name = package_name;
            _package.Name = Path.GetFileNameWithoutExtension(package_file_name);
            if (!IsSaved)
            {
                _package.Validate(_package.Connections, null, null, null);
                app.SaveToXml(package_name, _package, null);
                Console.WriteLine("\r\n\r\npackage saved to:\r\n{0}\r\n{1}", Directory.GetCurrentDirectory(), package_name);
                IsSaved = true;
            }
            return Path.Combine(package_name);
        }
        public string addConnection(Database database)
        {
            string server_name = database.Parent.Name;
            string database_name = database.Name;
            string connection_name = string.Format("{0}-{1}", cleanSsisTaskName(server_name), cleanSsisTaskName(database_name));
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
                string connection_string = string.Format(confmt, server_name, database_name);
                newConnectionManager.ConnectionString = connection_string;
                _connection_dict.Add(connection_name, connection_string);
                return connection_name;
            }
            ////cleanSsisTaskName(database_name) + "-" + cleanSsisTaskName(database_name);

            ////= string.Format("{0}-{1}", cleanSsisTaskName(server_name), cleanSsisTaskName(database_name));
            //ConnectionManager newConnectionManager = null;
            //foreach (ConnectionManager connection in _package.Connections)
            //{
            //    //string connection_string = connection.ConnectionString;
            //    //Match regex_match = connection_string_regex.Match(connection_string);
            //    ////Console.WriteLine(string.Format("{0}{1}", regex_match.Groups["server_name"].Value, regex_match.Groups["database_name"].Value));
            //    //if (regex_match.Success && regex_match.Groups["server_name"].Value == server_name && regex_match.Groups["database_name"].Value == database_name)
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
            //    connection_name = string.Format("{0}-{1}", cleanSsisTaskName(server_name), cleanSsisTaskName(database_name));
            //    Console.WriteLine(string.Format("loop finished: connection does not exist: {0}", connection_name));
            //    Console.WriteLine(string.Format("attempting to add new connection: {0}", connection_name));
            //    newConnectionManager = _package.Connections.Add("OLEDB");
            //    newConnectionManager.Name = connection_name;
            //    string confmt = "Data Source={0};" +
            //      "Initial Catalog={1};Provider=SQLOLEDB.1;" +
            //      "Integrated Security=SSPI;";
            //    newConnectionManager.ConnectionString = string.Format(confmt, server_name, database_name);
            //    return connection_name;
            //}
            //return connection_name;
        }
        public string addConnection(Table table)
        {
            Database database = table.Parent;
            Server server = database.Parent;
            return addConnection(server.Name, database.Name);
        }
        public string addConnection(string server_name, string database_name)
        {
            Database database = SchemaReader.getDatabase(server_name, database_name);
            return addConnection(database);
        }
        public string getTaskName()
        {
            return "";
        }
        /// <summary>
        /// Root implementation creation of a new Data Flow Task from src_table to dst_table in execs container.  All other addDataFlowTask methods overload this method.
        /// </summary>
        /// <param name="execs">Container for executable tasks.</param>
        /// <param name="src_table"></param>
        /// <param name="dst_table"></param>
        /// <returns></returns>
        public TaskHost addDataFlowTask(Executables execs, Table src_table, Table dst_table)
        {
            // initialize required connections
            string _src_cm_name = addConnection(src_table);
            string _dst_cm_name = addConnection(dst_table);
            string _src_server_name = src_table.Parent.Parent.Name;
            string _dst_server_name = dst_table.Parent.Parent.Name;
            //Console.WriteLine("\r\n{0}.{1}.{2}.{3} -->> {4}.{5}.{6}.{7}", src_table.Parent.Parent.ToString(), src_table.Parent.ToString(), src_table.Schema, src_table.Name, dst_table.Parent.Parent.ToString(), dst_table.Parent.ToString(), dst_table.Schema, dst_table.Name);

            /* ADD DATA FLOW TASK */

            Executable e = execs.Add("STOCK:PipelineTask");
            TaskHost th = e as TaskHost;
            th.Name = cleanSsisTaskName(src_table.Name);
            MainPipe dataFlowTask = th.InnerObject as MainPipe;
            // The Application object will be used to obtain the CreationName of a PipelineComponentInfo from its PipelineComponentInfos collection.
            //app = new Application();
            th.DelayValidation = true;
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
            // Set the custom properties: source view query and access mode
            source_component_wrapper.SetComponentProperty("AccessMode", 2);
            string src_query = string.Format("SELECT * FROM [{0}].[{1}];", src_table.Schema, src_table.Name);
            Console.WriteLine(src_query);
            source_component_wrapper.SetComponentProperty("SqlCommand", src_query);
            // Connect to the data source view
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
            // Set the custom properties: destination view and access mode
            destination_component_wrapper.SetComponentProperty("AccessMode", 3);
            string dst_table_str = string.Format("[{0}].[{1}]", dst_table.Schema, dst_table.Name);
            //Console.WriteLine("{0} dst_table_str", dst_table_str);
            destination_component_wrapper.SetComponentProperty("OpenRowset", dst_table_str);
            // Connect to the data destination view
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

                // check if the column match exists in the destination_component view 
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
            return th;
        }
        public TaskHost addDataFlowTask(Table src_table, Table dst_table)
        {
            return addDataFlowTask(_package.Executables, src_table, dst_table);
        }
        public TaskHost addDataFlowTask(string src_server_name, string src_database_name, string src_schema_name, string src_table_name, string dst_server_name, string dst_database_name, string dst_schema_name, string dst_table_name)
        {
            Table src_table = SchemaReader.getTable(src_server_name, src_database_name, src_schema_name, src_table_name);
            Table dst_table = SchemaReader.getTable(dst_server_name, dst_database_name, dst_schema_name, dst_table_name);
            return addDataFlowTask(src_table, dst_table);
        }
        public void getExecutables()
        {
            Executables execs = _package.Executables;
            getExecutables(execs);
        }
        public void getExecutables(Executables execs)
        {
            foreach (Executable e in execs)
            {
                Console.WriteLine(string.Format("\n\n\te.GetType() = {0}", e.GetType()));
                if (e.GetType() == typeof(TaskHost))
                {
                    //Console.WriteLine(string.Format("\n\tTaskHost"));
                    TaskHost th = e as TaskHost;
                    //Console.WriteLine(string.Format("\t\t\tTaskHost th = e as TaskHost;"));
                    Console.WriteLine(string.Format("\t\t\tName = {0}", th.Name));


                    //Console.WriteLine(string.Format("\t\t\tDescription = {0}", th.Description));
                    //Console.WriteLine(string.Format("\tDescription = {0}", th.Description));
                    //Console.WriteLine(string.Format("\tGetExecutionPath() = {0}", th.GetExecutionPath()));
                    //Console.WriteLine(string.Format("\tGetType() = {0}", th.GetType()));
                    //Console.WriteLine(string.Format("\tInnerObject.ToString() = {0}", th.InnerObject.ToString()));
                    if (th.InnerObject.GetType() == typeof(ExecuteSQLTask))
                    {
                        Console.WriteLine(string.Format("\t\t\tExecuteSQLTask"));
                    }
                    else
                    {
                        //Console.WriteLine(string.Format("\tGetType() = {0}", th.GetType()));
                        //Console.WriteLine(string.Format("\t\t\tInnerObject.GetType() = {0}", th.InnerObject.GetType()));
                        //Console.WriteLine(string.Format("\t\t\tInnerObject.ToString() = {0}", th.InnerObject.ToString()));
                    }
                    //foreach (DtsProperty prop in th.Properties)
                    //{
                    //    Console.WriteLine(string.Format("\t\tproperty: {0} (type={1}, connectionManager={2})", prop.Name, prop.Type, prop.ConnectionType));
                    //    //if (prop.Name == "ComponentMetaDataCollection")
                    //    //{
                    //    //    IDTSComponentMetaData100 destination_component = prop.GetValue(prop) as IDTSComponentMetaData100;
                    //    //}
                    //}
                    MainPipe mp = th.InnerObject as MainPipe;
                    if (mp != null)
                    {
                        //Console.WriteLine(string.Format("\t\tmp.ToString() = {0}", mp.ToString()));
                        foreach (IDTSComponentMetaData100 mdcol in mp.ComponentMetaDataCollection)
                        {
                            if (mdcol.Description == "OLE DB Destination")
                            {
                                IDTSCustomPropertyCollection100 props = mdcol.CustomPropertyCollection;
                                foreach (IDTSCustomProperty100 prop in props)
                                {
                                    if (prop.Name == "OpenRowset")
                                    {
                                        Console.WriteLine(string.Format("\t\t\tValue = {0}", prop.Value));
                                    }
                                }
                                IDTSRuntimeConnectionCollection100 conns = mdcol.RuntimeConnectionCollection;
                                foreach (IDTSRuntimeConnection100 conn in conns)
                                {
                                    if (conn.Name == "OleDbConnection")
                                    {
                                        //Console.WriteLine(string.Format("\t\t\tOleDbConnection"));
                                        Console.WriteLine(string.Format("\t\t\tName = {0}", conn.Name));
                                        //Console.WriteLine(string.Format("\t\t\tDescription = {0}", conn.Description));
                                        //Console.WriteLine(string.Format("\t\t\tConnectionManagerID = {0}", conn.ConnectionManagerID));
                                        foreach (ConnectionManager conmgr in _package.Connections)
                                        {
                                            if (conmgr.ID == conn.ConnectionManagerID)
                                            {
                                                //Console.WriteLine(string.Format("\t\t\tConnectionManager conmgr"));
                                                //Console.WriteLine(string.Format("\t\t\tName = {0}", conmgr.Name));
                                                //Console.WriteLine(string.Format("\t\t\tToString = {0}", conmgr.ToString()));
                                                //Console.WriteLine(string.Format("\t\t\tID = {0}", conmgr.ID));
                                                //Console.WriteLine(string.Format("\t\t\tCreationName = {0}", conmgr.CreationName));
                                                Console.WriteLine(string.Format("\t\t\tConnectionString = {0}", conmgr.ConnectionString));
                                                Console.WriteLine(string.Format("\t\t\tDatabase = {0}", CommonUtils.CommonUtils.extractDatabaseName(conmgr.ConnectionString)));
                                            }

                                        }
                                    }
                                }
                            }
                            //Console.WriteLine(string.Format("mdcol = mp.ComponentMetaDataCollection"));
                            //Console.WriteLine(string.Format("Name = {0}", mdcol.Name));
                            //Console.WriteLine(string.Format("Description = {0}", mdcol.Description));
                            //Console.WriteLine(string.Format("ComponentClassID = {0}", mdcol.ComponentClassID));
                            //Console.WriteLine(string.Format("mdcol.InputCollection"));
                            //Console.WriteLine(string.Format("Count = {0}", mdcol.InputCollection.Count));
                            //Console.WriteLine(string.Format("ToString() = {0}", mdcol.InputCollection.ToString()));
                            //Console.WriteLine(string.Format("mdcol.OutputCollection"));
                            //Console.WriteLine(string.Format("Count = {0}", mdcol.OutputCollection.Count));
                            //Console.WriteLine(string.Format("ToString() = {0}", mdcol.OutputCollection.ToString()));

                            //IDTSCustomPropertyCollection100 props = mdcol.CustomPropertyCollection;
                            //Console.WriteLine(string.Format("IDTSCustomPropertyCollection100 props = mdcol.CustomPropertyCollection;"));
                            //foreach (IDTSCustomProperty100 prop in props)
                            //{
                            //    Console.WriteLine(string.Format("IDTSCustomProperty100"));
                            //    Console.WriteLine(string.Format("Name = {0}",prop.Name));
                            //    Console.WriteLine(string.Format("Value = {0}",prop.Value));
                            //}

                            //IDTSPathCollection100 pc = mp.PathCollection;
                            //Console.WriteLine(string.Format("IDTSPathCollection100 pc = mp.PathCollection"));
                            //Console.WriteLine(string.Format("GetType() = {0}", pc.GetType()));
                            //Console.WriteLine(string.Format("ToString() = {0}", pc.ToString()));
                        }
                    }

                                        
                }
                else if (e.GetType() == typeof(Sequence))
                {
                    Console.WriteLine(string.Format("\tSequence"));
                    Sequence seq = e as Sequence;
                    Console.WriteLine(string.Format("\t\tName = {0}", seq.Name));
                    //Console.WriteLine(string.Format("\tGetExecutionPath() = {0}", seq.GetExecutionPath()));
                    getExecutables(seq.Executables);
                }
                else if (e.GetType() == typeof(ForEachLoop))
                {
                    Console.WriteLine(string.Format("\tForEachLoop", e.GetType()));
                    ForEachLoop loop = e as ForEachLoop;
                    Console.WriteLine(string.Format("\t\tName = {0}", loop.Name));
                    //Console.WriteLine(string.Format("\t\tGetExecutionPath() = {0}", loop.GetExecutionPath()));
                    getExecutables(loop.Executables);
                }
            }
        }
        public void getDestinationTables()
        {
            DestinationTables = new List<string>();
            getDestinationTables(_package.Executables);
        }
        public void getDestinationTables(Executables execs)
        {
            foreach (Executable exec in execs)
            {
                if (exec.GetType() == typeof(TaskHost))
                {
                    //Console.WriteLine(string.Format("\n\tTaskHost"));
                    TaskHost th = exec as TaskHost;
                    //Console.WriteLine(string.Format("\t\t\tTaskHost th = e as TaskHost;"));
                    //Console.WriteLine(string.Format("\t\t\tName = {0}", th.Name));


                    //Console.WriteLine(string.Format("\t\t\tDescription = {0}", th.Description));
                    //Console.WriteLine(string.Format("\tDescription = {0}", th.Description));
                    //Console.WriteLine(string.Format("\tGetExecutionPath() = {0}", th.GetExecutionPath()));
                    //Console.WriteLine(string.Format("\tGetType() = {0}", th.GetType()));
                    //Console.WriteLine(string.Format("\tInnerObject.ToString() = {0}", th.InnerObject.ToString()));
                    //if (th.InnerObject.GetType() == typeof(ExecuteSQLTask))
                    //{
                    //    Console.WriteLine(string.Format("\t\t\tExecuteSQLTask"));
                    //}
                    //else
                    //{
                    //    //Console.WriteLine(string.Format("\tGetType() = {0}", th.GetType()));
                    //    //Console.WriteLine(string.Format("\t\t\tInnerObject.GetType() = {0}", th.InnerObject.GetType()));
                    //    //Console.WriteLine(string.Format("\t\t\tInnerObject.ToString() = {0}", th.InnerObject.ToString()));
                    //}
                    //foreach (DtsProperty prop in th.Properties)
                    //{
                    //    Console.WriteLine(string.Format("\t\tproperty: {0} (type={1}, connectionManager={2})", prop.Name, prop.Type, prop.ConnectionType));
                    //    //if (prop.Name == "ComponentMetaDataCollection")
                    //    //{
                    //    //    IDTSComponentMetaData100 destination_component = prop.GetValue(prop) as IDTSComponentMetaData100;
                    //    //}
                    //}
                    MainPipe mp = th.InnerObject as MainPipe;
                    string databaseName = "";
                    string tableName = "";
                    if (mp != null)
                    {
                        //Console.WriteLine(string.Format("\t\tmp.ToString() = {0}", mp.ToString()));
                        foreach (IDTSComponentMetaData100 mdcol in mp.ComponentMetaDataCollection)
                        {
                            if (mdcol.Description == "OLE DB Destination")
                            {
                                IDTSCustomPropertyCollection100 props = mdcol.CustomPropertyCollection;
                                foreach (IDTSCustomProperty100 prop in props)
                                {
                                    if (prop.Name == "OpenRowset")
                                    {
                                        Console.WriteLine(string.Format("\t\t\tValue = {0}", prop.Value));
                                        tableName = prop.Value;
                                    }
                                }
                                IDTSRuntimeConnectionCollection100 conns = mdcol.RuntimeConnectionCollection;
                                foreach (IDTSRuntimeConnection100 conn in conns)
                                {
                                    if (conn.Name == "OleDbConnection")
                                    {
                                        //Console.WriteLine(string.Format("\t\t\tOleDbConnection"));
                                        //Console.WriteLine(string.Format("\t\t\tName = {0}", conn.Name));
                                        //Console.WriteLine(string.Format("\t\t\tDescription = {0}", conn.Description));
                                        //Console.WriteLine(string.Format("\t\t\tConnectionManagerID = {0}", conn.ConnectionManagerID));
                                        foreach (ConnectionManager conmgr in _package.Connections)
                                        {
                                            if (conmgr.ID == conn.ConnectionManagerID)
                                            {
                                                //Console.WriteLine(string.Format("\t\t\tConnectionManager conmgr"));
                                                //Console.WriteLine(string.Format("\t\t\tName = {0}", conmgr.Name));
                                                //Console.WriteLine(string.Format("\t\t\tToString = {0}", conmgr.ToString()));
                                                //Console.WriteLine(string.Format("\t\t\tID = {0}", conmgr.ID));
                                                //Console.WriteLine(string.Format("\t\t\tCreationName = {0}", conmgr.CreationName));
                                                //Console.WriteLine(string.Format("\t\t\tConnectionString = {0}", conmgr.ConnectionString));
                                                Console.WriteLine(string.Format("\t\t\tDatabase = {0}", CommonUtils.CommonUtils.extractDatabaseName(conmgr.ConnectionString)));
                                                databaseName = CommonUtils.CommonUtils.extractDatabaseName(conmgr.ConnectionString);
                                                tableName = databaseName + "." + tableName;
                                                DestinationTables.Add(tableName);
                                            }

                                        }
                                    }
                                }
                            }
                            //Console.WriteLine(string.Format("mdcol = mp.ComponentMetaDataCollection"));
                            //Console.WriteLine(string.Format("Name = {0}", mdcol.Name));
                            //Console.WriteLine(string.Format("Description = {0}", mdcol.Description));
                            //Console.WriteLine(string.Format("ComponentClassID = {0}", mdcol.ComponentClassID));
                            //Console.WriteLine(string.Format("mdcol.InputCollection"));
                            //Console.WriteLine(string.Format("Count = {0}", mdcol.InputCollection.Count));
                            //Console.WriteLine(string.Format("ToString() = {0}", mdcol.InputCollection.ToString()));
                            //Console.WriteLine(string.Format("mdcol.OutputCollection"));
                            //Console.WriteLine(string.Format("Count = {0}", mdcol.OutputCollection.Count));
                            //Console.WriteLine(string.Format("ToString() = {0}", mdcol.OutputCollection.ToString()));

                            //IDTSCustomPropertyCollection100 props = mdcol.CustomPropertyCollection;
                            //Console.WriteLine(string.Format("IDTSCustomPropertyCollection100 props = mdcol.CustomPropertyCollection;"));
                            //foreach (IDTSCustomProperty100 prop in props)
                            //{
                            //    Console.WriteLine(string.Format("IDTSCustomProperty100"));
                            //    Console.WriteLine(string.Format("Name = {0}",prop.Name));
                            //    Console.WriteLine(string.Format("Value = {0}",prop.Value));
                            //}

                            //IDTSPathCollection100 pc = mp.PathCollection;
                            //Console.WriteLine(string.Format("IDTSPathCollection100 pc = mp.PathCollection"));
                            //Console.WriteLine(string.Format("GetType() = {0}", pc.GetType()));
                            //Console.WriteLine(string.Format("ToString() = {0}", pc.ToString()));
                        }
                    }

                                        
                }
                else if (exec.GetType() == typeof(Sequence))
                {
                    //Console.WriteLine(string.Format("\tSequence"));
                    Sequence seq = exec as Sequence;
                    //Console.WriteLine(string.Format("\t\tName = {0}", seq.Name));
                    //Console.WriteLine(string.Format("\tGetExecutionPath() = {0}", seq.GetExecutionPath()));
                    getDestinationTables(seq.Executables);
                }
                else if (exec.GetType() == typeof(ForEachLoop))
                {
                    //Console.WriteLine(string.Format("\tForEachLoop", exec.GetType()));
                    ForEachLoop loop = exec as ForEachLoop;
                    //Console.WriteLine(string.Format("\t\tName = {0}", loop.Name));
                    //Console.WriteLine(string.Format("\t\tGetExecutionPath() = {0}", loop.GetExecutionPath()));
                    getDestinationTables(loop.Executables);
                }
            }
        }
        public TaskHost addSqlTask(Executables execs, Database database, string task_name, string sql_source)
        {
            Executable e = execs.Add("STOCK:SQLTask");
            TaskHost th = e as TaskHost;

            ExecuteSQLTask sql_task = th.InnerObject as ExecuteSQLTask;
            sql_task.SqlStatementSourceType = SqlStatementSourceType.DirectInput;
            sql_task.SqlStatementSource = sql_source;
            sql_task.Connection = addConnection(database.Parent.Name, database.Name);
            th.Name = task_name;
            //Console.WriteLine("BypassPrepare          {0}", th.Properties["BypassPrepare"].GetValue(th));
            //Console.WriteLine("CodePage               {0}", th.Properties["CodePage"].GetValue(th));
            //Console.WriteLine("Connection             {0}", th.Properties["Connection"].GetValue(th));
            //Console.WriteLine("ExecutionValue         {0}", th.Properties["ExecutionValue"].GetValue(th));
            //Console.WriteLine("IsStoredProcedure      {0}", th.Properties["IsStoredProcedure"].GetValue(th));
            //Console.WriteLine("ParameterBindings      {0}", th.Properties["ParameterBindings"].GetValue(th));
            //Console.WriteLine("ResultSetBindings      {0}", th.Properties["ResultSetBindings"].GetValue(th));
            //Console.WriteLine("ResultSetType          {0}", th.Properties["ResultSetType"].GetValue(th));
            //Console.WriteLine("SqlStatementSource     {0}", th.Properties["SqlStatementSource"].GetValue(th));
            //Console.WriteLine("SqlStatementSourceType {0}", th.Properties["SqlStatementSourceType"].GetValue(th));
            //Console.WriteLine("TimeOut                {0}", th.Properties["TimeOut"].GetValue(th));

            //Variable myVar = _package.Variables.Add("myVar", false, "User", 100);
            //th.Properties["SqlStatementSourceType"].SetValue(th, SqlStatementSourceType.Variable);
            //th.Properties["SqlStatementSource"].SetValue(th, "myVar");
            //th.Properties["ResultSetType"].SetValue(th, ResultSetType.ResultSetType_XML);

            //Console.WriteLine("New value of Source and SourceType:  {0}, {1}", th.Properties["SqlStatementSource"].GetValue(th), th.Properties["SqlStatementSourceType"].GetValue(th));
            //Console.WriteLine("New value of ResultSetType:  {0}", th.Properties["ResultSetType"].GetValue(th), th.Properties["SqlStatementSourceType"].GetValue(th));
            return th;
        }
        public TaskHost addSqlTask(Executables execs, string server_name, string database_name, string task_name, string sql_source)
        {
            Database database = SchemaReader.getDatabase(server_name, database_name);
            return addSqlTask(execs, database, task_name, sql_source);
        }
        public TaskHost addSqlTask(string server_name, string database_name, string task_name, string sql_source)
        {
            return addSqlTask(_package.Executables, server_name, database_name, task_name, sql_source);
        }

        public Sequence addSequence(Executables execs, string sequence_name)
        {
            //(Microsoft.SqlServer.Dts.Runtime.Sequence)
            Executable e = execs.Add("STOCK:SEQUENCE");
            Sequence seq = e as Sequence;
            seq.Name = cleanSsisTaskName(sequence_name);
            return seq;
        }
        public Sequence addSequence(string sequence_name)
        {
            return addSequence(_package.Executables, sequence_name);
        }

        /* Convience methods
         * 
         * 
         */
        public string addDataFlowTasksBySchema(string database_name, string schema_name, string src_server_name, string dst_server_name)
        {
            Sequence seq = addSequence(schema_name);

            Database src_database = SchemaReader.getDatabase(src_server_name, database_name);
            Database dst_database = SchemaReader.getDatabase(dst_server_name, database_name);
            foreach (Table src_table in src_database.Tables)
            {
                if (src_table.Schema == schema_name && dst_database.Tables[src_table.Name, src_table.Schema] != null)
                {
                    addTruncatePopulate(seq.Executables, src_table, dst_database.Tables[src_table.Name, src_table.Schema]);
                }
            }
            return seq.Name;

        }
        public void addDataFlowTasksBySchema(string database_name, string src_server_name, string dst_server_name)
        {
            Database database = SchemaReader.getDatabase(src_server_name, database_name);
            foreach (Schema schema in database.Schemas)
            {
                addDataFlowTasksBySchema(database_name, schema.Name, src_server_name, dst_server_name);
            }
        }
        public Sequence addTruncatePopulate(Executables execs, Table src_table, Table dst_table)
        {
            Sequence seq = addSequence(execs, src_table.Name);

            TaskHost th_sql = addSqlTask(seq.Executables, dst_table.Parent, string.Format("TRUNCATE TABLE {0}-{1}", dst_table.Schema, dst_table.Name), string.Format("TRUNCATE TABLE {0}.{1}", dst_table.Schema, dst_table.Name));
            ExecuteSQLTask sql = th_sql.InnerObject as ExecuteSQLTask;
            Executable e_sql = th_sql as Executable;

            TaskHost th_df = addDataFlowTask(seq.Executables, src_table, dst_table);
            MainPipe mp = th_df.InnerObject as MainPipe;
            Executable e_df = th_df as Executable;

            PrecedenceConstraint pre_const = seq.PrecedenceConstraints.Add(e_sql, e_df);
            pre_const.Name = dst_table.Name;

            return seq;
        }
        public Sequence addTruncatePopulate(Executables execs, string src_server_name, string src_database_name, string src_schema_name, string src_table_name, string dst_server_name, string dst_database_name, string dst_schema_name, string dst_table_name)
        {
            Table dst_table = SchemaReader.getTable(dst_server_name, dst_database_name, dst_schema_name, dst_table_name);
            Table src_table = SchemaReader.getTable(src_server_name, src_database_name, src_schema_name, src_table_name);
            return addTruncatePopulate(execs, src_table, dst_table);
        }
    }
}
