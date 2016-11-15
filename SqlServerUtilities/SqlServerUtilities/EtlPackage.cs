
using Microsoft.SqlServer.Dts.Pipeline.Wrapper;
using Microsoft.SqlServer.Dts.Runtime;
using Microsoft.SqlServer.Management.Smo;
using System;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.Data;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Diagnostics;
using Microsoft.SqlServer.Dts.Tasks.BulkInsertTask;
using Microsoft.SqlServer.Dts.Tasks.FileSystemTask;
using Microsoft.SqlServer.Dts.Tasks.ExecuteSQLTask;
using Microsoft.SqlServer.Dts.Tasks.SendMailTask;
using CommonUtils;

namespace SqlServerUtilities
{
    using connection_dictionary = Dictionary<string, string>;
    using Microsoft.SqlServer.Dts.Tasks.ExecuteSQLTask;
    using Sequence = Microsoft.SqlServer.Dts.Runtime.Sequence;
    using System.Collections;

    /// <summary>
    /// Generates SSIS ETL *.dtsx package file.  
    /// To allow for error handling, all implementating methods use SMO objects; 
    /// overloading string based methods create corresponding SMO objects and call SMO based methods.
    /// </summary>
    public class EtlPackage
    {
        private Package _package;
        private connection_dictionary _connectionDict;
        private Regex _connectionStringRegex = new Regex(@"Data Source=(?<server_name>\w+);Initial Catalog=(?<database_name>\w+);Provider=SQLOLEDB.1;Integrated Security=SSPI;");
        public Application App { get; set; }
        public List<Tuple<string, string>> DestinationTables;
        public StreamWriter LogFile;
        public string CleanSsisTaskName(string taskName)
        {
            return Regex.Replace(taskName, @"[\[\]\\\.]", "", RegexOptions.None, TimeSpan.FromSeconds(1.5));
        }
        //public EtlPackage(string src_server_name, string dst_server_name, string database_name)
        //{
        //    _package = new Package();
        //    _package.DelayValidation = true;
        //    _connection_dict = new connection_dictionary();
        //    package_file_name = string.Format("{0} {1}-{2}.dtsx", database_name, src_server_name, dst_server_name);
        //    addDataFlowTasksBySchema(database_name, src_server_name, dst_server_name);
        //}

        /// <summary>
        /// Constructor
        /// initialize EtlPackage object from *.dtsx file if it exists, if doesn't exist create new one.
        /// </summary>
        /// <param name="dtsxFilePath">path of dtsx package file</param>
        public EtlPackage(string dtsxFilePath)
        {
            App = new Application();
            if (File.Exists(dtsxFilePath))
            {
                Debug.WriteLine(string.Format("initializing EtlPackage from local file:\n\t{0}", dtsxFilePath));
                _package = App.LoadPackage(dtsxFilePath, null);
                _package.DelayValidation = true;
            }
            else
            {
                _package = new Package();
            }
            _connectionDict = new connection_dictionary();
        }
        /// <summary>
        /// Constructor
        /// initialize EtlPackage object from MSDB (ie. LoadFromDtsServer) deployed package
        /// </summary>
        /// <param name="ssisPath"></param>
        /// <param name="serverName"></param>
        /// <param name="events"></param>
        public EtlPackage(string ssisPath, string serverName, IDTSEvents events)
        {
            App = new Application();

            _package = App.LoadFromDtsServer(ssisPath, serverName, events);
            _package.DelayValidation = true;
            _connectionDict = new connection_dictionary();
            //Executable exec = _package.Executables["FLC"];
            //Executable exec2 = (exec as IDTSSequence).Executables.Add("STOCK:ScriptTask");
        }
        /// <summary>
        /// Default Constructor
        /// initialize EtlPackage empty package
        /// </summary>
        public EtlPackage()
        {
            App = new Application();
            _package = new Package();
            _package.DelayValidation = true;
            _connectionDict = new connection_dictionary();
            //Executable exec = _package.Executables["FLC"];
            //Executable exec2 = (exec as IDTSSequence).Executables.Add("STOCK:ScriptTask");
        }
        /// <summary>
        /// save EtlPackage to xml *.dtsx file
        /// </summary>
        /// <returns>path of saved *.dtsx file</returns>
        public string SavePackage()
        {
            _package.Validate(_package.Connections, null, null, null);
            App.SaveToXml(_package.Name, _package, null);
            Console.WriteLine("\r\n\r\npackage saved to:\r\n{0}\r\n{1}", Directory.GetCurrentDirectory(), _package.Name);
            return Path.Combine(_package.Name);
        }
        /// <summary>
        /// Add database connection to package.
        /// Follows ServerName-DatabaseName naming convection
        /// </summary>
        /// <param name="database"></param>
        /// <returns>connection name</returns>
        public string AddConnection(Database database)
        {
            string serverName = database.Parent.Name;
            string databaseName = database.Name;
            string connectionName = string.Format("{0}-{1}", CleanSsisTaskName(serverName), CleanSsisTaskName(databaseName));
            if (_connectionDict.ContainsKey(connectionName))
            {
                return connectionName;
            }
            else
            {
                ConnectionManager newConnectionManager = _package.Connections.Add("OLEDB");
                newConnectionManager.Name = connectionName;
                string connectionString = CommonUtils.CommonUtils.GetEtlConnectionString(serverName, databaseName);
                newConnectionManager.ConnectionString = connectionString;
                _connectionDict.Add(connectionName, connectionString);
                return connectionName;
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
        /// <summary>
        /// overload of addConnection(Database database)
        /// </summary>
        /// <param name="table"></param>
        /// <returns></returns>
        public string AddConnection(Table table)
        {
            Database database = table.Parent;
            Server server = database.Parent;
            return AddConnection(server.Name, database.Name);
        }
        /// <summary>
        /// overload of addConnection(Database database)
        /// </summary>
        /// <param name="table"></param>
        /// <returns></returns>
        public string AddConnection(string serverName, string databaseName)
        {
            Database database = SchemaReader.GetDatabase(serverName, databaseName);
            return AddConnection(database);
        }
        public string GetConnectionManagerConnectionString(string id)
        {
            foreach (ConnectionManager con in _package.Connections)
            {
                if (con.ID == id)
                {
                    return con.ConnectionString;
                }
            }
            return null;
        }
        public string GetTaskName()
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

        public TaskHost AddDataFlowTask(Executables execs, string srcServerName, string srcDatabaseName, string srcSchemaName, string srcObjectName, string dstServerName, string dstDatabaseName, string dstSchemaName, string dstObjectName)
        {
            // initialize required connections
            string srcCmName = AddConnection(srcServerName, srcDatabaseName);
            string dstCmName = AddConnection(dstServerName, dstDatabaseName);
            //string srcServerName = srcServerName;
            //string dstServerName = dstServerName;

            /* ADD DATA FLOW TASK */

            Executable e = execs.Add("STOCK:PipelineTask");
            TaskHost th = e as TaskHost;
            th.Name = CleanSsisTaskName(dstObjectName);
            MainPipe dataFlowTask = th.InnerObject as MainPipe;
            // The Application object will be used to obtain the CreationName of a PipelineComponentInfo from its PipelineComponentInfos collection.
            //app = new Application();
            th.DelayValidation = true;
            /* ADD SOURCE COMPONENT */

            // Add an OLE DB source to the data flow the CreationName property requires an Application source_component_wrapper.
            IDTSComponentMetaData100 sourceComponent = dataFlowTask.ComponentMetaDataCollection.New();
            sourceComponent.ComponentClassID = App.PipelineComponentInfos["OLE DB Source"].CreationName;
            // Get the design time source_component_wrapper of the source_component.
            CManagedComponentWrapper sourceComponentWrapper = sourceComponent.Instantiate();
            // Initialize the source_component
            sourceComponentWrapper.ProvideComponentProperties();
            // Set name of source_component
            string srcCompName = string.Format("src {0} {1} {2}", srcServerName, srcSchemaName, srcObjectName);
            //Console.WriteLine(src_comp_name);
            sourceComponent.Name = CleanSsisTaskName(srcCompName);
            // Specify the connection manager _package connections
            //Console.WriteLine(_package.Connections[_src_cm_name]);
            if (sourceComponent.RuntimeConnectionCollection.Count > 0)
            {
                sourceComponent.RuntimeConnectionCollection[0].ConnectionManager = DtsConvert.GetExtendedInterface(_package.Connections[srcCmName]);
                sourceComponent.RuntimeConnectionCollection[0].ConnectionManagerID = _package.Connections[srcCmName].ID;
            }
            //foreach (IDTSRuntimeConnection100 rt_con in source_component.RuntimeConnectionCollection)
            //{
            //    System.Console.WriteLine("connection name: {0}\r\nconnection string: {1}", rt_con.Name, rt_con.ConnectionManager.ConnectionString);
            //}
            //foreach (ConnectionManager con in _package.Connections)
            //{
            //    System.Console.WriteLine("connection manager: {0}", con.Name);
            //}
            // Set the custom properties: source view query and access mode  see here: https://msdn.microsoft.com/en-us/library/hh213133(v=sql.110).aspx

            /////////////////////////////////////////////////////////////////////////////////////////////////////////////////
            /////////////////////////////////////////////////////////////////////////////////////////////////////////////////
            // OpenRowset Using Fastload
            //source_component_wrapper.SetComponentProperty("AccessMode", 3);
            //source_component_wrapper.SetComponentProperty("OpenRowset", string.Format("[{0}].[{1}]",src_table.Schema, src_table.Name));
            /////////////////////////////////////////////////////////////////////////////////////////////////////////////////
            /////////////////////////////////////////////////////////////////////////////////////////////////////////////////
            sourceComponentWrapper.SetComponentProperty("AccessMode", 2);
            string srcQuery = string.Format("SELECT * FROM [{0}].[{1}];", srcSchemaName, srcObjectName);
            Console.WriteLine(srcQuery);
            sourceComponentWrapper.SetComponentProperty("SqlCommand", srcQuery);

            // Connect to the data source view
            sourceComponentWrapper.AcquireConnections(null);
            // Reinitialize the metadata.
            sourceComponentWrapper.ReinitializeMetaData();
            sourceComponentWrapper.ReleaseConnections();

            /* ADD DESTINATION COMPONENT */

            // Add an OLE DB source to the data flow the CreationName property requires an Application source_component_wrapper.
            IDTSComponentMetaData100 destinationComponent = dataFlowTask.ComponentMetaDataCollection.New();
            destinationComponent.ComponentClassID = App.PipelineComponentInfos["OLE DB Destination"].CreationName;
            // Create the design-time source_component_wrapper of the destination_component.
            CManagedComponentWrapper destinationComponentWrapper = destinationComponent.Instantiate();
            // The ProvideComponentProperties method creates a default input.
            destinationComponentWrapper.ProvideComponentProperties();
            // Set name of destination_component
            string dstCompName = string.Format("dst - {0} {1} {2}", dstServerName, dstSchemaName, dstObjectName);
            //Console.WriteLine(dst_comp_name);
            destinationComponent.Name = CleanSsisTaskName(dstCompName);
            // Specify the connection manager from _package connections
            if (destinationComponent.RuntimeConnectionCollection.Count > 0)
            {
                destinationComponent.RuntimeConnectionCollection[0].ConnectionManager = DtsConvert.GetExtendedInterface(_package.Connections[dstCmName]);
                destinationComponent.RuntimeConnectionCollection[0].ConnectionManagerID = _package.Connections[dstCmName].ID;
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
            destinationComponentWrapper.SetComponentProperty("AccessMode", 3);
            string dstTableStr = string.Format("[{0}].[{1}]", dstSchemaName, dstObjectName);
            //Console.WriteLine("{0} dst_table_str", dst_table_str);
            destinationComponentWrapper.SetComponentProperty("OpenRowset", dstTableStr);
            // Connect to the data destination view
            destinationComponentWrapper.AcquireConnections(null);
            // Reinitialize the metadata.
            destinationComponentWrapper.ReinitializeMetaData();
            destinationComponentWrapper.ReleaseConnections();

            /* ADD PATH FROM source_component TO destination_component */

            // Create the path object
            IDTSPath100 path = dataFlowTask.PathCollection.New();
            path.AttachPathAndPropagateNotifications(sourceComponent.OutputCollection[0], destinationComponent.InputCollection[0]);

            /* MAP COLUMNS FROM source_component TO destination_component */

            // Get the destination_component's default input and virtual input.
            IDTSInput100 input = destinationComponent.InputCollection[0];
            IDTSVirtualInput100 vInput = input.GetVirtualInput();

            // Iterate through the virtual input column collection.
            foreach (IDTSVirtualInputColumn100 vColumn in vInput.VirtualInputColumnCollection)
            {
                // Call the SetUsageType method of the destination_component to add each available virtual input column as an input column.
                IDTSInputColumn100 vCol = destinationComponentWrapper.SetUsageType(input.ID, vInput, vColumn.LineageID, DTSUsageType.UT_READONLY);

                // check if the column match exists in the destination_component view 
                string cinputColumnName = vColumn.Name;
                var columnExist = (from item in input.ExternalMetadataColumnCollection.Cast<IDTSExternalMetadataColumn100>()
                                   where item.Name == cinputColumnName && item.DataType == vColumn.DataType
                                   select item).Count();
                // check if the column is an identity column
                var isIdentity = (from Column item in SchemaReader.GetColumns(srcServerName, srcDatabaseName, srcSchemaName, srcObjectName)
                                  where item.Identity == true
                                  && item.Name == cinputColumnName
                                  select item).Count();
                // if match then map
                if (columnExist > 0)
                {
                    if (isIdentity == 1)
                    {
                        Console.WriteLine("{0} is an identity column", SchemaReader.GetColumns(srcServerName, srcDatabaseName, srcSchemaName, srcObjectName)[cinputColumnName]);
                    }
                    else
                    {
                        destinationComponentWrapper.MapInputColumn(input.ID, vCol.ID, input.ExternalMetadataColumnCollection[vColumn.Name].ID);
                        Console.WriteLine("\t{0} ({1}) => {2}", input.ExternalMetadataColumnCollection[vColumn.Name].Name, input.ExternalMetadataColumnCollection[vColumn.Name].DataType.ToString(), vColumn.Name);
                    }
                    // confirm component initialized and valid 
                    if (destinationComponentWrapper.Validate() == DTSValidationStatus.VS_NEEDSNEWMETADATA)
                    {
                        Console.WriteLine("removing invalid column mapping");
                        // https://msdn.microsoft.com/en-us/library/microsoft.sqlserver.dts.pipeline.pipelinecomponent.reinitializemetadata.aspx
                        //destination_component.RemoveInvalidInputColumns();
                        //destination_component_wrapper.ReinitializeMetaData();
                    }

                }
            }
            dataFlowTask.EnableDisconnectedColumns = false;

            // validate _package
            DTSExecResult validation = _package.Validate(_package.Connections, null, null, null);
            return th;
        }

        public TaskHost AddDataFlowTask(Executables execs, Table srcTable, Table dstTable)
        {
            return AddDataFlowTask(execs, srcTable.Parent.Parent.Name, srcTable.Parent.Name, srcTable.Schema,
                srcTable.Name, dstTable.Parent.Parent.Name, dstTable.Parent.Name, dstTable.Schema, dstTable.Name);

            //    // initialize required connections
            //    string _src_cm_name = addConnection(src_table);
            //    string _dst_cm_name = addConnection(dst_table);
            //    string _src_server_name = src_table.Parent.Parent.Name;
            //    string _dst_server_name = dst_table.Parent.Parent.Name;

            //    /*
            //     * 
            //     * NEW PART
            //     * 
            //     * 
            //     */
            //     string 






            //    //Console.WriteLine("\r\n{0}.{1}.{2}.{3} -->> {4}.{5}.{6}.{7}", src_table.Parent.Parent.ToString(), src_table.Parent.ToString(), src_table.Schema, src_table.Name, dst_table.Parent.Parent.ToString(), dst_table.Parent.ToString(), dst_table.Schema, dst_table.Name);

            //    /* ADD DATA FLOW TASK */

            //    Executable e = execs.Add("STOCK:PipelineTask");
            //    TaskHost th = e as TaskHost;
            //    th.Name = cleanSsisTaskName(src_table.Name);
            //    MainPipe dataFlowTask = th.InnerObject as MainPipe;
            //    // The Application object will be used to obtain the CreationName of a PipelineComponentInfo from its PipelineComponentInfos collection.
            //    //app = new Application();
            //    th.DelayValidation = true;
            //    /* ADD SOURCE COMPONENT */

            //    // Add an OLE DB source to the data flow the CreationName property requires an Application source_component_wrapper.
            //    IDTSComponentMetaData100 source_component = dataFlowTask.ComponentMetaDataCollection.New();
            //    source_component.ComponentClassID = app.PipelineComponentInfos["OLE DB Source"].CreationName;
            //    // Get the design time source_component_wrapper of the source_component.
            //    CManagedComponentWrapper source_component_wrapper = source_component.Instantiate();
            //    // Initialize the source_component
            //    source_component_wrapper.ProvideComponentProperties();
            //    // Set name of source_component
            //    string src_comp_name = string.Format("src {0} {1} {2}", _src_server_name, src_table.Schema, src_table.Name);
            //    //Console.WriteLine(src_comp_name);
            //    source_component.Name = cleanSsisTaskName(src_comp_name);
            //    // Specify the connection manager _package connections
            //    //Console.WriteLine(_package.Connections[_src_cm_name]);
            //    if (source_component.RuntimeConnectionCollection.Count > 0)
            //    {
            //        source_component.RuntimeConnectionCollection[0].ConnectionManager = DtsConvert.GetExtendedInterface(_package.Connections[_src_cm_name]);
            //        source_component.RuntimeConnectionCollection[0].ConnectionManagerID = _package.Connections[_src_cm_name].ID;
            //    }
            //    //foreach (IDTSRuntimeConnection100 rt_con in source_component.RuntimeConnectionCollection)
            //    //{
            //    //    System.Console.WriteLine("connection name: {0}\r\nconnection string: {1}", rt_con.Name, rt_con.ConnectionManager.ConnectionString);
            //    //}
            //    //foreach (ConnectionManager con in _package.Connections)
            //    //{
            //    //    System.Console.WriteLine("connection manager: {0}", con.Name);
            //    //}
            //    // Set the custom properties: source view query and access mode  see here: https://msdn.microsoft.com/en-us/library/hh213133(v=sql.110).aspx

            //    /////////////////////////////////////////////////////////////////////////////////////////////////////////////////
            //    /////////////////////////////////////////////////////////////////////////////////////////////////////////////////
            //    // OpenRowset Using Fastload
            //    //source_component_wrapper.SetComponentProperty("AccessMode", 3);
            //    //source_component_wrapper.SetComponentProperty("OpenRowset", string.Format("[{0}].[{1}]",src_table.Schema, src_table.Name));
            //    /////////////////////////////////////////////////////////////////////////////////////////////////////////////////
            //    /////////////////////////////////////////////////////////////////////////////////////////////////////////////////
            //    source_component_wrapper.SetComponentProperty("AccessMode", 2);
            //    string src_query = string.Format("SELECT * FROM [{0}].[{1}];", src_table.Schema, src_table.Name);
            //    Console.WriteLine(src_query);
            //    source_component_wrapper.SetComponentProperty("SqlCommand", src_query);

            //    // Connect to the data source view
            //    source_component_wrapper.AcquireConnections(null);
            //    // Reinitialize the metadata.
            //    source_component_wrapper.ReinitializeMetaData();
            //    source_component_wrapper.ReleaseConnections();

            //    /* ADD DESTINATION COMPONENT */

            //    // Add an OLE DB source to the data flow the CreationName property requires an Application source_component_wrapper.
            //    IDTSComponentMetaData100 destination_component = dataFlowTask.ComponentMetaDataCollection.New();
            //    destination_component.ComponentClassID = app.PipelineComponentInfos["OLE DB Destination"].CreationName;
            //    // Create the design-time source_component_wrapper of the destination_component.
            //    CManagedComponentWrapper destination_component_wrapper = destination_component.Instantiate();
            //    // The ProvideComponentProperties method creates a default input.
            //    destination_component_wrapper.ProvideComponentProperties();
            //    // Set name of destination_component
            //    string dst_comp_name = string.Format("dst - {0} {1} {2}", _dst_server_name, dst_table.Schema, dst_table.Name);
            //    //Console.WriteLine(dst_comp_name);
            //    destination_component.Name = cleanSsisTaskName(dst_comp_name);
            //    // Specify the connection manager from _package connections
            //    if (destination_component.RuntimeConnectionCollection.Count > 0)
            //    {
            //        destination_component.RuntimeConnectionCollection[0].ConnectionManager = DtsConvert.GetExtendedInterface(_package.Connections[_dst_cm_name]);
            //        destination_component.RuntimeConnectionCollection[0].ConnectionManagerID = _package.Connections[_dst_cm_name].ID;
            //    }
            //    //foreach (IDTSRuntimeConnection100 rt_con in source_component.RuntimeConnectionCollection)
            //    //{
            //    //    System.Console.WriteLine("connection name: {0}\r\nconnection string: {1}", rt_con.Name, rt_con.ConnectionManager.ConnectionString);
            //    //}
            //    //foreach (ConnectionManager con in _package.Connections)
            //    //{
            //    //    System.Console.WriteLine("connection manager: {0}", con.Name);
            //    //}
            //    // Set the custom properties: destination view and access mode
            //    destination_component_wrapper.SetComponentProperty("AccessMode", 3);
            //    string dst_table_str = string.Format("[{0}].[{1}]", dst_table.Schema, dst_table.Name);
            //    //Console.WriteLine("{0} dst_table_str", dst_table_str);
            //    destination_component_wrapper.SetComponentProperty("OpenRowset", dst_table_str);
            //    // Connect to the data destination view
            //    destination_component_wrapper.AcquireConnections(null);
            //    // Reinitialize the metadata.
            //    destination_component_wrapper.ReinitializeMetaData();
            //    destination_component_wrapper.ReleaseConnections();

            //    /* ADD PATH FROM source_component TO destination_component */

            //    // Create the path object
            //    IDTSPath100 path = dataFlowTask.PathCollection.New();
            //    path.AttachPathAndPropagateNotifications(source_component.OutputCollection[0], destination_component.InputCollection[0]);

            //    /* MAP COLUMNS FROM source_component TO destination_component */

            //    // Get the destination_component's default input and virtual input.
            //    IDTSInput100 input = destination_component.InputCollection[0];
            //    IDTSVirtualInput100 vInput = input.GetVirtualInput();

            //    // Iterate through the virtual input column collection.
            //    foreach (IDTSVirtualInputColumn100 vColumn in vInput.VirtualInputColumnCollection)
            //    {
            //        // Call the SetUsageType method of the destination_component to add each available virtual input column as an input column.
            //        IDTSInputColumn100 vCol = destination_component_wrapper.SetUsageType(input.ID, vInput, vColumn.LineageID, DTSUsageType.UT_READONLY);

            //        // check if the column match exists in the destination_component view 
            //        string cinputColumnName = vColumn.Name;
            //        var columnExist = (from item in input.ExternalMetadataColumnCollection.Cast<IDTSExternalMetadataColumn100>()
            //                           where item.Name == cinputColumnName && item.DataType == vColumn.DataType
            //                           select item).Count();
            //        // check if the column is an identity column
            //        var isIdentity = (from Column item in src_table.Columns
            //                          where item.Identity == true
            //                          && item.Name == cinputColumnName
            //                          select item).Count();
            //        // if match then map
            //        if (columnExist > 0)
            //        {
            //            if (isIdentity == 1)
            //            {
            //                Console.WriteLine("{0} is an identity column", src_table.Columns[cinputColumnName]);
            //            }
            //            else
            //            {
            //                destination_component_wrapper.MapInputColumn(input.ID, vCol.ID, input.ExternalMetadataColumnCollection[vColumn.Name].ID);
            //                Console.WriteLine("\t{0} ({1}) => {2}", input.ExternalMetadataColumnCollection[vColumn.Name].Name, input.ExternalMetadataColumnCollection[vColumn.Name].DataType.ToString(), vColumn.Name);
            //            }
            //            // confirm component initialized and valid 
            //            if (destination_component_wrapper.Validate() == DTSValidationStatus.VS_NEEDSNEWMETADATA)
            //            {
            //                Console.WriteLine("removing invalid column mapping");
            //                // https://msdn.microsoft.com/en-us/library/microsoft.sqlserver.dts.pipeline.pipelinecomponent.reinitializemetadata.aspx
            //                //destination_component.RemoveInvalidInputColumns();
            //                //destination_component_wrapper.ReinitializeMetaData();
            //            }

            //        }
            //    }
            //    dataFlowTask.EnableDisconnectedColumns = false;

            //    // validate _package
            //    DTSExecResult validation = _package.Validate(_package.Connections, null, null, null);
            //    return th;
        }
        public TaskHost AddDataFlowTask(Table srcTable, Table dstTable)
        {
            return AddDataFlowTask(_package.Executables, srcTable, dstTable);
        }
        public TaskHost AddDataFlowTask(string srcServerName, string srcDatabaseName, string srcSchemaName, string srcTableName, string dstServerName, string dstDatabaseName, string dstSchemaName, string dstTableName)
        {
            Table srcTable = SchemaReader.GetTable(srcServerName, srcDatabaseName, srcSchemaName, srcTableName);
            Table dstTable = SchemaReader.GetTable(dstServerName, dstDatabaseName, dstSchemaName, dstTableName);
            return AddDataFlowTask(srcTable, dstTable);
        }
        private void ParseDataFlow(TaskHost th, int tabCount)
        {
            string valueStr = "";
            "-------------------------------------------------------------".Print(0, ref LogFile);
            string.Format("Data Flow Task: {0}", th.Name).Print(tabCount, ref LogFile);
            MainPipe mp = th.InnerObject as MainPipe;
            foreach (IDTSComponentMetaData100 mdcol in mp.ComponentMetaDataCollection)
            {
                string.Format("Component: {0}", mdcol.ContactInfo.Split(';')[0]).Print(tabCount + 1, ref LogFile);
                string.Format("{0}: {1}", nameof(mdcol.Name), mdcol.Name).Print(tabCount + 2, ref LogFile);
                string.Format("{0}: {1}", nameof(mdcol.Description), mdcol.Description).Print(tabCount + 2, ref LogFile);

                IDTSCustomPropertyCollection100 props = mdcol.CustomPropertyCollection;
                if (props.Count > 0)
                {
                    "Component Custom Properties:".Print(tabCount + 2, ref LogFile);
                    // https://msdn.microsoft.com/en-us/library/hh213133.aspx
                    foreach (IDTSCustomProperty100 prop in props)
                    {
                        valueStr = prop.Value as string;
                        if (prop.Name == "SqlCommand" && valueStr.Length > 0)
                        {
                            "*********************************************".Print(0, ref LogFile);
                            string.Format("{0}:", prop.Name).Print(tabCount + 3, ref LogFile);
                            string.Format("{0}", valueStr).Print(0, ref LogFile);
                            "*********************************************".Print(0, ref LogFile);
                        }
                    }
                }
                try
                {
                    IDTSRuntimeConnectionCollection100 conns = mdcol.RuntimeConnectionCollection;
                    if (conns.Count > 0)
                    {
                        string.Format("Component Connections:").Print(tabCount + 2, ref LogFile);
                        foreach (IDTSRuntimeConnection100 conn in conns)
                        {
                            string conStr = GetConnectionManagerConnectionString(conn.ConnectionManagerID);
                            string db = CommonUtils.CommonUtils.ExtractDatabaseName(conStr);
                            string.Format("{0}: {1}", nameof(conn.Name), conn.Name).Print(tabCount + 3, ref LogFile);
                            string.Format("{0}: {1}", nameof(db), db).Print(tabCount + 4, ref LogFile);
                            string.Format("{0}: {1}", nameof(conStr), conStr).Print(tabCount + 4, ref LogFile);
                        }
                    }
                }
                catch (Exception expection)
                {
                    LogFile.WriteLine(expection);
                }
            }
        }
        public List<Executable> GetFirstExecutables()
        {
            List<PrecedenceConstraint> cnstsList = _package.PrecedenceConstraints.AsQueryable() as List<PrecedenceConstraint>;
            IEnumerable<Executable> execs = from y in cnstsList
                                            where !(from z in cnstsList
                                                    select z.ConstrainedExecutable).Contains(y.PrecedenceExecutable)
                                            select y.PrecedenceExecutable;
            return execs.ToList<Executable>();
        }

        public void ReadExecutables()
        {
            // check executables from object
            Executables execs = _package.Executables;
            LogFile = new StreamWriter(Path.Combine(CommonUtils.CommonUtils.Cwd(), _package.Name + ".txt"));
            string.Format("Connections:", _package.Connections.Count).Print(0, ref LogFile);
            foreach (ConnectionManager item in _package.Connections)
            {
                item.AcquireConnection(null);
                string.Format("{0}: ({1}) {2} {3}", item.Name, item.CreationName, item.ConnectionString, item.ID).Print(0, ref LogFile);
            }
            Console.WriteLine(_package.PrecedenceConstraints);
            List<PrecedenceConstraint> cnstsList = new List<PrecedenceConstraint>();
            PrecedenceConstraintEnumerator enm = _package.PrecedenceConstraints.GetEnumerator();
            while ((enm.MoveNext()) && (enm.Current != null))
            {
                cnstsList.Add(enm.Current);
            }
            var x = from y in cnstsList
                    where !(from z in cnstsList
                            select z.ConstrainedExecutable).Contains(y.PrecedenceExecutable)
                    select y.PrecedenceExecutable;
            foreach (var item in x)
            {
                Console.WriteLine(item);
                ReadExecutable(item);
            }
            Executable a = x.First();
        }
        public void ReadExecutable(Executable e)
        {
            ReadExecutable(e, 0);
        }
        public void ReadExecutable(Executable e, int tabCount)
        {
            if (e.GetType() == typeof(TaskHost))
            {
                
                TaskHost th = e as TaskHost;
                if (th.InnerObject is MainPipe)
                {
                    ParseDataFlow(th, tabCount + 1);
                }
                //Console.WriteLine(th.InnerObject is MainPipe);
                //MainPipe mp = th.InnerObject as MainPipe;
                //if (mp != null)
                //{
                //    logFile.WriteLine(string.Format(flowfmt, th.Name));
                //    foreach (IDTSComponentMetaData100 mdcol in mp.ComponentMetaDataCollection)
                //    {
                //        logFile.WriteLine(string.Format(flowdtfmt, nameof(mdcol.ContactInfo), mdcol.ContactInfo.Split(';')[0]));
                //        logFile.WriteLine(string.Format(flowdtfmt, nameof(mdcol.Name), mdcol.Name));
                //        logFile.WriteLine(string.Format(flowdtfmt, nameof(mdcol.Description), mdcol.Description));
                //        IDTSCustomPropertyCollection100 props = mdcol.CustomPropertyCollection;

                //        if (mdcol.ContactInfo.Contains("Source"))
                //        {
                //            //Console.WriteLine(string.Format(dtfmt,props))

                //        }
                //        // https://msdn.microsoft.com/en-us/library/hh213133.aspx
                //        foreach (IDTSCustomProperty100 prop in props)
                //        {
                //            logFile.WriteLine(string.Format(flowdtfmt, prop.Name, prop.Value));
                //        }
                //        try
                //        {
                //            IDTSRuntimeConnectionCollection100 conns = mdcol.RuntimeConnectionCollection;
                //            foreach (IDTSRuntimeConnection100 conn in conns)
                //            {
                //                logFile.WriteLine(string.Format(flowdtfmt, conn.Name, conn.ConnectionManager));
                //            }
                //        }
                //        catch (Exception expection)
                //        {
                //            logFile.WriteLine(expection);

                //        }

                //        //    if (mdcol.Description == "OLE DB Destination")
                //        //{
                //        //    //IDTSCustomPropertyCollection100 props = mdcol.CustomPropertyCollection;
                //        //    foreach (IDTSCustomProperty100 prop in props)
                //        //    {
                //        //        if (prop.Name == "OpenRowset")
                //        //        {
                //        //            Console.WriteLine(string.Format("\t\t\tValue = {0}", prop.Value));
                //        //        }
                //        //    }
                //        //    IDTSRuntimeConnectionCollection100 conns = mdcol.RuntimeConnectionCollection;
                //        //foreach (IDTSRuntimeConnection100 conn in conns)
                //        //    {
                //        //        if (conn.Name == "OleDbConnection")
                //        //        {
                //        //            //Console.WriteLine(string.Format("\t\t\tOleDbConnection"));
                //        //            Console.WriteLine(string.Format("\t\t\tName = {0}", conn.Name));
                //        //            //Console.WriteLine(string.Format("\t\t\tDescription = {0}", conn.Description));
                //        //            //Console.WriteLine(string.Format("\t\t\tConnectionManagerID = {0}", conn.ConnectionManagerID));
                //        //            foreach (ConnectionManager conmgr in _package.Connections)
                //        //            {
                //        //                if (conmgr.ID == conn.ConnectionManagerID)
                //        //                {
                //        //                    //Console.WriteLine(string.Format("\t\t\tConnectionManager conmgr"));
                //        //                    //Console.WriteLine(string.Format("\t\t\tName = {0}", conmgr.Name));
                //        //                    //Console.WriteLine(string.Format("\t\t\tToString = {0}", conmgr.ToString()));
                //        //                    //Console.WriteLine(string.Format("\t\t\tID = {0}", conmgr.ID));
                //        //                    //Console.WriteLine(string.Format("\t\t\tCreationName = {0}", conmgr.CreationName));
                //        //                    Console.WriteLine(string.Format("\t\t\tConnectionString = {0}", conmgr.ConnectionString));
                //        //                    Console.WriteLine(string.Format("\t\t\tDatabase = {0}", CommonUtils.CommonUtils.extractDatabaseName(conmgr.ConnectionString)));
                //        //                }

                //        //            }
                //        //        }
                //        //    }
                //        //Console.WriteLine(string.Format("mdcol = mp.ComponentMetaDataCollection"));
                //        //Console.WriteLine(string.Format("Name = {0}", mdcol.Name));
                //        //Console.WriteLine(string.Format("Description = {0}", mdcol.Description));
                //        //Console.WriteLine(string.Format("ComponentClassID = {0}", mdcol.ComponentClassID));
                //        //Console.WriteLine(string.Format("mdcol.InputCollection"));
                //        //Console.WriteLine(string.Format("Count = {0}", mdcol.InputCollection.Count));
                //        //Console.WriteLine(string.Format("ToString() = {0}", mdcol.InputCollection.ToString()));
                //        //Console.WriteLine(string.Format("mdcol.OutputCollection"));
                //        //Console.WriteLine(string.Format("Count = {0}", mdcol.OutputCollection.Count));
                //        //Console.WriteLine(string.Format("ToString() = {0}", mdcol.OutputCollection.ToString()));

                //        //IDTSCustomPropertyCollection100 props = mdcol.CustomPropertyCollection;
                //        //Console.WriteLine(string.Format("IDTSCustomPropertyCollection100 props = mdcol.CustomPropertyCollection;"));
                //        //foreach (IDTSCustomProperty100 prop in props)
                //        //{
                //        //    Console.WriteLine(string.Format("IDTSCustomProperty100"));
                //        //    Console.WriteLine(string.Format("Name = {0}",prop.Name));
                //        //    Console.WriteLine(string.Format("Value = {0}",prop.Value));
                //        //}

                //        //IDTSPathCollection100 pc = mp.PathCollection;
                //        //Console.WriteLine(string.Format("IDTSPathCollection100 pc = mp.PathCollection"));
                //        //Console.WriteLine(string.Format("GetType() = {0}", pc.GetType()));
                //        //Console.WriteLine(string.Format("ToString() = {0}", pc.ToString()));
                //        }
                //}
                else
                {
                    //Console.WriteLine(string.Format(fmt, th.InnerObject.GetType().Name, th.Name));
                    //logFile.WriteLine(string.Format(fmt, th.InnerObject.GetType().Name, th.Name));
                    string.Format("{0}: {1}", th.InnerObject.GetType().Name, th.Name).Print(tabCount, ref LogFile);

                    //logFile.WriteLine(string.Format(dtfmt, nameof(th.Description), th.Description));
                    //logFile.WriteLine(string.Format(dtfmt, nameof(th.GetExecutionPath), th.GetExecutionPath()));
                    string.Format("{0}: {1}", nameof(th.Description), th.Description).Print(tabCount, ref LogFile);
                    string.Format("{0}: {1}", nameof(th.GetExecutionPath), th.GetExecutionPath()).Print(tabCount, ref LogFile);
                }


            }
            //https://msdn.microsoft.com/en-us/library/microsoft.sqlserver.dts.runtime.executable.aspx
            else if (e.GetType() == typeof(Sequence))
            {
                //Console.WriteLine(string.Format("\tSequence"));
                Sequence seq = e as Sequence;
                //Console.WriteLine(string.Format("\t\tName = {0}", seq.Name));
                //Console.WriteLine(string.Format(fmt, e.GetType().Name, seq.Name));
                //logFile.WriteLine(string.Format(fmt, e.GetType().Name, seq.Name));
                string.Format("{0}: {1}", e.GetType().Name, seq.Name).Print(tabCount, ref LogFile);

                //Console.WriteLine(string.Format("\tGetExecutionPath() = {0}", seq.GetExecutionPath()));
                ReadExecutables(seq.Executables, tabCount + 2);
            }
            else if (e.GetType() == typeof(ForEachLoop))
            {
                //Console.WriteLine(string.Format("\tForEachLoop", e.GetType()));
                ForEachLoop loop = e as ForEachLoop;
                //Console.WriteLine(string.Format("\t\tName = {0}", loop.Name));
                //Console.WriteLine(string.Format("\t\tGetExecutionPath() = {0}", loop.GetExecutionPath()));
                //Console.WriteLine(string.Format(fmt, e.GetType().Name, loop.Name));
                //logFile.WriteLine(string.Format(fmt, e.GetType().Name, loop.Name));
                string.Format("{0}: {1}", e.GetType().Name, loop.Name).Print(tabCount, ref LogFile);

                ReadExecutables(loop.Executables, tabCount + 2);
            }
            else if (e.GetType() == typeof(ForLoop))
            {
                //Console.WriteLine(string.Format("\tForEachLoop", e.GetType()));
                ForLoop loop = e as ForLoop;
                //Console.WriteLine(string.Format("\t\tName = {0}", loop.Name));
                //Console.WriteLine(string.Format("\t\tGetExecutionPath() = {0}", loop.GetExecutionPath()));
                //Console.WriteLine(string.Format(fmt, e.GetType().Name, loop.Name));
                //logFile.WriteLine(string.Format(fmt, e.GetType().Name, loop.Name));
                string.Format("{0}: {1}", e.GetType().Name, loop.Name).Print(tabCount, ref LogFile);
                ReadExecutables(loop.Executables, tabCount + 2);
            }
            else if (e.GetType() == typeof(Package))
            {
                //Console.WriteLine(string.Format("\tForEachLoop", e.GetType()));
                Package loop = e as Package;
                //Console.WriteLine(string.Format("\t\tName = {0}", loop.Name));
                //Console.WriteLine(string.Format("\t\tGetExecutionPath() = {0}", loop.GetExecutionPath()));
                //Console.WriteLine(string.Format(fmt, e.GetType().Name, loop.Name));
                //logFile.WriteLine(string.Format(fmt, e.GetType().Name, loop.Name));
                string.Format("{0}: {1}", e.GetType().Name, loop.Name).Print(tabCount, ref LogFile);
                ReadExecutables(loop.Executables, tabCount + 2);
            }
            else
            {
                //Console.WriteLine(string.Format(fmt, e.GetType(), "UNHANDLED executable type"));
                //logFile.WriteLine(string.Format(fmt, e.GetType(), "UNHANDLED executable type"));
                string.Format("{0}: {1}", e.GetType(), "UNHANDLED executable type").Print(tabCount, ref LogFile);
            }
        }
        public void ReadExecutables(Executables execs)
        {
            ReadExecutables(execs, 0);
        }
        public void ReadExecutables(Executables execs, int tabCount)
        {
            //string tab = new string('\t', tabCount);
            //string fmt = tab + "{0}: {1}";
            //string dtfmt = tab + "-{0}: {1}";
            //string flowfmt = tab + "DataFlow: {0}";
            //tab = new string('\t', tabCount + 1);
            //string flowdtfmt = tab + "-{0} {1}";
            foreach (Executable e in execs)
            {
                ReadExecutable(e, tabCount);
            }
        }
        public void ReadDestinationTables()
        {
            DestinationTables = new List<Tuple<string, string>>();
            ReadDestinationTables(_package.Executables);
        }
        public void ReadDestinationTables(Executables execs)
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
                                                Console.WriteLine(string.Format("\t\t\tDatabase = {0}", CommonUtils.CommonUtils.ExtractDatabaseName(conmgr.ConnectionString)));
                                                databaseName = CommonUtils.CommonUtils.ExtractDatabaseName(conmgr.ConnectionString);
                                                Tuple<string, string> databaseTable = new Tuple<string, string>(databaseName, tableName);

                                                //tableName = databaseName + "." + tableName;
                                                //DestinationTables.Add(tableName);
                                                DestinationTables.Add(databaseTable);
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
                    ReadDestinationTables(seq.Executables);
                }
                else if (exec.GetType() == typeof(ForEachLoop))
                {
                    //Console.WriteLine(string.Format("\tForEachLoop", exec.GetType()));
                    ForEachLoop loop = exec as ForEachLoop;
                    //Console.WriteLine(string.Format("\t\tName = {0}", loop.Name));
                    //Console.WriteLine(string.Format("\t\tGetExecutionPath() = {0}", loop.GetExecutionPath()));
                    ReadDestinationTables(loop.Executables);
                }
            }
        }
        public TaskHost AddSqlTask(Executables execs, Database database, string taskName, string sqlSource)
        {
            Executable e = execs.Add("STOCK:SQLTask");
            TaskHost th = e as TaskHost;

            ExecuteSQLTask sqlTask = th.InnerObject as ExecuteSQLTask;
            sqlTask.SqlStatementSourceType = SqlStatementSourceType.DirectInput;
            sqlTask.SqlStatementSource = sqlSource;
            sqlTask.Connection = AddConnection(database.Parent.Name, database.Name);
            th.Name = taskName;
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
        public TaskHost AddSqlTask(Executables execs, string serverName, string databaseName, string taskName, string sqlSource)
        {
            Database database = SchemaReader.GetDatabase(serverName, databaseName);
            return AddSqlTask(execs, database, taskName, sqlSource);
        }
        public TaskHost AddSqlTask(string serverName, string databaseName, string taskName, string sqlSource)
        {
            return AddSqlTask(_package.Executables, serverName, databaseName, taskName, sqlSource);
        }

        public Sequence AddSequence(Executables execs, string sequenceName)
        {
            //(Microsoft.SqlServer.Dts.Runtime.Sequence)
            Executable e = execs.Add("STOCK:SEQUENCE");
            Sequence seq = e as Sequence;
            seq.Name = CleanSsisTaskName(sequenceName);
            return seq;
        }
        public Sequence AddSequence(string sequenceName)
        {
            return AddSequence(_package.Executables, sequenceName);
        }

        /* Convience methods
         * 
         * 
         */
        public string AddDataFlowTasksBySchema(string databaseName, string schemaName, string srcServerName, string dstServerName)
        {
            Sequence seq = AddSequence(schemaName);

            Database srcDatabase = SchemaReader.GetDatabase(srcServerName, databaseName);
            Database dstDatabase = SchemaReader.GetDatabase(dstServerName, databaseName);
            foreach (Table srcTable in srcDatabase.Tables)
            {
                if (srcTable.Schema == schemaName && dstDatabase.Tables[srcTable.Name, srcTable.Schema] != null)
                {
                    AddTruncatePopulate(seq.Executables, srcTable, dstDatabase.Tables[srcTable.Name, srcTable.Schema]);
                }
            }
            return seq.Name;

        }
        public void AddDataFlowTasksBySchema(string databaseName, string srcServerName, string dstServerName)
        {
            Database database = SchemaReader.GetDatabase(srcServerName, databaseName);
            foreach (Schema schema in database.Schemas)
            {
                AddDataFlowTasksBySchema(databaseName, schema.Name, srcServerName, dstServerName);
            }
        }
        public Sequence AddTruncatePopulate(Executables execs, Table srcTable, Table dstTable)
        {
            Sequence seq = AddSequence(execs, srcTable.Name);

            TaskHost thSql = AddSqlTask(seq.Executables, dstTable.Parent, string.Format("TRUNCATE TABLE {0}-{1}", dstTable.Schema, dstTable.Name), string.Format("TRUNCATE TABLE {0}.{1}", dstTable.Schema, dstTable.Name));
            ExecuteSQLTask sql = thSql.InnerObject as ExecuteSQLTask;
            Executable eSql = thSql as Executable;

            TaskHost thDf = AddDataFlowTask(seq.Executables, srcTable, dstTable);
            MainPipe mp = thDf.InnerObject as MainPipe;
            Executable eDf = thDf as Executable;

            PrecedenceConstraint preConst = seq.PrecedenceConstraints.Add(eSql, eDf);
            preConst.Name = dstTable.Name;

            return seq;
        }
        public Sequence AddTruncatePopulate(Executables execs, string srcServerName, string srcDatabaseName, string srcSchemaName, string srcTableName, string dstServerName, string dstDatabaseName, string dstSchemaName, string dstTableName)
        {
            Table dstTable = SchemaReader.GetTable(dstServerName, dstDatabaseName, dstSchemaName, dstTableName);
            Table srcTable = SchemaReader.GetTable(srcServerName, srcDatabaseName, srcSchemaName, srcTableName);
            return AddTruncatePopulate(execs, srcTable, dstTable);
        }
        public void ExecutePackage()
        {
            DTSExecResult res = _package.Execute();
            Console.WriteLine(res.ToString());
        }
    }
}
