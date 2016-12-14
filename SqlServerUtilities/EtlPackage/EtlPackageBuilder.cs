using System;
using System.Diagnostics;
using System.IO;
using System.Linq;
using Microsoft.SqlServer.Dts.Pipeline.Wrapper;
using Microsoft.SqlServer.Dts.Runtime;
using Microsoft.SqlServer.Management.Sdk.Sfc;
using Microsoft.SqlServer.Management.Smo;

namespace EtlPackage
{
    public class EtlPackageBuilder
    {
        private Application _application;
        private Package _package;

        private string PackageName
        {
            set { _package.Name = value; }
        }

        public EtlPackageBuilder(string packageName)
        {
            // The Application object will be used to obtain the CreationName of a PipelineComponentInfo from its PipelineComponentInfos collection.
            _application = new Application();
            _package = new Package();
            PackageName = packageName;
        }

        /// <summary>
        /// creates Ole Database ConnectionManager to Database in package if one does not already exist
        /// </summary>
        /// <param name="serverName"></param>
        /// <param name="databaseName"></param>
        /// <returns></returns>
        public string AddOleDbConnectionManager(string serverName, string databaseName)
        {
            Debug.Print("AddOleDbConnectionManager: {0}", databaseName);
            foreach (var packageConnection in _package.Connections)
            {
                if (SqlUtilities.ExtractDatabaseName(packageConnection.ConnectionString) == databaseName)
                {
                    Debug.Print("AddOleDbConnectionManager: {0} exists in ConnectionManager: {1}", databaseName,
                        packageConnection.Name);
                    return packageConnection.Name;
                }
            }
            Debug.Print("AddOleDbConnectionManager: create new ConnectionManager to: {0} {1}", serverName, databaseName);
            return ConnectionManagerBuilder.Build.CreateOleDbConnection(_package, serverName, databaseName);
        }

        /// <summary>
        /// creates Ole Database ConnectionManager to Database in package if one does not already exist
        /// </summary>
        /// <param name="database"></param>
        /// <returns>return of ConnectionManager</returns>
        public string AddOleDbConnectionManager(Database database)
        {
            string databaseName = database.Name;
            string serverName = database.Parent.Name;
            return AddOleDbConnectionManager(serverName, databaseName);
        }

        public void AddOleDataFlow(string srcServerName, string srcDatabaseName, string srcSchemaName,
            string srcObjectName, string dstServerName, string dstDatabaseName, string dstSchemaName,
            string dstObjectName)
        {
            string srcCmName = AddOleDbConnectionManager(srcServerName, srcDatabaseName);
            string dstCmName = AddOleDbConnectionManager(dstServerName, dstDatabaseName);

            Executable executable = _package.Executables.Add("STOCK:PipelineTask");
            TaskHost taskHost = executable as TaskHost;
            Debug.Assert(taskHost != null, "taskHost != null");
            taskHost.Name = SqlUtilities.CleanSsisTaskName(dstObjectName);
            Debug.Assert(taskHost != null, "th != null");
            MainPipe dataFlowTask = taskHost.InnerObject as MainPipe;
            taskHost.DelayValidation = true;

            /* 
             * ADD SOURCE COMPONENT 
             */
            // Add an OLE DB source to the data flow the CreationName property requires an Application source_component_wrapper.
            Debug.Assert(dataFlowTask != null, "dataFlowTask != null");
            IDTSComponentMetaData100 sourceComponent = dataFlowTask.ComponentMetaDataCollection.New();
            sourceComponent.ComponentClassID = _application.PipelineComponentInfos["OLE DB Source"].CreationName;
            // Get the design time source_component_wrapper of the source_component.
            CManagedComponentWrapper sourceComponentWrapper = sourceComponent.Instantiate();
            // Initialize the source_component
            sourceComponentWrapper.ProvideComponentProperties();
            // Set name of source_component
            string srcCompName = $"src {srcServerName} {srcSchemaName} {srcObjectName}";
            //Console.WriteLine(src_comp_name);
            sourceComponent.Name = SqlUtilities.CleanSsisTaskName(srcCompName);
            // Specify the connection manager _package connections
            //Console.WriteLine(_package.Connections[_src_cm_name]);
            if (sourceComponent.RuntimeConnectionCollection.Count > 0)
            {
                sourceComponent.RuntimeConnectionCollection[0].ConnectionManager =
                    DtsConvert.GetExtendedInterface(_package.Connections[srcCmName]);
                sourceComponent.RuntimeConnectionCollection[0].ConnectionManagerID = _package.Connections[srcCmName].ID;
            }
            sourceComponentWrapper.SetComponentProperty("AccessMode", 2);
            string srcQuery = $"SELECT * FROM [{srcSchemaName}].[{srcObjectName}];";
            Console.WriteLine(srcQuery);
            sourceComponentWrapper.SetComponentProperty("SqlCommand", srcQuery);

            // Connect to the data source view
            sourceComponentWrapper.AcquireConnections(null);
            // Reinitialize the metadata.
            sourceComponentWrapper.ReinitializeMetaData();
            sourceComponentWrapper.ReleaseConnections();


            /* 
             * ADD DESTINATION COMPONENT 
             */
            // Add an OLE DB source to the data flow the CreationName property requires an Application source_component_wrapper.
            IDTSComponentMetaData100 destinationComponent = dataFlowTask.ComponentMetaDataCollection.New();
            destinationComponent.ComponentClassID =
                _application.PipelineComponentInfos["OLE DB Destination"].CreationName;
            // Create the design-time source_component_wrapper of the destination_component.
            CManagedComponentWrapper destinationComponentWrapper = destinationComponent.Instantiate();
            // The ProvideComponentProperties method creates a default input.
            destinationComponentWrapper.ProvideComponentProperties();
            // Set name of destination_component
            string dstCompName = $"dst - {dstServerName} {dstSchemaName} {dstObjectName}";
            //Console.WriteLine(dst_comp_name);
            destinationComponent.Name = SqlUtilities.CleanSsisTaskName(dstCompName);
            // Specify the connection manager from _package connections
            if (destinationComponent.RuntimeConnectionCollection.Count > 0)
            {
                destinationComponent.RuntimeConnectionCollection[0].ConnectionManager =
                    DtsConvert.GetExtendedInterface(_package.Connections[dstCmName]);
                destinationComponent.RuntimeConnectionCollection[0].ConnectionManagerID =
                    _package.Connections[dstCmName].ID;
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
            string dstTableStr = $"[{dstSchemaName}].[{dstObjectName}]";
            //Console.WriteLine("{0} dst_table_str", dst_table_str);
            destinationComponentWrapper.SetComponentProperty("OpenRowset", dstTableStr);
            // Connect to the data destination view
            destinationComponentWrapper.AcquireConnections(null);
            // Reinitialize the metadata.
            destinationComponentWrapper.ReinitializeMetaData();
            destinationComponentWrapper.ReleaseConnections();


            /* 
             * MAP COLUMNS FROM source_component TO destination_component 
             */

            // Get the destination_component's default input and virtual input.
            IDTSInput100 input = destinationComponent.InputCollection[0];
            IDTSVirtualInput100 vInput = input.GetVirtualInput();

            // Iterate through the virtual input column collection.
            foreach (IDTSVirtualInputColumn100 vColumn in vInput.VirtualInputColumnCollection)
            {
                // Call the SetUsageType method of the destination_component to add each available virtual input column as an input column.
                IDTSInputColumn100 vCol = destinationComponentWrapper.SetUsageType(input.ID, vInput, vColumn.LineageID,
                    DTSUsageType.UT_READONLY);

                // check if the column match exists in the destination_component view 
                string cinputColumnName = vColumn.Name;
                var columnExist =
                (from item in input.ExternalMetadataColumnCollection.Cast<IDTSExternalMetadataColumn100>()
                    where item.Name == cinputColumnName && item.DataType == vColumn.DataType
                    select item).Count();
                // check if the column is an identity column
                var isIdentity =
                (from Column item in
                    SqlUtilities.GetColumns(srcServerName, srcDatabaseName, srcSchemaName, srcObjectName)
                    where item.Identity == true
                          && item.Name == cinputColumnName
                    select item).Count();
                // if match then map
                if (columnExist > 0)
                {
                    if (isIdentity == 1)
                    {
                        Console.WriteLine("{0} is an identity column",
                            SqlUtilities.GetColumns(srcServerName, srcDatabaseName, srcSchemaName, srcObjectName)[
                                cinputColumnName]);
                    }
                    else
                    {
                        destinationComponentWrapper.MapInputColumn(input.ID, vCol.ID,
                            input.ExternalMetadataColumnCollection[vColumn.Name].ID);
                        Console.WriteLine("\t{0} ({1}) => {2}",
                            input.ExternalMetadataColumnCollection[vColumn.Name].Name,
                            input.ExternalMetadataColumnCollection[vColumn.Name].DataType.ToString(), vColumn.Name);
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
        }

        public void AddOleDataFlow(Table srcTable, Table dstTable)
        {
            string srcServerName = srcTable.Parent.Parent.Name;
            string srcDatabaseName = srcTable.Parent.Name;
            string srcSchemaName = srcTable.Schema;
            string srcObjectName = srcTable.Name;
            string srcCmName = AddOleDbConnectionManager(srcTable.Parent);
            string dstServerName = dstTable.Parent.Parent.Name;
            string dstDatabaseName = dstTable.Parent.Name;
            string dstSchemaName = dstTable.Schema;
            string dstObjectName = dstTable.Name;
            string dstCmName = AddOleDbConnectionManager(dstTable.Parent);

            AddOleDataFlow(srcServerName, srcDatabaseName, srcSchemaName: srcSchemaName, srcObjectName: srcObjectName,
                dstServerName: dstServerName, dstDatabaseName: dstDatabaseName, dstSchemaName: dstSchemaName,
                dstObjectName: dstObjectName);
        }

        public void AddOleDataFlow(View srcView, Table dstTable)
        {
            string srcServerName = srcView.Parent.Parent.Name;
            string srcDatabaseName = srcView.Parent.Name;
            string srcSchemaName = srcView.Schema;
            string srcObjectName = srcView.Name;

            string dstServerName = dstTable.Parent.Parent.Name;
            string dstDatabaseName = dstTable.Parent.Name;
            string dstSchemaName = dstTable.Schema;
            string dstObjectName = dstTable.Name;

            AddOleDataFlow(srcServerName, srcDatabaseName, srcSchemaName: srcSchemaName, srcObjectName: srcObjectName,
                dstServerName: dstServerName, dstDatabaseName: dstDatabaseName, dstSchemaName: dstSchemaName,
                dstObjectName: dstObjectName);
        }

        public void AddOleDataFlow(Urn srcUrn, Urn dstUrn)
        {
            string srcServerQualifiedName = SqlUtilities.ExtractServerQualifiedObjectName(srcUrn);
            string srcServerName = srcServerQualifiedName.Split('.')[0];
            string srcDatabaseName = srcServerQualifiedName.Split('.')[1];
            string srcSchemaName = srcServerQualifiedName.Split('.')[2];
            string srcObjectName = srcServerQualifiedName.Split('.')[3];

            string dstServerQualifiedName = SqlUtilities.ExtractServerQualifiedObjectName(dstUrn);
            string dstServerName = dstServerQualifiedName.Split('.')[0];
            string dstDatabaseName = dstServerQualifiedName.Split('.')[1];
            string dstSchemaName = dstServerQualifiedName.Split('.')[2];
            string dstObjectName = dstServerQualifiedName.Split('.')[3];

            AddOleDataFlow(srcServerName, srcDatabaseName, srcSchemaName: srcSchemaName, srcObjectName: srcObjectName,
                dstServerName: dstServerName, dstDatabaseName: dstDatabaseName, dstSchemaName: dstSchemaName,
                dstObjectName: dstObjectName);
        }

        public void SavePackage(string DestinationPath)
        {
            if (!Directory.Exists(DestinationPath))
            {
                
            }
        }
}
    public sealed class ConnectionManagerBuilder
    {
        private static volatile ConnectionManagerBuilder _instance;
        private static readonly object SyncRoot = new object();
        private ConnectionManagerBuilder() { }
        public static ConnectionManagerBuilder Build
        {
            get
            {
                if (_instance == null)
                {
                    lock (SyncRoot)
                    {
                        if (_instance == null)
                            _instance = new ConnectionManagerBuilder();
                    }
                }
                return _instance;
            }
        }
        public string CreateOleDbConnection(Package package, string serverName, string databaseName)
        {
            ConnectionManager conMgr = package.Connections.Add("OLEDB");
            conMgr.ConnectionString = "Provider=SQLOLEDB.1;" +
                                      $"Integrated Security=SSPI;Initial Catalog={databaseName};" +
                                      $"Data Source={serverName};";
            conMgr.Name = $"OLE DB {serverName} {databaseName}";
            conMgr.Description = $"OLE DB {serverName} {databaseName}";
            return conMgr.Name;
        }
        public string CreateFlatFileConnection(Package package, string filePath)
        {
            ConnectionManager conMgr = package.Connections.Add("File");
            conMgr.ConnectionString = $@"{filePath}";
            string filename = Path.GetFileNameWithoutExtension(filePath);
            conMgr.Name = $"{filename}";
            conMgr.Description = $"{filename}";
            return conMgr.Name;
        }
    }
}
