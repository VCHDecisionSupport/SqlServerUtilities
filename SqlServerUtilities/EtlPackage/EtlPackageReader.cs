using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.SqlServer.Dts.Pipeline.Wrapper;
using Microsoft.SqlServer.Dts.Runtime;
using Microsoft.SqlServer.Dts.Tasks.ExecuteSQLTask;
using Microsoft.SqlServer.Management.Smo;
using Sequence = Microsoft.SqlServer.Dts.Runtime.Sequence;

namespace EtlPackage
{
    // https://msdn.microsoft.com/en-us/library/w369ty8x.aspx
    public class DataFlowEventArgs : EventArgs
    {
        public string DataFlowName { get; set; }
        public string SourceDatabaseName { get; set; }
        public string SourceQuery { get; set; }
        public string DestinationDatabaseName { get; set; }
        public string DestinationTableName { get; set; }
        public int NestedLevel { get; set; }
        public DataFlowEventArgs(string DataFlowName, string SourceDatabaseName, string SourceQuery, string DestinationDatabaseName, string DestinationTableName, int NestedLevel)
        {
            this.DataFlowName = DataFlowName;
            this.SourceDatabaseName = SourceDatabaseName;
            this.SourceQuery = SourceQuery;
            this.DestinationDatabaseName = DestinationDatabaseName;
            this.DestinationTableName = DestinationTableName;
            this.NestedLevel = NestedLevel;
        }
    }
    public class PackageEventArgs : EventArgs
    {
        public string PackageName { get; set; }
        public PackageEventArgs(string PackageName)
        {
            this.PackageName = PackageName;
        }
    }
    public class ExecuteSqlEventArgs: EventArgs
    {
        public string ExecuteSqlName { get; set; }
        public ExecuteSqlEventArgs(string ExecuteSqlName)
        {
            this.ExecuteSqlName = ExecuteSqlName;
        }
    }
        // publisher of DataFlowEventArgs events
        public class EtlPackageReader
    {
        private Application _application;
        private Package _package;
        //private MarkDownWriter _md;
        public event EventHandler<PackageEventArgs> RaisePackageEvent;
        public event EventHandler<DataFlowEventArgs> RaiseDataFlowEvent;
        public event EventHandler<ExecuteSqlEventArgs> RaiseExecuteSqlEvent;
        private int _nestedLevel = 1;
        protected virtual void OnRaiseDataFlowEvent(DataFlowEventArgs dataFlowEventArgs)
        {
            // Make a temporary copy of the event to avoid possibility of
            // a race condition if the last subscriber unsubscribes
            // immediately after the null check and before the event is raised.
            EventHandler<DataFlowEventArgs> handler = RaiseDataFlowEvent;

            // Event will be null if there are no subscribers
            if (handler != null)
            {
                // Format the string to send inside the CustomEventArgs parameter
                //e.Message += String.Format(" at {0}", DateTime.Now.ToString());

                // Use the () operator to raise the event.
                handler(this, dataFlowEventArgs);
            }


        }
        protected virtual void OnRaisePackageEvent(PackageEventArgs packageEventArgs)
        {
            EventHandler<PackageEventArgs> handler = RaisePackageEvent;
            // Event will be null if there are no subscribers
            if (handler != null)
            {
                // Format the string to send inside the CustomEventArgs parameter
                //e.Message += String.Format(" at {0}", DateTime.Now.ToString());

                // Use the () operator to raise the event.
                handler(this, packageEventArgs);
            }
        }
        protected virtual void OnRaiseExecuteSqlEvent(ExecuteSqlEventArgs executeSqlEventArgs)
        {
            EventHandler<ExecuteSqlEventArgs> handler = RaiseExecuteSqlEvent;
            // Event will be null if there are no subscribers
            if (handler != null)
            {
                // Format the string to send inside the CustomEventArgs parameter
                //e.Message += String.Format(" at {0}", DateTime.Now.ToString());

                // Use the () operator to raise the event.
                handler(this, executeSqlEventArgs);
            }
        }
        public EtlPackageReader(string etlPackagePath)
        {
            Console.WriteLine($"EtlPackageReader({etlPackagePath})...");
            _application = new Application();
            IDTSEvents idtsEvents = new DefaultEvents();
            if (File.Exists(etlPackagePath))
            {
                Debug.WriteLine($"package file found on filesystem\nloading...");
                _package = _application.LoadPackage(etlPackagePath, idtsEvents);
                //_md = new MarkDownWriter($"{Path.GetDirectoryName(etlPackagePath)}/{Path.GetFileNameWithoutExtension(etlPackagePath)}.md");
                //OnRaisePackageEvent(new PackageEventArgs(Path.GetFileNameWithoutExtension(etlPackagePath)));
            }
            else
            {
                Debug.Write($"ERROR package file NOT found on filesystem\nloading...");
                throw new Exception($"EtlPackageReader({etlPackagePath}) path not found");
            }
            Console.WriteLine($"Package {_package.Name} loaded.");
        }
        public EtlPackageReader(string SsisServerName, string ssisEtlPath)
        {
            Console.WriteLine($"EtlPackageReader({SsisServerName}, {ssisEtlPath})...");
            _application = new Application();
            IDTSEvents idtsEvents = new DefaultEvents();
            if (_application.ExistsOnDtsServer(ssisEtlPath, SsisServerName))
            {
                Debug.WriteLine($"package file found on Ssis Server (Dts)\nloading...");
                _package = _application.LoadFromDtsServer(ssisEtlPath, SsisServerName, idtsEvents);
                //_md = new MarkDownWriter(ssisEtlPath.Split('\\').Last().Split('/').Last().Replace("\\", "").Replace("/", "") + ".md");
                //OnRaisePackageEvent(new PackageEventArgs(ssisEtlPath.Split('\\').Last().Split('/').Last().Replace("\\", "").Replace("/", "")));

            }
            else
            {
                Debug.WriteLine($"ERROR package ({ssisEtlPath}) NOT found on Dts Server: {SsisServerName}\nloading...");
                throw new Exception($"EtlPackageReader({ssisEtlPath} on {SsisServerName}) not found");
            }
            Console.WriteLine($"Package {_package.Name} loaded.");
        }
        public void ReadConnectionManagers()
        {
            _nestedLevel += 1;
            Debug.IndentLevel = _nestedLevel;
            foreach (ConnectionManager connectionManager in _package.Connections)
            {
                Debug.Print($"Connection Manager Name: {connectionManager.Name}");
                _nestedLevel += 1;
                Debug.IndentLevel = _nestedLevel;
                Debug.Print($"Connection Manager Server: {SqlUtilities.ExtractServerName(connectionManager.ConnectionString)}");
                Debug.Print($"Connection Manager Database: {SqlUtilities.ExtractDatabaseName(connectionManager.ConnectionString)}");
                _nestedLevel -= 1;
                Debug.IndentLevel = _nestedLevel;
            }
            _nestedLevel -= 1;
            Debug.IndentLevel = _nestedLevel;
        }
        public void ReadExecutables()
        {
            OnRaisePackageEvent(new PackageEventArgs(_package.Name));
            Executables executables = _package.Executables;
            ReadExecutables(executables);
            //_md.Close();
        }
        public void ReadExecutables(Executables executables)
        {
            _nestedLevel += 1;
            Debug.IndentLevel = _nestedLevel;
            //_md.NestedLevel += 1;
            foreach (Executable executable in executables)
            {
                Debug.Print($"executable type: {executable.ToString()}");
                Debug.Print($"executable type: {executable.ToString()}");
                if (executable is TaskHost)
                {
                    Debug.Print("TaskHost");
                    TaskHost taskHost = executable as TaskHost;
                    if (taskHost.InnerObject is MainPipe)
                    {
                        Debug.Print("MainPipe");
                        ParseDataFlow(taskHost);
                    }
                    else if (taskHost.InnerObject is ExecuteSQLTask)
                    {
                        Debug.Print("ExecuteSqlTask");
                        ParseExecuteSql(taskHost);
                    }
                    else
                    {
                        Debug.Print($"\n\n\n\t{taskHost.InnerObject.GetType()} taskHost type is NOT IMPLEMENTED\n\n\n");
                    }
                }
                else if (executable.GetType() == typeof(Sequence))
                {
                    //Console.WriteLine(string.Format("\tSequence"));
                    Sequence sequence = executable as Sequence;
                    //Console.WriteLine(string.Format("\t\tName = {0}", seq.Name));
                    //Console.WriteLine(string.Format(fmt, e.GetType().Name, seq.Name));
                    //logFile.WriteLine(string.Format(fmt, e.GetType().Name, seq.Name));
                    Debug.Print($"executable name: {sequence.Name}");
                    //_md.WriteTitle($"Sequence: {sequence.Name}");
                    ReadExecutables(sequence.Executables);
                }
                else if (executable.GetType() == typeof(ForEachLoop))
                {
                    //Console.WriteLine(string.Format("\tForEachLoop", e.GetType()));
                    ForEachLoop loop = executable as ForEachLoop;
                    //Console.WriteLine(string.Format("\t\tName = {0}", loop.Name));
                    //Console.WriteLine(string.Format("\t\tGetExecutionPath() = {0}", loop.GetExecutionPath()));
                    //Console.WriteLine(string.Format(fmt, e.GetType().Name, loop.Name));
                    //logFile.WriteLine(string.Format(fmt, e.GetType().Name, loop.Name));
                    Debug.Print($"executable name: {loop.Name}");
                    //_md.WriteTitle($"ForEachLoop: {loop.Name}");
                    ReadExecutables(loop.Executables);
                }
                else if (executable.GetType() == typeof(ForLoop))
                {
                    //Console.WriteLine(string.Format("\tForEachLoop", e.GetType()));
                    ForLoop loop = executable as ForLoop;
                    //Console.WriteLine(string.Format("\t\tName = {0}", loop.Name));
                    //Console.WriteLine(string.Format("\t\tGetExecutionPath() = {0}", loop.GetExecutionPath()));
                    //Console.WriteLine(string.Format(fmt, e.GetType().Name, loop.Name));
                    //logFile.WriteLine(string.Format(fmt, e.GetType().Name, loop.Name));
                    //string.Format("{0}: {1}", executable.GetType().Name, loop.Name).Print(tabCount, ref LogFile);
                    Debug.Print($"executable name: {loop.Name}");
                    //_md.WriteTitle($"ForLoop: {loop.Name}");
                    ReadExecutables(loop.Executables);
                }
                else if (executable.GetType() == typeof(Package))
                {
                    //Console.WriteLine(string.Format("\tForEachLoop", e.GetType()));
                    Package loop = executable as Package;
                    //Console.WriteLine(string.Format("\t\tName = {0}", loop.Name));
                    //Console.WriteLine(string.Format("\t\tGetExecutionPath() = {0}", loop.GetExecutionPath()));
                    //Console.WriteLine(string.Format(fmt, e.GetType().Name, loop.Name));
                    //logFile.WriteLine(string.Format(fmt, e.GetType().Name, loop.Name));
                    Debug.Print($"Child Package: {loop.Name}");
                    //_md.WriteTitle($"executable name: {loop.Name}");
                    ReadExecutables(loop.Executables);
                }
                else
                {
                    //Console.WriteLine(string.Format(fmt, e.GetType(), "UNHANDLED executable type"));
                    //logFile.WriteLine(string.Format(fmt, e.GetType(), "UNHANDLED executable type"));
                    Debug.Print($"\n\n\n\tparsing of {executable.GetType()} executable type is NOT IMPLEMENTED\n\n\n");
                }
            }
            _nestedLevel -= 1;
            Debug.IndentLevel = _nestedLevel;
        }
        private void ParseExecuteSql(TaskHost taskHost)
        {
            Debug.Print($"Execute Sql Task: {taskHost.Name}");
            _nestedLevel += 1;
            Debug.IndentLevel = _nestedLevel;
            //_md.WriteTitle($"Execute Sql Task: {taskHost.Name}");
            ExecuteSQLTask executeSql = taskHost.InnerObject as ExecuteSQLTask;
            Debug.WriteLine($"Connection: {executeSql.Connection}");
            Debug.WriteLine($"SqlStatementSource: {executeSql.SqlStatementSource}");
            //_md.WriteCode($"{executeSql.SqlStatementSource}");
            var parameterBindings = executeSql.ParameterBindings;
            foreach (IDTSParameterBinding parameterBinding in parameterBindings)
            {
                Debug.WriteLine($"DtsVariableName: {parameterBinding.DtsVariableName}");
                _nestedLevel += 1;
                Debug.IndentLevel = _nestedLevel;
                Debug.WriteLine($"DataType Number: {parameterBinding.DataType}");
                Debug.WriteLine($"ParameterDirection: {parameterBinding.ParameterDirection}");
                _nestedLevel -= 1;
                Debug.IndentLevel = _nestedLevel;
            }
            _nestedLevel -= 1;
            Debug.IndentLevel = _nestedLevel;
            OnRaiseExecuteSqlEvent(new ExecuteSqlEventArgs(taskHost.Name));
        }
        private void ParseDataFlow(TaskHost taskHost)
        {
            string sourceQuery = null;
            string sourceConnectionManagerID = null;
            string destinationTableName = null;
            string destinationConnectionManagerID = null;
            Debug.Print($"Data Flow Task: {taskHost.Name}");
            _nestedLevel += 1;
            Debug.IndentLevel = _nestedLevel;
            // replaced with event handler pattern
            //_md.WriteTitle($"Data Flow Task: {taskHost.Name}");
            MainPipe mainPipe = taskHost.InnerObject as MainPipe;
            IDTSComponentMetaDataCollection100 metaDataCollection = mainPipe.ComponentMetaDataCollection;
            // loop over all components (eg. Destination, Source, Derived Column, etc)
            foreach (IDTSComponentMetaData100 componentMetaData in metaDataCollection)
            {
                string componentMetaDataType = componentMetaData.ContactInfo.Split(';')[0];
                Debug.Print($"component: {componentMetaDataType}");
                Debug.Print($"{nameof(componentMetaData.Name)}: {componentMetaData.Name}");
                Debug.Print($"{nameof(componentMetaData.Description)}: {componentMetaData.Description}");

                // try to get connection manager info (need runtime to validate connections... doesn't work unless connections can be resolved)
                try
                {
                    string tempConnectionManagerId = null;
                    Debug.Print($"{nameof(componentMetaData.RuntimeConnectionCollection)}: {componentMetaData.RuntimeConnectionCollection[0].ConnectionManagerID}");
                    IDTSRuntimeConnectionCollection100 runtimeConnectionCollection = componentMetaData.RuntimeConnectionCollection;
                    foreach (IDTSRuntimeConnection100 runtimeConnection in runtimeConnectionCollection)
                    {
                        tempConnectionManagerId = runtimeConnection.ConnectionManagerID;
                        if (componentMetaDataType == "OLE DB Destination")
                        {
                            destinationConnectionManagerID = tempConnectionManagerId;
                            Debug.Print($"DataFlow Destination Connection Manager: {destinationConnectionManagerID}");
                        }
                        else if (componentMetaDataType == "OLE DB Source")
                        {
                            sourceConnectionManagerID = tempConnectionManagerId;
                            Debug.Print($"DataFlow Source Connection Manager: {sourceConnectionManagerID}");
                        }
                    }
                }
                catch (Exception)
                {
                    destinationConnectionManagerID = "Unable to connect.";
                    sourceConnectionManagerID = "Unable to connect.";
                }

                IDTSCustomPropertyCollection100 customPropertyCollection = componentMetaData.CustomPropertyCollection;
                if (componentMetaDataType == "OLE DB Destination")
                {
                    if (customPropertyCollection.Count > 0)
                    {
                        _nestedLevel += 1;
                        Debug.IndentLevel = _nestedLevel;
                        foreach (IDTSCustomProperty100 customProperty in customPropertyCollection)
                        {
                            string valueStr = customProperty.Value as string;
                            if (customProperty.Name == "OpenRowset" && valueStr.Length > 0)
                            {
                                Debug.Print($"{nameof(customProperty.Name)}: {customProperty.Name}");
                                Debug.Print($"{nameof(customProperty.Value)}: {customProperty.Value}");
                                destinationTableName = valueStr;
                                //_md.WriteDataFlowDestinationTable(destinationTableName);
                            }
                        }
                        _nestedLevel -= 1;
                        Debug.IndentLevel = _nestedLevel;
                    }
                }
                else if (componentMetaDataType == "OLE DB Source")
                {
                    if (customPropertyCollection.Count > 0)
                    {
                        _nestedLevel += 1;
                        Debug.IndentLevel = _nestedLevel;
                        foreach (IDTSCustomProperty100 customProperty in customPropertyCollection)
                        {
                            string valueStr = customProperty.Value as string;
                            if (customProperty.Name == "SqlCommand" && valueStr.Length > 0)
                            {
                                Debug.Print($"{nameof(customProperty.Name)}: {customProperty.Name}");
                                Debug.Print($"{nameof(customProperty.Value)}: {customProperty.Value}");
                                sourceQuery = valueStr;
                                //_md.WriteCode(sourceQuery);
                            }
                        }
                        _nestedLevel -= 1;
                        Debug.IndentLevel = _nestedLevel;
                    }
                }

                OnRaiseDataFlowEvent(new DataFlowEventArgs(taskHost.Name, null, sourceQuery, null, destinationTableName, _nestedLevel));
            }
            if (sourceQuery == null)
            {
                Debug.Print($"sourceQuery not found");
                return;
            }
            if (destinationTableName == null)
            {
                Debug.Print($"destinationTableName not found");
                return;
            }
            if (sourceConnectionManagerID != null && destinationConnectionManagerID != null)
            {
                sourceQuery =
                    $"-- Source: {sourceConnectionManagerID}\n-- Destination: {destinationConnectionManagerID}\n\n" +
                    sourceQuery;
            }
            sourceQuery.ToFile($"{Path.Combine(Utilities.Cwd(), destinationTableName + ".sql")}");
            _nestedLevel -= 1;
            Debug.IndentLevel = _nestedLevel;
        }
    }
}
