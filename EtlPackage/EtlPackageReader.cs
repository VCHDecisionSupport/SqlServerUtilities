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
    public enum ExecutableTypeEnum
    {
        Package,
        Sequence,
        Loop,
        DataFlow,
        ExecuteSql,
        ChildPackage
    }
    public class SsisExecutable : EventArgs
    {
        public string ExecutableName { get; set; }
        public int NestedLevel { get; set; }
        public ExecutableTypeEnum ExecutableType { get; set; }
        public SsisExecutable(string ExecutableName, ExecutableTypeEnum ExecutableType)
        {
            this.ExecutableName = ExecutableName;
            this.ExecutableType = ExecutableType;
            NestedLevel = 1;
        }
    }
    public class DataFlowEventArgs : SsisExecutable
    {
        public string SourceDatabaseName { get; set; }
        public string SourceQuery { get; set; }
        public string DestinationDatabaseName { get; set; }
        public string DestinationTableName { get; set; }
        public DataFlowEventArgs(string DataFlowName, string SourceDatabaseName, string SourceQuery, string DestinationDatabaseName, string DestinationTableName, int NestedLevel) : base(DataFlowName, ExecutableTypeEnum.DataFlow)
        {
            this.SourceDatabaseName = SourceDatabaseName;
            this.SourceQuery = SourceQuery;
            this.DestinationDatabaseName = DestinationDatabaseName;
            this.DestinationTableName = DestinationTableName;
        }
    }
    public class PackageEventArgs : SsisExecutable
    {
        public PackageEventArgs(string PackageName) : base(PackageName, ExecutableTypeEnum.Package) { }
    }
    public class ExecuteSqlEventArgs : SsisExecutable
    {
        public string SqlSource { get; set; }
        public List<string> ParameterNames { get; set; }
        public ExecuteSqlEventArgs(string ExecuteSqlName, int NestedLevel, string SqlSource, List<string> ParameterNames) : base(ExecuteSqlName, ExecutableTypeEnum.ExecuteSql)
        {
            this.SqlSource = SqlSource;
            this.ParameterNames = ParameterNames;
        }
    }
    public class ChildPackageEventArgs : SsisExecutable
    {
        public ChildPackageEventArgs(string ChildPackageEventName, int NestedLevel) : base(ChildPackageEventName, ExecutableTypeEnum.ChildPackage) { }
    }
    public class LoopEventArgs : SsisExecutable
    {
        public LoopEventArgs(string LoopEventName, int NestedLevel) : base(LoopEventName, ExecutableTypeEnum.Loop) { }
    }
    public class SequenceEventArgs : SsisExecutable
    {
        public SequenceEventArgs(string SequenceEventName, int NestedLevel) : base(SequenceEventName, ExecutableTypeEnum.Sequence) { }
    }

    // publisher of DataFlowEventArgs events
    public class EtlPackageReader
    {
        private Application _application;
        private Package _package;
        public bool IsValid { get; set; } = true;
        public string PackageName
        {
            get
            {
                return _package.Name;
            }
        }
        public event EventHandler<PackageEventArgs> RaisePackageEvent;
        public event EventHandler<DataFlowEventArgs> RaiseDataFlowEvent;
        public event EventHandler<ExecuteSqlEventArgs> RaiseExecuteSqlEvent;
        public event EventHandler<SequenceEventArgs> RaiseSequenceEvent;
        public event EventHandler<LoopEventArgs> RaiseLoopEvent;
        public event EventHandler<ChildPackageEventArgs> RaiseChildPackageEvent;
        private int NestedLevel { get; set; }
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
        protected virtual void OnRaiseSequenceEvent(SequenceEventArgs SequenceEventArgs)
        {
            EventHandler<SequenceEventArgs> handler = RaiseSequenceEvent;
            // Event will be null if there are no subscribers
            if (handler != null)
            {
                // Format the string to send inside the CustomEventArgs parameter
                //e.Message += String.Format(" at {0}", DateTime.Now.ToString());

                // Use the () operator to raise the event.
                handler(this, SequenceEventArgs);
            }
        }
        protected virtual void OnRaiseLoopEvent(LoopEventArgs LoopEventArgs)
        {
            EventHandler<LoopEventArgs> handler = RaiseLoopEvent;
            // Event will be null if there are no subscribers
            if (handler != null)
            {
                // Format the string to send inside the CustomEventArgs parameter
                //e.Message += String.Format(" at {0}", DateTime.Now.ToString());

                // Use the () operator to raise the event.
                handler(this, LoopEventArgs);
            }
        }
        protected virtual void OnRaiseChildPackageEvent(ChildPackageEventArgs ChildPackageEventArgs)
        {
            EventHandler<ChildPackageEventArgs> handler = RaiseChildPackageEvent;
            // Event will be null if there are no subscribers
            if (handler != null)
            {
                // Format the string to send inside the CustomEventArgs parameter
                //e.Message += String.Format(" at {0}", DateTime.Now.ToString());

                // Use the () operator to raise the event.
                handler(this, ChildPackageEventArgs);
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
            bool packageExists;
            try
            {
                //packageExists = _application.ExistsOnDtsServer(ssisEtlPath, SsisServerName);
                _package = _application.LoadFromDtsServer(ssisEtlPath, SsisServerName, idtsEvents);
            }
            catch (Exception e)
            {
                _package = null;
                Debug.WriteLine(e.GetType());
                Debug.WriteLine(e.Message);
                Debug.WriteLine($"ERROR package ({ssisEtlPath}) NOT found on Dts Server: {SsisServerName}\nloading...");
                //throw new Exception($"EtlPackageReader({ssisEtlPath} on {SsisServerName}) not found");
                IsValid = false;
            }
            if (_package != null)
            {   
                Console.WriteLine($"Package: {_package.Name} loaded.");
            }
        }
        public void ReadConnectionManagers()
        {
            NestedLevel += 1;
            Debug.IndentLevel = NestedLevel;
            foreach (ConnectionManager connectionManager in _package.Connections)
            {
                Debug.Print($"Connection Manager Name: {connectionManager.Name}");
                NestedLevel += 1;
                Debug.IndentLevel = NestedLevel;
                Debug.Print($"Connection Manager Server: {SqlUtilities.ExtractServerName(connectionManager.ConnectionString)}");
                Debug.Print($"Connection Manager Database: {SqlUtilities.ExtractDatabaseName(connectionManager.ConnectionString)}");
                NestedLevel -= 1;
                Debug.IndentLevel = NestedLevel;
            }
            NestedLevel -= 1;
            Debug.IndentLevel = NestedLevel;
        }
        public void ProcessPackage()
        {
            List<string> to = new List<string>();
            List<string> from = new List<string>();
            foreach (var precedenceConstraint in _package.PrecedenceConstraints)
            {
                Debug.WriteLine(precedenceConstraint.ConstrainedExecutable.ToString());
            }
            ReadExecutables();
        }
        private void ReadExecutables()
        {
            OnRaisePackageEvent(new PackageEventArgs(_package.Name));
            Executables executables = _package.Executables;
            ReadExecutables(executables);
            //_md.Close();
        }
        private void ReadExecutables(Executables executables)
        {
            NestedLevel += 1;
            Debug.IndentLevel = NestedLevel;
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
                    OnRaiseSequenceEvent(new SequenceEventArgs(sequence.Name, NestedLevel));
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
                    OnRaiseLoopEvent(new LoopEventArgs(loop.Name, NestedLevel));
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
                    OnRaiseLoopEvent(new LoopEventArgs(loop.Name, NestedLevel));
                    //_md.WriteTitle($"ForLoop: {loop.Name}");
                    ReadExecutables(loop.Executables);
                }
                else if (executable.GetType() == typeof(Package))
                {
                    //Console.WriteLine(string.Format("\tForEachLoop", e.GetType()));
                    Package pkg = executable as Package;
                    //Console.WriteLine(string.Format("\t\tName = {0}", loop.Name));
                    //Console.WriteLine(string.Format("\t\tGetExecutionPath() = {0}", loop.GetExecutionPath()));
                    //Console.WriteLine(string.Format(fmt, e.GetType().Name, loop.Name));
                    //logFile.WriteLine(string.Format(fmt, e.GetType().Name, loop.Name));
                    OnRaiseChildPackageEvent(new ChildPackageEventArgs(pkg.Name, NestedLevel));
                    Debug.Print($"Child Package: {pkg.Name}");
                    //_md.WriteTitle($"executable name: {loop.Name}");
                    ReadExecutables(pkg.Executables);
                }
                else
                {
                    //Console.WriteLine(string.Format(fmt, e.GetType(), "UNHANDLED executable type"));
                    //logFile.WriteLine(string.Format(fmt, e.GetType(), "UNHANDLED executable type"));
                    Debug.Print($"\n\n\n\tparsing of {executable.GetType()} executable type is NOT IMPLEMENTED\n\n\n");
                }
            }
            NestedLevel -= 1;
            Debug.IndentLevel = NestedLevel;
        }
        private void ParseExecuteSql(TaskHost taskHost)
        {
            Debug.Print($"Execute Sql Task: {taskHost.Name}");
            NestedLevel += 1;
            Debug.IndentLevel = NestedLevel;
            //_md.WriteTitle($"Execute Sql Task: {taskHost.Name}");
            ExecuteSQLTask executeSql = taskHost.InnerObject as ExecuteSQLTask;
            Debug.WriteLine($"Connection: {executeSql.Connection}");
            Debug.WriteLine($"SqlStatementSource: {executeSql.SqlStatementSource}");
            //_md.WriteCode($"{executeSql.SqlStatementSource}");
            var parameterBindings = executeSql.ParameterBindings;
            List<string> parameterNames = new List<string>();
            foreach (IDTSParameterBinding parameterBinding in parameterBindings)
            {
                Debug.WriteLine($"DtsVariableName: {parameterBinding.DtsVariableName}");
                NestedLevel += 1;
                Debug.IndentLevel = NestedLevel;
                Debug.WriteLine($"DataType Number: {parameterBinding.DataType}");
                Debug.WriteLine($"ParameterDirection: {parameterBinding.ParameterDirection}");
                NestedLevel -= 1;
                Debug.IndentLevel = NestedLevel;
                parameterNames.Add(parameterBinding.DtsVariableName);
            }
            NestedLevel -= 1;
            Debug.IndentLevel = NestedLevel;
            OnRaiseExecuteSqlEvent(new ExecuteSqlEventArgs(taskHost.Name, NestedLevel, executeSql.SqlStatementSource, parameterNames));
        }
        private void ParseDataFlow(TaskHost taskHost)
        {
            string sourceQuery = null;
            string sourceConnectionManagerID = null;
            string sourceDatabaseName = null;
            string destinationTableName = null;
            string destinationConnectionManagerID = null;
            string destinationDatabaseName = null;
            Debug.Print($"Data Flow Task: {taskHost.Name}");
            NestedLevel += 1;
            Debug.IndentLevel = NestedLevel;
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
                    Debug.Print($"{nameof(componentMetaData.RuntimeConnectionCollection)}: {componentMetaData.RuntimeConnectionCollection[0].ConnectionManagerID}");
                    IDTSRuntimeConnectionCollection100 runtimeConnectionCollection = componentMetaData.RuntimeConnectionCollection;
                    foreach (IDTSRuntimeConnection100 runtimeConnection in runtimeConnectionCollection)
                    {
                        if (componentMetaDataType == "OLE DB Destination")
                        {
                            destinationConnectionManagerID = runtimeConnection.ConnectionManagerID;
                            Debug.Print($"DataFlow Destination Connection Manager: {destinationConnectionManagerID}");
                        }
                        else if (componentMetaDataType == "OLE DB Source")
                        {
                            sourceConnectionManagerID = runtimeConnection.ConnectionManagerID;
                            Debug.Print($"DataFlow Source Connection Manager: {sourceConnectionManagerID}");
                        }
                    }
                }
                catch (Exception)
                {
                    if (destinationConnectionManagerID == null)
                    {
                        destinationConnectionManagerID = "Unable to connect.";
                    }
                    if (sourceConnectionManagerID == null)
                    {
                        sourceConnectionManagerID = "Unable to connect.";
                    }
                }

                IDTSCustomPropertyCollection100 customPropertyCollection = componentMetaData.CustomPropertyCollection;
                if (componentMetaDataType == "OLE DB Destination")
                {
                    if (customPropertyCollection.Count > 0)
                    {
                        NestedLevel += 1;
                        Debug.IndentLevel = NestedLevel;
                        foreach (IDTSCustomProperty100 customProperty in customPropertyCollection)
                        {
                            string valueStr = customProperty.Value as string;
                            if (customProperty.Name == "OpenRowset" && valueStr.Length > 0)
                            {
                                Debug.Print($"{nameof(customProperty.Name)}: {customProperty.Name}");
                                Debug.Print($"{nameof(customProperty.Value)}: {customProperty.Value}");
                                destinationTableName = valueStr;
                                destinationDatabaseName = "[" + SqlUtilities.ExtractDatabaseName(_package.Connections[destinationConnectionManagerID].ConnectionString) + "]";
                                //_md.WriteDataFlowDestinationTable(destinationTableName);
                            }
                        }
                        NestedLevel -= 1;
                        Debug.IndentLevel = NestedLevel;
                    }
                }
                else if (componentMetaDataType == "OLE DB Source")
                {
                    if (customPropertyCollection.Count > 0)
                    {
                        NestedLevel += 1;
                        Debug.IndentLevel = NestedLevel;
                        foreach (IDTSCustomProperty100 customProperty in customPropertyCollection)
                        {
                            string valueStr = customProperty.Value as string;
                            if (customProperty.Name == "SqlCommand" && valueStr.Length > 0)
                            {
                                Debug.Print($"{nameof(customProperty.Name)}: {customProperty.Name}");
                                Debug.Print($"{nameof(customProperty.Value)}: {customProperty.Value}");
                                sourceQuery = valueStr;
                                sourceDatabaseName = "[" + SqlUtilities.ExtractDatabaseName(_package.Connections[sourceConnectionManagerID].ConnectionString) + "]";
                                //_md.WriteCode(sourceQuery);
                            }
                        }
                        NestedLevel -= 1;
                        Debug.IndentLevel = NestedLevel;
                    }
                }
            }
            OnRaiseDataFlowEvent(new DataFlowEventArgs(taskHost.Name, sourceDatabaseName, sourceQuery, destinationDatabaseName, destinationTableName, NestedLevel));
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
            
            NestedLevel -= 1;
            Debug.IndentLevel = NestedLevel;
        }
    }
}
