using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

namespace EtlPackage
{
    // subscribes to events from EtlPackageReader objects
    public class MarkDownWriter : StreamWriter
    {
        public int NestedLevel { get; set; }
        public MarkDownWriter(string path, EtlPackageReader etlPackageReader) : base(path)
        {
            base.AutoFlush = true;
            NestedLevel = 1;
            etlPackageReader.RaiseDataFlowEvent += HandleDataFlowEvent;
            etlPackageReader.RaisePackageEvent += HandlePackageEvent;
            etlPackageReader.RaiseExecuteSqlEvent += HandleExecuteSqlEvent;
            etlPackageReader.RaiseChildPackageEvent += HandleChildPackageEvent;
            etlPackageReader.RaiseLoopEvent += HandleLoopEvent;
            etlPackageReader.RaiseSequenceEvent += HandleSequenceEvent;
            etlPackageReader.RaiseChildPackageEvent += HandleChildPackageEvent;
        }
        public MarkDownWriter(string path) : base(path)
        {
            base.AutoFlush = true;
            NestedLevel = 1;
        }
        public void SetEtlPackageReader(EtlPackageReader etlPackageReader)
        {
            etlPackageReader.RaiseDataFlowEvent += HandleDataFlowEvent;
            etlPackageReader.RaisePackageEvent += HandlePackageEvent;
            etlPackageReader.RaiseExecuteSqlEvent += HandleExecuteSqlEvent;
            etlPackageReader.RaiseChildPackageEvent += HandleChildPackageEvent;
            etlPackageReader.RaiseLoopEvent += HandleLoopEvent;
            etlPackageReader.RaiseSequenceEvent += HandleSequenceEvent;
            etlPackageReader.RaiseChildPackageEvent += HandleChildPackageEvent;
        }
        // Define what actions to take when the event is raised.
        void HandleDataFlowEvent(object sender, DataFlowEventArgs dataFlowEventArgs)
        {
            this.NestedLevel = dataFlowEventArgs.NestedLevel;
            string dataFlowMarkDown = $"__DataFlow: {dataFlowEventArgs.ExecutableName}__\n\t_Destination Table: {dataFlowEventArgs.DestinationTableName}_\n\n";
            this.Write(dataFlowMarkDown);
            this.WriteCode(dataFlowEventArgs.SourceQuery);
        }
        void HandlePackageEvent(object sender, PackageEventArgs packageEventArgs)
        {
            this.NestedLevel = packageEventArgs.NestedLevel;
            WriteTitle(packageEventArgs.ExecutableName);
        }
        void HandleExecuteSqlEvent(object sender, ExecuteSqlEventArgs executeSqlEventArgs)
        {
            this.NestedLevel = executeSqlEventArgs.NestedLevel;
            string executeSqlMarkDown = $"__Execute Sql: {executeSqlEventArgs.ExecutableName}__\n\n";
            this.Write(executeSqlMarkDown);
            WriteCode(executeSqlEventArgs.SqlSource);
        }
        void HandleChildPackageEvent(object sender, ChildPackageEventArgs childPackageArgs)
        {
            this.NestedLevel = childPackageArgs.NestedLevel;
            WriteTitle(childPackageArgs.ExecutableName);
        }
        void HandleLoopEvent(object sender, LoopEventArgs loopEventArgs)
        {
            this.NestedLevel = loopEventArgs.NestedLevel;
            WriteTitle(loopEventArgs.ExecutableName);
        }
        void HandleSequenceEvent(object sender, SequenceEventArgs sequenceEventArgs)
        {
            this.NestedLevel = sequenceEventArgs.NestedLevel;
            WriteTitle($"Sequence: {sequenceEventArgs.ExecutableName}");
        }
        public void WriteTitle(string title)
        {
            string markup = new string('#', NestedLevel);
            this.Write(string.Format($"\n{markup} {title}\n"));
        }
        public void WriteCode(string code)
        {
            if (code != null && code != "")
            {
                this.WriteLine('\t' + code.Replace("\n\n", "\n").Replace("    ", "\t").Replace("\n", "\n\t").Trim());
                //Regex d = new Regex(@"[\n]*([.]*)[\n]*\Z");
                //code = d.Match(code).Groups[0].Value;
                //this.WriteLine(code);
            }
            else
            {
                this.WriteLine('\t' + "-- no sql code found.");
            }
        }
        public override Encoding Encoding
        {
            get { return Encoding.Default; }
        }
        internal void WriteDataFlowDestinationTable(string destinationTableName)
        {
            this.Write($"_Destination Table:_ __`{destinationTableName}`__\n\n");
        }
    }
}
