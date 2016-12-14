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
        public MarkDownWriter(string path, EtlPackageReader etlPackageReader) : base(path)
        {
            base.AutoFlush = true;
            NestedLevel = 1;
            etlPackageReader.RaiseDataFlowEvent += HandleDataFlowEvent;
            etlPackageReader.RaisePackageEvent += HandlePackageEvent;
            etlPackageReader.RaiseExecuteSqlEvent += HandleExecuteSqlEvent;
        }
        // Define what actions to take when the event is raised.
        void HandleDataFlowEvent(object sender, DataFlowEventArgs dataFlowEventArgs)
        {
            this.NestedLevel = dataFlowEventArgs.NestedLevel;
            WriteTitle(dataFlowEventArgs.DataFlowName);
        }
        void HandlePackageEvent(object sender, PackageEventArgs packageEventArgs)
        {
            WriteTitle(packageEventArgs.PackageName);
        }
        void HandleExecuteSqlEvent(object sender, ExecuteSqlEventArgs executeSqlEventArgs)
        {
            WriteTitle(executeSqlEventArgs.ExecuteSqlName);
        }
        public int NestedLevel { get; set; }
        public void WriteTitle(string title)
        {
            string markup = new string('#', NestedLevel);
            this.Write(string.Format($"\n{markup} {title}\n"));
        }
        public void WriteCode(string code)
        {
            this.WriteLine('\t'+code.Replace("\n\n","\n").Replace("    ","\t").Replace("\n","\n\t").Trim());
            //Regex d = new Regex(@"[\n]*([.]*)[\n]*\Z");
            //code = d.Match(code).Groups[0].Value;
            //this.WriteLine(code);
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
