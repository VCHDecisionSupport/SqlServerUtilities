using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

namespace EtlPackage
{
    public class MarkDownWriter : StreamWriter
    {
        public MarkDownWriter(string path) : base(path)
        {
            base.AutoFlush = true;
            NestedLevel = 1;
            WriteTitle(Path.GetFileNameWithoutExtension(path));
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
