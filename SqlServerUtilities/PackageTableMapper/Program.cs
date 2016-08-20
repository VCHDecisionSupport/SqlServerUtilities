using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace PackageTableMapper
{
    class Program
    {
        static void Parse_MsdbEtl()
        {
            string serverName = Environment.MachineName;
            Console.WriteLine("current MachineName: {0}", serverName);

            if (!serverName.Contains("DBDECSUP0"))
            {
                Console.WriteLine("current machine name does not contain \"DBDECSUP0\": defaulting to DEV Sql Server: STDBDECSUP01", serverName);
                serverName = "STDBDECSUP01";
            }
            string databaseName = "MSDB";
            MsdbReader rdr = new MsdbReader(serverName, databaseName);
            rdr.BreadthFirstCrawl();
            foreach (string key in rdr.PkgTablePairs.Keys)
            {
                Console.WriteLine(string.Format("Package Name: {0}\n\tTable Names:", key));
                foreach (var value in rdr.PkgTablePairs[key])
                {
                    Console.WriteLine(string.Format("\t{0}", value));
                }
            }
            //rdr.SaveToSqlScript();
            //rdr.InsertInto();
        }
        static void Main(string[] args)
        {
            Parse_MsdbEtl();
        }
    }
}
