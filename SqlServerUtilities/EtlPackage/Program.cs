using System;
using System.Diagnostics;
using System.Text.RegularExpressions;
using Microsoft.SqlServer.Management.Smo;

namespace EtlPackage
{
    public class Program
    {
        static void test_EtlPackage_init()
        {
            EtlPackage etlPackage = new EtlPackage("etlPackage");
            etlPackage.AddOleDbConnectionManager(new Database(new Server("STDBDECSUP01"), "DSDW"));
            etlPackage.AddOleDbConnectionManager(new Database(new Server("STDBDECSUP01"), "DSDW"));
            etlPackage.AddOleDbConnectionManager(new Database(new Server("STDBDECSUP01"), "CommunityMart"));
            etlPackage.AddOleDbConnectionManager(new Database(new Server("STDBDECSUP01"), "CommunityMart"));
        }
        static void Main(string[] args)
        {
            TextWriterTraceListener myWriter = new
            TextWriterTraceListener(System.Console.Out);
            Debug.Listeners.Add(myWriter);

            //Database database = SqlUtilities.GetDatabase("STDBDECSUP01", "CommunityMart");
            //foreach (Table table in database.Tables)
            //{
            //    Debug.WriteLine(table.Urn);
            //    // Server[@Name='STDBDECSUP01']/Database[@Name='CommunityMart']/Table[@Name='WaitlistType' and @Schema='Dim']

            //}

            string urn = @"Server[@Name='STDBDECSUP01']/Database[@Name='CommunityMart']/Table[@Name='WaitlistType' and @Schema='Dim']";
            //Regex regex = new Regex(@"(\w+)\[@(\w+)='(\w+)'\]/(\w+)\[@(\w+)='(\w+)'\].*");
            Regex regex = new Regex(@"Server\[@Name='(?<ServerName>\w+)'\]/Database\[@Name='(?<DatabaseName>\w+)'\]/(?<ObjectTypeName>\w+)\[@Name='(?<ObjectName>\w+)' and @Schema='(?<ObjectSchemaName>\w+)'\].*");
            MatchCollection matches = regex.Matches(urn);
            foreach (Match match in matches)
            {
                GroupCollection groups = match.Groups;

                Debug.WriteLine(groups["ServerName"].Value);
                Debug.WriteLine(groups["DatabaseName"].Value);
                Debug.WriteLine(groups["ObjectTypeName"].Value);
                Debug.WriteLine(groups["ObjectName"].Value);
                Debug.WriteLine(groups["ObjectSchemaName"].Value);
            }
            Console.ReadKey();
        }
    }
}
