using System;
using System.CodeDom.Compiler;
using System.Data;
using System.Data.SqlClient;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;
using Microsoft.SqlServer.Management.Smo;

namespace EtlPackage
{
    public class Program
    {
        static void test_EtlPackage_init()
        {
            EtlPackageBuilder etlPackageBuilder = new EtlPackageBuilder("etlPackageBuilder");
            etlPackageBuilder.AddOleDbConnectionManager(new Database(new Server("STDBDECSUP01"), "DSDW"));
            etlPackageBuilder.AddOleDbConnectionManager(new Database(new Server("STDBDECSUP01"), "DSDW"));
            etlPackageBuilder.AddOleDbConnectionManager(new Database(new Server("STDBDECSUP01"), "CommunityMart"));
            etlPackageBuilder.AddOleDbConnectionManager(new Database(new Server("STDBDECSUP01"), "CommunityMart"));
        }

        static void test_EtlPackageReader_FileSystem()
        {
            EtlPackageReader etlPackageReader =
                new EtlPackageReader(
                    "C:\\Users\\gcrowell\\Dropbox\\Vault\\ECommunity\\Dev\\Landing\\ETL\\ChildPARISLandingA_E.dtsx");
            //EtlPackageReader etlPackageReader = new EtlPackageReader("C:\\Users\\user\\Source\\Repos\\SqlServerUtilities\\SqlServerUtilities\\SandBox\\\\ChildPARISLandingA_E.dtsx");
            etlPackageReader.ReadExecutables();
        }
        static void test_EtlPackageReader_Msdb()
        {
            EtlPackageReader etlPackageReader = new EtlPackageReader("STDBDECSUP01", @"MSDB\Community\PopulateCommunityMart\PopulateCommunityMart");
            etlPackageReader.ReadExecutables();
        }
        private static void test_EtlPackageReaderFolder()
        {
            string currentWorkingDirectory = Utilities.Cwd();
            var etlPackagePaths = Directory.EnumerateFiles(currentWorkingDirectory, "*.dtsx").ToList();
            if (etlPackagePaths.Count == 0)
            {
                Console.WriteLine($"No *.dtsx files found in directory: {currentWorkingDirectory}");
            }
            foreach (string etlPackagePath in etlPackagePaths)
            {
                EtlPackageReader etlPackageReader = new EtlPackageReader(etlPackagePath);
                etlPackageReader.ReadConnectionManagers();
                etlPackageReader.ReadExecutables();
            }
        }

        static void HandleCommandLineArgs(string[] args)
        {
            if (args.Length == 0)
            {
                System.Console.WriteLine("Parsing Etl Packages in current working directory...");
                test_EtlPackageReaderFolder();
            }
            else if (args.Length != 1 ||
                     !String.Equals(args[0], "-IncludeSubFolders", StringComparison.CurrentCultureIgnoreCase))
            {
                System.Console.WriteLine("Error.  Invalid argument.\nUsage: -IncludeSubFolders");
            }
            else if (args.Length == 1 ||
                     String.Equals(args[0], "-IncludeSubFolders", StringComparison.CurrentCultureIgnoreCase))
            {
                System.Console.WriteLine("Parsing Etl Packages in current working directory is subfolders...");
                //test_EtlPackageReaderRecursive();
            }
        }

        public static void tables_to_views()
        {
            string sharePointServerName = "SPDBSSPS008";
            string dsDevServerName = "STDBDECSUP01";
            string sharePointDatabaseName = "DSSPPROD_UsageAndHealth";

            SqlConnection dsDevConnection = SqlUtilities.GetSqlConnection(dsDevServerName);
            dsDevConnection.ChangeDatabase(sharePointDatabaseName);
            Debug.WriteLine(dsDevConnection.State);
            Database database = SqlUtilities.GetDatabase(dsDevServerName, sharePointDatabaseName);
            foreach (View table in database.Views)
            {
                var sqlScript = string.Format("DROP VIEW {0}.{1};", table.Schema, table.Name);
                Debug.WriteLine(sqlScript);
                SqlCommand sqlCommand = dsDevConnection.CreateCommand();
                sqlCommand.CommandText = sqlScript;
                sqlCommand.ExecuteNonQuery();
                //sqlScript = string.Format("CREATE VIEW {0}.{1}\nAS\nSELECT *\nFROM {2}.{0}.{1};", table.Schema, table.Name,"gcDev");
                //Debug.WriteLine(sqlScript);
                //sqlCommand = dsDevConnection.CreateCommand();
                //sqlCommand.CommandText = sqlScript;
                //sqlCommand.ExecuteNonQuery();
            }
        }

        public static void test_Connectivity()
        {
            string[] serverNames = { "SPDBDECSUP04", "STDBDECSUP03", "STDBDECSUP02", "STDBDECSUP01" };
            foreach (string serverName in serverNames)
            {
                try
                {
                    Server server = new Server(serverName);
                    Debug.WriteLine("connecting to {0}", serverName, serverName);
                    var sqlConntection = server.ConnectionContext.SqlConnectionObject;
                    sqlConntection.Open();
                    if (sqlConntection.State == ConnectionState.Open)
                    {
                        Debug.WriteLine("\tconnected to {0}", serverName, serverName);
                    }
                }
                catch (Exception e)
                {
                    Debug.WriteLine("ERROR did not connect to {0}", serverName);
                    Debug.WriteLine(e.Message);
                }
            }
        }
        public static void build_SharePointLogEtl()
        {

            EtlPackageBuilder sharePointUsageLogEtl = new EtlPackageBuilder("SharePointUsageLogEtl");
            string sharePointServerName = "SPDBSSPS008";
            string dsDevServerName = "STDBDECSUP01";
            string sharePointDatabaseName = "DSSPPROD_UsageAndHealth";
            string dsDatabaseName = "DSSPPROD_UsageAndHealth";
            string dsMockSourceDatabaseName = "gcDev";
            sharePointUsageLogEtl.AddOleDbConnectionManager(sharePointServerName, sharePointDatabaseName);
            sharePointUsageLogEtl.AddOleDbConnectionManager(dsDevServerName, sharePointDatabaseName);

            //Server sharePointServer = new Server(sharePointServerName);
            //Database sharePointDatabase = new Database(sharePointServer, sharePointDatabaseName);

            Server dsDevServer = new Server(dsDevServerName);
            Debug.WriteLine("Destination server databases found: {0}", dsDevServer.Databases.Count);

            Database dsDevDatabase = dsDevServer.Databases[dsDatabaseName];
            Debug.WriteLine("Destination tables found: {0}", dsDevDatabase.Tables.ToString());

            foreach (Table table in dsDevDatabase.Tables)
            {
                Debug.WriteLine("Data object: {0}", table.Name);

                //sharePointUsageLogEtl.AddOleDataFlow(sharePointServerName, sharePointDatabaseName, table.Schema, table.Name, table.Parent.Parent.Name, table.Parent.Name, table.Schema, table.Name);
            }
        }
        public static void test_MarkDownWriter()
        {
            MarkDownWriter md = new MarkDownWriter("fuckit.md");
            md.WriteTitle("this is a title");
            md.WriteCode("SELECT * FROM nowhere;");
            md.Close();
        }
        static int Main(string[] args)
        {
            TextWriterTraceListener debugWriter = new TextWriterTraceListener(System.Console.Out);
            Debug.Listeners.Add(debugWriter);

            TextWriterTraceListener textDebugger = new TextWriterTraceListener(Utilities.Cwd() + "/pkg.md");
            Debug.Listeners.Add(textDebugger);

            //tables_to_views();
            //build_SharePointLogEtl();

            //test_EtlPackageReader_Msdb();
            test_MarkDownWriter();

            Console.WriteLine($"\n\nexecution complete.  press any key to exit.");
            Console.ReadKey();
            return 0;
        }


    }
}
