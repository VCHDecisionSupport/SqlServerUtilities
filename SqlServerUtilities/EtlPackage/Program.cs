using Microsoft.SqlServer.Dts.Runtime;
using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;

namespace EtlPackage
{

    public class Program
    {
        public static void CliUsage()
        {
            Dictionary<string, string> arg_usage = new Dictionary<string, string>();
            arg_usage.Add("-MarkDown", "(switch) DEFAULT.  markdown documentation will be produced (readme.md).");
            arg_usage.Add("-InsertPackageMap", "(switch) Map.PackageTable will be merged against dataflows destinations.  Requires a valid -ServerName argument.");
            arg_usage.Add(@"-MsdbPath ""<msdb_package_path>""", @"Packages deployed to MSDB path: <msdb_package_path> will be processed.  If <msdb_package_path> is a directory than each package will be processed.  Requires a valid -ServerName argument.");
            arg_usage.Add(@"-LocalPath ""<full path>""", @"Package files (*.dtsx) at <full path> will be processed.  If <full path> is a directory than each package will be processed.");
            arg_usage.Add("-ServerName <server_name", "If -InsertPackageMap then Map.PackageTable will be populated on <server_name>.  If -MsdbPath then MSDB path on <server_name> will be queries for deployed packages.");
            Debug.WriteLine($"\n\nusage: SqlUtilities ...\nwhere ... is:\n");
            foreach (string key in arg_usage.Keys)
            {
                string usage = $"{key,-30}\n\t{arg_usage[key]}";
                Debug.WriteLine(usage);
            }
        }
        public static void CliHandler(string[] args)
        {
            List<string> args_list = new List<string>(args);
            bool MarkDown = args_list.Contains("-MarkDown");
            bool InsertPackageMap = args_list.Contains("-InsertPackageMap");
            int MsdbPathIndex = args_list.FindIndex(x => x == "-MsdbPath");
            int LocalPathIndex = args_list.FindIndex(x => x == "-LocalPath");
            int ServerNameIndex = args_list.FindIndex(x => x == "-ServerName");
            Debug.WriteLine($"{nameof(MarkDown)}: {MarkDown}");
            Debug.WriteLine($"{nameof(InsertPackageMap)}: {InsertPackageMap}");
            Debug.WriteLine($"{nameof(MsdbPathIndex)}: {MsdbPathIndex}");
            Debug.WriteLine($"{nameof(LocalPathIndex)}: {LocalPathIndex}");
            Debug.WriteLine($"{nameof(ServerNameIndex)}: {ServerNameIndex}");
            string server_name = "";
            string MsdbPath = "";
            string LocalPath = "";
            if (MsdbPathIndex != -1 && LocalPathIndex != -1)
            {
                Debug.WriteLine("ERROR: MsdbPath and LocalPath cannot be used at same time.");
                //throw new Exception("ERROR: MsdbPath and LocalPath cannot be used at same time.");
                Environment.Exit(0);
                return;
            }
            if (MsdbPathIndex != -1 || InsertPackageMap)
            {
                // check for valid server name
                server_name = args_list[ServerNameIndex + 1];
                Debug.WriteLine($"{nameof(server_name)}: {server_name}");
                SqlConnection sqlCon = SqlUtilities.GetSqlConnection(server_name);
                // server exists
            }
            if(MsdbPathIndex != -1)
            { 
                Application app = new Application();
                MsdbPath = args_list[MsdbPathIndex + 1];
                if(!app.FolderExistsOnSqlServer(MsdbPath, server_name, null, null) && !app.ExistsOnSqlServer(MsdbPath, server_name,null,null))
                {
                    Debug.WriteLine($"ERROR: MsdbPath:{MsdbPath} doesn't exist on server: {server_name}.");
                    Environment.Exit(0);
                    return;
                }
                else
                {
                    if (app.FolderExistsOnSqlServer(MsdbPath, server_name, null, null))
                    {
                        //TODO:...
                    }
                    else if(app.ExistsOnSqlServer(MsdbPath, server_name, null, null))
                    {
                        //TODO:...
                    }
                }
            }
            if (LocalPathIndex != -1)
            {
                LocalPath = args_list[LocalPathIndex + 1];
                if(!Directory.Exists(LocalPath) && !File.Exists(LocalPath))
                {
                    Debug.WriteLine($"ERROR: LocalPath:{LocalPath} doesn't exist.");
                    Environment.Exit(0);
                    return;
                }
                else
                {
                    if (Directory.Exists(LocalPath))
                    {
                        var etlPackagePaths = Directory.EnumerateFiles(LocalPath, "*.dtsx").ToList();
                        // TODO...
                    }
                    if (File.Exists(LocalPath))
                    {
                        // TODO...
                    }
                }
            }
        }
        static int Main(string[] args)
        {
            TextWriterTraceListener debugWriter = new TextWriterTraceListener(System.Console.Out);
            Debug.Listeners.Add(debugWriter);

            //TextWriterTraceListener textDebugger = new TextWriterTraceListener(Utilities.Cwd() + "/EtlPackageReaderLog.md");
            //Debug.Listeners.Add(textDebugger);

            //tables_to_views();
            //build_SharePointLogEtl();

            //test_RepoCrawl();
            //test_MarkDownWriter();
            //test_regex();
            //test_EtlPackageReader_FileSystem();
            //UnitTest.test_EtlPackageReaderFolder_MarkDown();
            //UnitTest.test_crawl_msdb();
            CliUsage();

            CliHandler(args);

            Console.WriteLine($"\n\nexecution complete.  press any key to exit.");
            Console.ReadKey();
            return 0;
        }


    }
}
