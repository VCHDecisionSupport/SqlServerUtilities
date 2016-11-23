using System;
using System.CodeDom.Compiler;
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

        static void test_EtlPackageReader()
        {
            EtlPackageReader etlPackageReader = new EtlPackageReader("C:\\Users\\user\\Dropbox\\Vault\\ECommunity\\Dev\\Landing\\ETL\\ChildPARISLandingA_E.dtsx");
            //EtlPackageReader etlPackageReader = new EtlPackageReader("C:\\Users\\user\\Source\\Repos\\SqlServerUtilities\\SqlServerUtilities\\SandBox\\\\ChildPARISLandingA_E.dtsx");
            etlPackageReader.ReadExecutables();
        }

        static void test_EtlPackageReaderRecursive()
        {
            string currentWorkingDirectory = Utilities.Cwd();
            throw new NotImplementedException();
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
            else if (args.Length != 1 || !String.Equals(args[0], "-IncludeSubFolders", StringComparison.CurrentCultureIgnoreCase))
            {
                System.Console.WriteLine("Error.  Invalid argument.\nUsage: -IncludeSubFolders");
            }
            else if (args.Length == 1 || String.Equals(args[0], "-IncludeSubFolders", StringComparison.CurrentCultureIgnoreCase))
            {
                System.Console.WriteLine("Parsing Etl Packages in current working directory is subfolders...");
                test_EtlPackageReaderRecursive();
            }
        }
        static int Main(string[] args)
        {
            TextWriterTraceListener debugWriter = new TextWriterTraceListener(System.Console.Out);
            Debug.Listeners.Add(debugWriter);
            
            
            //test_EtlPackageReader();


            Console.WriteLine($"execution complete.  press any key to exit.");
            Console.ReadKey();
            return 0;
        }

        
    }
}
