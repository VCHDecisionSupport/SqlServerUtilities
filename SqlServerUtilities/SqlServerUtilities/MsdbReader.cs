using Microsoft.SqlServer.Dts.Runtime;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SqlServerUtilities
{
    public class MsdbReader
    {
        string serverName { get; set; }
        string rootDtsFolder { get; set; }
        Application ssisApplication { get; set; }
        public Dictionary<string, List<string>> PkgTablePairs { get; set; }
        public MsdbReader()
        {
            this.serverName = "STDBDECSUP01";
            this.rootDtsFolder = "MSDB";
            ssisApplication = new Application();

        }
        public MsdbReader(string serverName)
        {
            this.serverName = serverName;
            this.rootDtsFolder = "MSDB";
            ssisApplication = new Application();
        }
        public MsdbReader(string serverName, string rootDtsFolder)
        {
            this.serverName = serverName;
            this.rootDtsFolder = rootDtsFolder;
            ssisApplication = new Application();
        }
        public void BreadthFirstCrawl()
        {
            PkgTablePairs = new Dictionary<string, List<string>>();
            BreadthFirstCrawl(rootDtsFolder);
        }
        public void BreadthFirstCrawl(string dtsPath)
        {
            PackageInfos sqlPackages;
            sqlPackages = ssisApplication.GetDtsServerPackageInfos(dtsPath, this.serverName);
            bool cont = true;
            if (sqlPackages.Count > 0 && cont)
            {
                Console.WriteLine(string.Format("\nPackages stored in {0}:", dtsPath));
                foreach (PackageInfo sqlPackage in sqlPackages)
                {
                    string ssisPath = Path.Combine(dtsPath, sqlPackage.Name);
                    if (!ssisApplication.FolderExistsOnDtsServer(ssisPath, this.serverName))
                    {
                        Console.WriteLine(string.Format("{0} is a package", sqlPackage.Name));
                        //if (sqlPackage.Name == "PopulateCommunityMart")
                        {
                            try
                            {
                                EtlPackage etl = new EtlPackage(ssisPath, this.serverName, null);
                                etl.getDestinationTables();
                                if (PkgTablePairs.Keys.Contains<string>(sqlPackage.Name))
                                {
                                    PkgTablePairs[sqlPackage.Name].AddRange(etl.DestinationTables);
                                }
                                else
                                {
                                    PkgTablePairs.Add(sqlPackage.Name, etl.DestinationTables);
                                }
                                //cont = false;
                                //break;
                            }
                            catch (Exception)
                            {
                                Console.WriteLine(string.Format("unable to load package."));
                            }
                            
                        }
                    }
                }
                foreach (PackageInfo sqlPackage in sqlPackages)
                {
                    string ssisPath = Path.Combine(dtsPath, sqlPackage.Name);
                    if (ssisApplication.FolderExistsOnDtsServer(ssisPath, this.serverName))
                    {
                        Console.WriteLine(string.Format("\t{0} is a folder", ssisPath));
                        BreadthFirstCrawl(ssisPath);
                    }
                }
                Console.WriteLine();
            }
        }
        public void SaveToSqlScript()
        {
            string pout = Path.Combine(CommonUtils.CommonUtils.cwd(),"DataLineage.sql");
            StreamWriter fout = new StreamWriter(pout);
            foreach (string key in PkgTablePairs.Keys)
            {
                foreach (string value in PkgTablePairs[key])
                {
                    string sql = string.Format("INSERT INTO tab VALUES ('{0}','{1}')", key, value);
                    fout.WriteLine(sql);
                }
            }
            fout.Close();
        }
    }
}
