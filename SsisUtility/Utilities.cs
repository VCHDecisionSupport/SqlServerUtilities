using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SsisUtility
{
    public static class Utilities
    {
        public static void CrawlRepos()
        {
            var pkg_dir_path = @"C:\Users\user\Dropbox\Vault";
            CrawlRepos(pkg_dir_path);
        }
        public static void CrawlRepos(string pkg_dir_path)
        {
            Debug.WriteLine($"crawling repo path: {pkg_dir_path}...");
            var _repo_root = new DirectoryInfo(pkg_dir_path);
            var pkg_file_infos = _repo_root.GetFiles("*.dtsx");
            foreach (var pkg_file_info in pkg_file_infos)
            {
                var etl_rdr = new EtlPackageReader(pkg_file_info.FullName);
                etl_rdr.ProcessPackage();
            }
            var sub_dirs = _repo_root.GetDirectories();
            foreach (var sub_dir in sub_dirs)
            {
                CrawlRepos(sub_dir.FullName);
            }
            Debug.WriteLine($"repo path crawl complete({pkg_dir_path})");

        }
        public static String Cwd()
        {
            string path = Directory.GetCurrentDirectory();
            Debug.Print("The current directory is {0}", path);
            return path;
        }
        public static String Cwd(String newDir)
        {
            if (!Directory.Exists(newDir))
            {
                Directory.CreateDirectory(newDir);
            }
            Directory.SetCurrentDirectory(newDir);
            return Cwd();
        }
        public static void user_exit()
        {
            Console.WriteLine("\r\n\r\nexecution complete\r\npress enter key to exit");
            Console.ReadKey();
            //while (Console.ReadKey().Key != ConsoleKey.Escape && Console.ReadKey().Key != ConsoleKey.Enter) { }
        }

        public static void ToFile(this string fileContents, string filePath)
        {
            StreamWriter streamWriter = new StreamWriter(filePath);
            streamWriter.Write(fileContents);
            streamWriter.Close();
        }
    }
}
