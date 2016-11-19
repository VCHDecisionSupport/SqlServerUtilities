using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace EtlPackage
{
    public static class Utilities
    {
        public static void StartUp()
        {

        }
        public static void ShutDown()
        {

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
