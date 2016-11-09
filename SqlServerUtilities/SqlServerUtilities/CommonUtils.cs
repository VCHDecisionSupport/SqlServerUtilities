using System;
using System.Diagnostics;
using System.IO;
using System.Text;
using System.Text.RegularExpressions;

namespace CommonUtils
{
    public static class CommonUtils
    {
        public enum PostExecutionSteps
        {
            OpenCwd = 1,
            EscapeExit = 2,
            ExtendedExit = 3
        }
        public static PostExecutionSteps delay_exit(int seconds)
        {
            ConsoleKeyInfo k = new ConsoleKeyInfo();
            Console.WriteLine("\nExecution Complete\nPress any key in the next {0} seconds.", seconds);
            int nextMessage = 0;
            for (int cnt = seconds * 10; cnt > 0; cnt--)
            {
                if (Console.KeyAvailable == true)
                {
                    k = Console.ReadKey();
                    if (k.Key == ConsoleKey.Escape)
                        return PostExecutionSteps.EscapeExit;
                    else if (k.Key == ConsoleKey.O)
                    {
                        Process.Start(Directory.GetCurrentDirectory());
                        return PostExecutionSteps.OpenCwd;
                    }
                    cnt = 0;
                    delay_exit(seconds * 10);
                }
                else if (cnt / 10.0 >= nextMessage)
                {
                    //Console.WriteLine("{0} seconds", (cnt / 10).ToString());
                    System.Threading.Thread.Sleep(100);
                }
                else
                {
                    System.Threading.Thread.Sleep(100);
                }
            }
            Console.WriteLine("The key pressed was " + k.Key);
            return PostExecutionSteps.ExtendedExit;
        }
        public static void Message(String text, int tab)
        {
            String offset = new String('\t', tab);
            Console.WriteLine("{0}{1}", offset, text);
            //System.IO.StreamWriter file = new System.IO.StreamWriter(output_path, true);
            //file.WriteLine("{0}{1}", offset, text);
            //file.Close();
        }
        public static void Message(String text, int tab, System.IO.StreamWriter file)
        {
            String offset = new String('\t', tab);
            Console.WriteLine("{0}{1}", offset, text);
            file.WriteLine("{0}{1}", offset, text);
        }
        public static void Message(String text)
        {
            Message(text, 0);
        }
        public static void user_exit()
        {
            Console.WriteLine("\r\n\r\nexecution complete\r\npress enter key to exit");
            Console.ReadKey();
            //while (Console.ReadKey().Key != ConsoleKey.Escape && Console.ReadKey().Key != ConsoleKey.Enter) { }
        }
        public static String Cwd()
        {
            string path = Directory.GetCurrentDirectory();
            Console.WriteLine("The current directory is {0}", path);
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
        public static void save_to_file(String outputPath, String content)
        {
            System.IO.StreamWriter file = new System.IO.StreamWriter(outputPath, false);
            file.WriteLine(content);
            file.Close();
        }
        public static void save_to_file(String outputPath, StringBuilder content)
        {
            System.IO.StreamWriter file = new System.IO.StreamWriter(outputPath, false);
            file.WriteLine(content);
            file.Close();
        }
        public static void PreExecutionSetup()
        {
            Console.BufferHeight = 9999;
            Console.WindowHeight = (int)(Console.LargestWindowHeight * .5);
            Console.BufferWidth = Console.BufferWidth< Console.LargestWindowHeight ? (int)(Console.LargestWindowWidth*.75) : Console.BufferWidth;
            Console.WindowWidth = (int)(Console.LargestWindowWidth * .75);
            string bitexe;
            if (Environment.Is64BitProcess)
            {
                bitexe = "64 bit";
            }
            else
            {
                bitexe = "32 bit";
            }
            TextWriterTraceListener myWriter = new TextWriterTraceListener(System.Console.Out);
            Debug.Listeners.Add(myWriter);
            Console.WriteLine(string.Format("Current Machine: {0}\nCurrent UserName: {1}\nRunning as {2} executable", Environment.MachineName, Environment.UserName, bitexe));
        }
        public static void Print(this string input, int tabCount, ref StreamWriter wrtr)
        {
            string tab = new string('\t', tabCount);
            Console.WriteLine(string.Format("{0}{1}", tab,input));
            wrtr.WriteLine(string.Format("{0}{1}", tab,input));
        }
        public static void Print(this string input, int tabCount)
        {
            string tab = new string('\t', tabCount);
            Console.WriteLine(string.Format("{0}{1}", tab, input));
        }
        public static void Print(this string input)
        {
            input.Print(0);
        }
        public static void Print(this object obj)
        {
            System.Type t = obj.GetType();
            string.Format("({0}){1}", t.ToString(), obj.ToString()).Print();
        }
        public static string ExtractDatabaseName(string connectionString)
        {
            Regex regex = new Regex(@"Initial Catalog[ ]*=[ ]*(?<database>[\w\..]+);");
            MatchCollection matches = regex.Matches(connectionString);
            //Console.WriteLine("{0} matches found in:\n   {1}",
            //             matches.Count,
            //             connectionString);

            // Report on each match.
            foreach (Match match in matches)
            {
                GroupCollection groups = match.Groups;
                //Console.WriteLine("'{0}' repeated at positions {1} and {2}",
                //                  groups["database"].Value,
                //                  groups[0].Index,
                //                  groups[1].Index);
                return groups["database"].Value;
            }
            return null;
        }
        public static string GetEtlConnectionString(string serverName, string databaseName)
        {
            string confmt = "Data Source={0};" +
                  "Initial Catalog={1};Provider=SQLOLEDB.1;" +
                  "Integrated Security=SSPI;";
            string connectionString = string.Format(confmt, serverName, databaseName);
            return connectionString;
        }
        public static string GetSqlConnectionString(string serverName, string databaseName)
        {
            string confmt = "Data Source={0};" +
                  "Initial Catalog={1};" +
                  "Integrated Security=SSPI;";
            string connectionString = string.Format(confmt, serverName, databaseName);
            return connectionString;
        }
    }
}
