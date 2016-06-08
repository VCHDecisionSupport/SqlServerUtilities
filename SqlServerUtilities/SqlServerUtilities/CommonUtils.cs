using System;
using System.Diagnostics;
using System.IO;
using System.Text;

namespace CommonUtils
{
    class CommonUtils
    {
        public enum PostExecutionSteps
        {
            OPEN_CWD = 1,
            ESCAPE_EXIT = 2,
            EXTENDED_EXIT = 3
        }
        public static PostExecutionSteps delay_exit(int seconds)
        {
            ConsoleKeyInfo k = new ConsoleKeyInfo();
            Console.WriteLine("\nExecution Complete\nPress any key in the next {0} seconds.", seconds);
            int next_message = 0;
            for (int cnt = seconds * 10; cnt > 0; cnt--)
            {
                if (Console.KeyAvailable == true)
                {
                    k = Console.ReadKey();
                    if (k.Key == ConsoleKey.Escape)
                        return PostExecutionSteps.ESCAPE_EXIT;
                    else if (k.Key == ConsoleKey.O)
                    {
                        Process.Start(Directory.GetCurrentDirectory());
                        return PostExecutionSteps.OPEN_CWD;
                    }
                    cnt = 0;
                    delay_exit(seconds * 10);
                }
                else if (cnt / 10.0 >= next_message)
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
            return PostExecutionSteps.EXTENDED_EXIT;
        }
        public static void message(String text, int tab)
        {
            String offset = new String('\t', tab);
            Console.WriteLine("{0}{1}", offset, text);
            //System.IO.StreamWriter file = new System.IO.StreamWriter(output_path, true);
            //file.WriteLine("{0}{1}", offset, text);
            //file.Close();
        }
        public static void message(String text, int tab, System.IO.StreamWriter file)
        {
            String offset = new String('\t', tab);
            Console.WriteLine("{0}{1}", offset, text);
            file.WriteLine("{0}{1}", offset, text);
        }
        public static void message(String text)
        {
            message(text, 0);
        }
        public static void user_exit()
        {
            Console.WriteLine("\r\n\r\nexecution complete\r\npress enter key to exit");
            while (Console.ReadKey().Key != ConsoleKey.Escape && Console.ReadKey().Key != ConsoleKey.Enter) { }
        }
        public static String cwd()
        {
            string path = Directory.GetCurrentDirectory();
            Console.WriteLine("The current directory is {0}", path);
            return path;
        }
        public static String cwd(String new_dir)
        {
            if (!Directory.Exists(new_dir))
            {
                Directory.CreateDirectory(new_dir);
            }
            Directory.SetCurrentDirectory(new_dir);
            return cwd();
        }
        public static void save_to_file(String output_path, String content)
        {
            System.IO.StreamWriter file = new System.IO.StreamWriter(output_path, false);
            file.WriteLine(content);
            file.Close();
        }
        public static void save_to_file(String output_path, StringBuilder content)
        {
            System.IO.StreamWriter file = new System.IO.StreamWriter(output_path, false);
            file.WriteLine(content);
            file.Close();
        }
        public static void preExecutionSetup()
        {
            Console.BufferHeight = 9999;
            Console.WindowHeight = (int)(Console.LargestWindowHeight * .5);
            Console.BufferWidth = (int)(Console.LargestWindowWidth*.75);
            Console.WindowWidth = (int)(Console.LargestWindowWidth * .75);
        }
    }
}
