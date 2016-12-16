﻿using System;
using System.Diagnostics;
using System.Text;

namespace EtlPackage
{
    
    public class Program
    {
        
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
            UnitTest.test_EtlPackageReaderFolder();

            Console.WriteLine($"\n\nexecution complete.  press any key to exit.");
            Console.ReadKey();
            return 0;
        }


    }
}
