﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;


namespace PackageTableMapper
{
    class Program
    {
        static void Parse_MsdbEtl(int TestTypeID)
        {
            string serverName = Environment.MachineName;
            Console.WriteLine("current MachineName: {0}", serverName);

            if (!serverName.Contains("DBDECSUP0"))
            {
                Console.WriteLine("current machine name does not contain \"DBDECSUP0\": defaulting to DEV Sql Server: STDBDECSUP01", serverName);
                serverName = "STDBDECSUP01";
            }
            string databaseName = "MSDB";
            MsdbReader rdr = new MsdbReader(serverName, databaseName, TestTypeID);
            rdr.BreadthFirstCrawl();
            foreach (string key in rdr.PkgTablePairs.Keys)
            {
                Console.WriteLine(string.Format("Package Name: {0}\n\tTable Names:", key));
                foreach (var value in rdr.PkgTablePairs[key])
                {
                    Console.WriteLine(string.Format("\t{0}", value));
                }
            }
            rdr.SaveToSqlScript();
            rdr.InsertInto();
        }
        /// <summary>
        /// PackageTableMapper: C# console app populates all columns in the DQMF.dbo.ETL_PackageTable table.  PkgID is looked up from the DQMF.dbo.ETL_Package table.  PackageTableMapper walks Sql Server MSDB folders; reads the deployed SSIS ETL package files(*.dtsx) into memory using Microsoft.SqlServer.Dts C# .NET namespaces.  The destination table name from each Data Flow Task is extracted; it's ObjectID is looked up from DQMF.dbo.MD_Object.
        /// </summary>
        static void Main(string[] args)
        {
            Console.WriteLine(string.Format("USAGE example:\nPackageTableMapper TestTypeID=#\nwhere # is 0, 1, 2, or 3"));
            int TestTypeID = 3;
            if (args.Count() == 0)
            {
                Console.WriteLine(string.Format("TestTypeID not specified.  Default is {0}.", TestTypeID));
                Parse_MsdbEtl(TestTypeID);
            }
            else
            {
                Console.WriteLine(string.Format("given argument: '{0}'", args[0]));
                TestTypeID = int.Parse(args[0].Split('=')[1].Trim());
                Parse_MsdbEtl(TestTypeID);
            }
        }
    }
}
