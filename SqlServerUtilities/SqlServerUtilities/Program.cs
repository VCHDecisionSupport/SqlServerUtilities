
using System;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.SqlServer.Management.Smo;
using Microsoft.SqlServer.Management.Sdk.Sfc;  

namespace SqlServerUtilities
{
    class Program
    {
        static void ssis_foo()
        {
            EtlPackage pkg = new EtlPackage();
            //pkg.getConnection("localhost", "CommunityMart");
            //pkg.getConnection(@"localhost\PROD", "DSDW");
            //pkg.addDataFlowTask("STDBDECSUP02", "DSDW", "Community", "YouthClinicActivityFact", "STDBDECSUP01", "DSDW", "Community", "YouthClinicActivityFact");
            pkg.addDataFlowTasksBySchema("CommunityMart", "dbo", @"STDBDECSUP02", @"STDBDECSUP01");
            pkg.addDataFlowTasksBySchema("CommunityMart", "Dim", @"STDBDECSUP02", @"STDBDECSUP01");
            pkg.savePackage();
        }
        static void createEtl()
        {
            string src_server;
            string dst_server;
            EtlPackage pkg;

            dst_server = "STDBDECSUP01";
            src_server = "STDBDECSUP03";
            pkg = new EtlPackage(string.Format("CommunityMart {0}-{1}.dtsx", src_server, dst_server));
            pkg.addDataFlowTasksBySchema("CommunityMart", "dbo", src_server, dst_server);
            pkg.addDataFlowTasksBySchema("CommunityMart", "Dim", src_server, dst_server);
            pkg.savePackage();

            pkg = new EtlPackage(string.Format("DSDW {0}-{1}.dtsx", src_server, dst_server));
            pkg.addDataFlowTasksBySchema("DSDW", "Community", src_server, dst_server);
            pkg.addDataFlowTasksBySchema("DSDW", "Dim", src_server, dst_server);
            pkg.savePackage();
            
            
            dst_server = "STDBDECSUP01";
            src_server = "STDBDECSUP02";
            pkg = new EtlPackage(string.Format("CommunityMart {0}-{1}.dtsx", src_server, dst_server));
            pkg.addDataFlowTasksBySchema("CommunityMart", "dbo", src_server, dst_server);
            pkg.addDataFlowTasksBySchema("CommunityMart", "Dim", src_server, dst_server);
            pkg.savePackage();
            
            pkg = new EtlPackage(string.Format("DSDW {0}-{1}.dtsx", src_server, dst_server));
            pkg.addDataFlowTasksBySchema("DSDW", "Community", src_server, dst_server);
            pkg.addDataFlowTasksBySchema("DSDW", "Dim", src_server, dst_server);
            pkg.savePackage();
        }
        static void test_etl()
        {
            string src_server_name = "STDBDECSUP03";
            string src_database_name = "CommunityMart";
            string src_schema_name = "Dim";
            string src_table_name = "LocalReportingOffice";
            string dst_server_name = "STDBDECSUP01";
            string dst_database_name = "CommunityMart";
            string dst_schema_name = "Dim";
            string dst_table_name = "LocalReportingOffice";
            EtlPackage pkg = new EtlPackage(string.Format("CommunityMart {0}-{1}.dtsx", src_server_name, dst_server_name));
            //pkg.addDataFlowTasksBySchema("CommunityMart", "dbo", src_server, dst_server);
            //pkg.addDataFlowTasksBySchema("CommunityMart", "Dim", src_server, dst_server);
            //pkg.addDataFlowTask(src_server_name, src_database_name, src_schema_name, src_table_name, dst_server_name, dst_database_name, dst_schema_name, dst_table_name);
            //pkg.addSqlTask(dst_server_name, dst_database_name, "this is task name", string.Format("TRUNCATE TABLE {0}.{1}.{2}",dst_database_name, dst_schema_name, dst_table_name));
            //pkg.addTruncatePopulate(src_server_name, src_database_name, src_schema_name, src_table_name, dst_server_name, dst_database_name, dst_schema_name, dst_table_name);
            pkg.savePackage();
        }
        private static void test_scripting()
        {
            ScriptingOptions opts = new ScriptingOptions();
            //scripter.Options.DriAll = true;
            opts.ScriptDrops = false;// script drops of objects that depend on this
            opts.WithDependencies = true;
            opts.ClusteredIndexes = true;
            opts.IncludeIfNotExists = true;
            opts.Indexes = true;// To include indexes  
            opts.NoCollation = true; // stupid collation for character columns
            opts.NonClusteredIndexes = true;   // to include referential constraints in the script 
            opts.ScriptSchema = true;
            opts.NoFileGroup = true;

            Server server = SchemaReader.getServer("STDBDECSUP01");
            Database database = server.Databases["DSDW"];
            //Table view = database_name.Tables["ReferralReason", "Dim"];
            Scripter scripter = new Scripter(server);
            scripter.Options = opts;
            //foreach (Table view in database_name.Tables)
            //{
            //    StringCollection str_col = scripter.Script(new Urn[] { view.Urn });
            //    foreach (string sql in str_col)
            //    {
            //        Console.WriteLine(sql);
            //    }
            //    break;
            //}

            //foreach (StoredProcedure proc in database_name.StoredProcedures)
            //{
            //    StringCollection str_col = scripter.Script(new Urn[] { proc.Urn });
            //    foreach (string sql in str_col)
            //    {
            //        Console.WriteLine(sql);
            //    }
            //    break;
            //}

            foreach (var obj in database.Schemas)
            {
                Console.WriteLine(obj.GetType().Name);
                
                SqlSmoObject smo_obj = obj as SqlSmoObject;
                Schema sch = smo_obj as Schema;
                break;
                //StringCollection str_col = scripter.Script(database_name.Schemas.GetEnumerator().Current);
                //foreach (string sql in str_col)
                //{
                //    Console.WriteLine(sql);
                //}
                break;
            }
            foreach (var obj in database.Tables)
            {
                Console.WriteLine(obj.GetType().Name);
                SqlSmoObject smo_obj = obj as SqlSmoObject;
                StringCollection str_col = scripter.Script(new Urn[] { smo_obj.Urn });
                foreach (string sql in str_col)
                {
                    Console.WriteLine(sql);
                }
                break;
            }

            foreach (var obj in database.Views)
            {
                Console.WriteLine(obj.GetType().Name);
                SqlSmoObject smo_obj = obj as SqlSmoObject;
                StringCollection str_col = scripter.Script(new Urn[] { smo_obj.Urn });
                foreach (string sql in str_col)
                {
                    Console.WriteLine(sql);
                }
                break;
            }
            foreach (var obj in database.UserDefinedFunctions)
            {
                Console.WriteLine(obj.GetType().Name);
                SqlSmoObject smo_obj = obj as SqlSmoObject;
                StringCollection str_col = scripter.Script(new Urn[] { smo_obj.Urn });
                foreach (string sql in str_col)
                {
                    Console.WriteLine(sql);
                }
                break;
            }
            foreach (var obj in database.StoredProcedures)
            {
                Console.WriteLine(obj.GetType().Name);
                SqlSmoObject smo_obj = obj as SqlSmoObject;
                StringCollection str_col = scripter.Script(new Urn[] { smo_obj.Urn });
                foreach (string sql in str_col)
                {
                    Console.WriteLine(sql);
                }
                break;
            }

        }
        static void test_script_extensions()
        {
            Server server = SchemaReader.getServer("localhost");
            Database src_database = server.Databases["DSDW"];
            string dst_database_name = "DSDW2";
            Database dst_database;
            if (!server.Databases.Contains(dst_database_name))
            {
                dst_database = new Database(server, "DSDW2");

                dst_database.Create();
            }
            else
            {
                dst_database = server.Databases[dst_database_name];
            }

            foreach (Table src_table in src_database.Tables)
            {
                Console.WriteLine(src_table.Name);
                Table dst_table = new Table(dst_database, src_table.Schema, src_table.Name);
                foreach (Column src_column in src_table.Columns)
                {
                    Column dst_column = new Column(dst_table, src_column.Name, src_column.DataType);
                    dst_column.Nullable = src_column.Nullable;
                    dst_column.Default = src_column.Default;
                    dst_table.Columns.Add(dst_column);
                }
                dst_table.Create();
                break;
            }
        }
        static void test_SchemaWriter()
        {
            SchemaWriter wrtr = new SchemaWriter("STDBDECSUP01", "ShellDSDW");
            Table src_table = SchemaReader.getTable("STDBDECSUP01", "DSDW", "Dim", "Date");
            wrtr.AddTable(src_table);
            string ddl= wrtr.CreateAll();
            Console.WriteLine(ddl);
        }
        static void Main(string[] args)
        {
            CommonUtils.CommonUtils.preExecutionSetup();
            //ssis_foo();
            //createEtl();
            //createEtlFromExcel();
            //test_etl();
            //test_scripting();
            //test_script_extensions();
            test_SchemaWriter();
            CommonUtils.CommonUtils.user_exit();
        }

        
    }
}


