
using System;
using System.IO;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.SqlServer.Management.Smo;
using Microsoft.SqlServer.Management.Sdk.Sfc;
using Microsoft.SqlServer.Dts.Runtime;
using Microsoft.SqlServer.TransactSql.ScriptDom;
using System.Diagnostics;
using CommonUtils;
namespace SqlServerUtilities
{
    //internal class SQLVisitor : TSqlFragmentVisitor
    //{
    //    private int SELECTcount = 0;
    //    private int INSERTcount = 0;
    //    private int UPDATEcount = 0;
    //    private int DELETEcount = 0;


    //    private string GetNodeTokenText(TSqlFragment fragment)
    //    {
    //        StringBuilder tokenText = new StringBuilder();
    //        for (int counter = fragment.FirstTokenIndex; counter <= fragment.LastTokenIndex; counter++)
    //        {
    //            tokenText.Append(fragment.ScriptTokenStream[counter].Text);
    //        }
    //        return tokenText.ToString();
    //    }

    //    // SELECTs 
    //    public override void ExplicitVisit(SelectStatement node)
    //    {
    //        Console.WriteLine("found SELECT statement with text: " + GetNodeTokenText(node));
    //        SELECTcount++;
    //    }
    //    // SELECTs 
    //    public override void ExplicitVisit(OutputIntoClause node)
    //    {
    //        Console.WriteLine("found OutputIntoClause statement with text: " + GetNodeTokenText(node));
    //        //SELECTcount++;
    //    }
    //    // INSERTs 
    //    public override void ExplicitVisit(InsertStatement node)
    //    {
    //        INSERTcount++;
    //    }
    //    // UPDATEs 
    //    public override void ExplicitVisit(UpdateStatement node)
    //    {
    //        UPDATEcount++;
    //    }
    //    // DELETEs 
    //    public override void ExplicitVisit(DeleteStatement node)
    //    {
    //        DELETEcount++;
    //    }
    //    public void DumpStatistics()
    //    {
    //        Console.WriteLine(string.Format("Found {0} SELECTs, {1} INSERTs, {2} UPDATEs & {3} DELETEs",
    //            this.SELECTcount,
    //            this.INSERTcount,
    //            this.UPDATEcount,
    //            this.DELETEcount));
    //    }
    //}
    //static void script_dom()
    //    {
    //        string scriptPath = Path.Combine(CommonUtils.CommonUtils.cwd(), "TestScript.sql");
    //        TextReader txtRdr = new StreamReader(scriptPath);
    //        TSql110Parser parser = new TSql110Parser(true);
    //        IList<ParseError> errors;
    //        TSqlFragment sqlFragment = parser.Parse(txtRdr, out errors);

    //        SQLVisitor myVisitor = new SQLVisitor();
    //        sqlFragment.Accept(myVisitor);


    //        myVisitor.DumpStatistics();

    //    }
    class Program
    {
        static void test_Etl_Database(string databaseName)
        {
            string src_server;
            string dst_server;
            EtlPackage pkg;
            dst_server = "STDBDECSUP01";
            src_server = "SPDBDECSUP04";
            pkg = new EtlPackage(string.Format("{0} {1}-{2}.dtsx", databaseName, src_server, dst_server));
            pkg.addDataFlowTasksBySchema(databaseName, "dbo", src_server, dst_server);
            pkg.addDataFlowTasksBySchema(databaseName, "Dim", src_server, dst_server);
            pkg.savePackage();
        }
        static void test_Etl_Table(string table_name)
        {
            string src_server_name = "STDBDECSUP03";
            string src_database_name = "CommunityMart";
            string src_schema_name = "dbo";
            string src_table_name = table_name;
            string dst_server_name = "STDBDECSUP01";
            string dst_database_name = "CommunityMart";
            string dst_schema_name = "dbo";
            string dst_table_name = table_name;
            EtlPackage pkg = new EtlPackage(string.Format("CommunityMart {0}-{1}.dtsx", src_server_name, dst_server_name));
            //packageName.addDataFlowTasksBySchema("CommunityMart", "dbo", src_server, dst_server);
            //packageName.addDataFlowTasksBySchema("CommunityMart", "Dim", src_server, dst_server);
            pkg.addDataFlowTask(src_server_name, src_database_name, src_schema_name, src_table_name, dst_server_name, dst_database_name, dst_schema_name, dst_table_name);
            //packageName.addSqlTask(dst_server_name, dst_database_name, "this is task name", string.Format("TRUNCATE TABLE {0}.{1}.{2}",dst_database_name, dst_schema_name, dst_table_name));
            //packageName.addTruncatePopulate(src_server_name, src_database_name, src_schema_name, src_table_name, dst_server_name, dst_database_name, dst_schema_name, dst_table_name);
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
        //static void test_script_extensions()
        //{
        //    Server server = SchemaReader.getServer("STDBDECSUP01");
        //    Database src_database = server.Databases["DSDW"];
        //    Console.WriteLine(string.Format("{0}", src_database.ToScript()));
        //string dst_database_name = "DSDW2";
        //Database dst_database;
        //if (!server.Databases.Contains(dst_database_name))
        //{
        //    dst_database = new Database(server, "DSDW2");

        //    dst_database.Create();
        //}
        //else
        //{
        //    dst_database = server.Databases[dst_database_name];
        //}

        //foreach (Table src_table in src_database.Tables)
        //{
        //    Console.WriteLine(src_table.Name);
        //    Table dst_table = new Table(dst_database, src_table.Schema, src_table.Name);
        //    foreach (Column src_column in src_table.Columns)
        //    {
        //        Column dst_column = new Column(dst_table, src_column.Name, src_column.DataType);
        //        dst_column.Nullable = src_column.Nullable;
        //        dst_column.Default = src_column.Default;
        //        dst_table.Columns.Add(dst_column);
        //    }
        //    dst_table.Create();
        //    break;
        //}
        //}
        static void test_EtlPackage_MSDB()
        {
            string pkgPath = @"MSDB\Emerg\Emerg_PHC\PHCMain";
            string server = "STDBDECSUP01";
            EtlPackage pkg = new EtlPackage(pkgPath, server, null);
            pkg.readExecutables();
            //pkg.logFile.Close();
        }
        static void test_EtlPackage_Reader()
        {
            string pkgPath = @"Z:\GITHUB\SqlServerUtilities\SqlServerUtilities\EtlTest\PopulateCommunityMart.dtsx";
            //string pkgPath = @"CommunityMart SPDBDECSUP04-STDBDECSUP01.dtsx";
            //pkgPath = Path.Combine(CommonUtils.CommonUtils.cwd(), pkgPath);
            EtlPackage pkg = new EtlPackage(pkgPath);
            pkg.readExecutables();
            //pkg.logFile.Close();
        }
        static void test_SchemaWriter()
        {
            Table tab = SchemaReader.getTable("STDBDECSUP01", "CommunityMart", "dbo", "ReferralFact");
            tab = SchemaReader.getTable("STDBDECSUP01", "AutoTest", "dbo", "TableProfile");
            string tab_sql = tab.ToScript();
            Console.WriteLine(string.Format("{0}", tab_sql));
            StreamWriter wrtr = new StreamWriter(CommonUtils.CommonUtils.cwd() + "/tabledef.sql");
            wrtr.Write(tab_sql);
            wrtr.Close();
        }
        public static void test_Depend()
        {
            Table tab = SchemaReader.getTable("STDBDECSUP01", "CommunityMart", "dbo", "ReferralFact");
            tab = SchemaReader.getTable("STDBDECSUP01", "AutoTest", "dbo", "TableProfile");
            DependencyWalker wkr = new DependencyWalker(SchemaReader.getServer("STDBDECSUP01"));
            DependencyTree tree = wkr.DiscoverDependencies(new[] { tab as SqlSmoObject }, DependencyType.Children);
            DependencyTreeNode node;// = tree;
            if (tree.HasChildNodes)
            {
                node = tree.FirstChild;
                node.Print();
                node.Urn.Print();
                node.GetType().Print();
                node.Urn.Type.Print();
                node.Urn.Value.Print();
                if (node.NumberOfSiblings > 0)
                {
                    node = tree.NextSibling;
                    node.Print();
                    node.Urn.Print();
                    node.GetType().Print();
                    node.Urn.Type.Print();
                    node.Urn.Value.Print();
                }
                    //node = node.NextSibling;
                    //node.Print();
                    //node.Urn.Print();
                    //node.GetType().Print();
                    //node.Urn.Type.Print();
                    //node.Urn.Value.Print();
                }
            //tree.HasChildNodes.Print();
            //Urn urn = tree.FirstChild.Urn;
            //urn.Print();

            //tree.Print();

        }
        static void browseMsdb()
        {
            string sqlFolder;
            string sqlServer;

            Application ssisApplication;
            PackageInfos sqlPackages;

            sqlServer = "STDBDECSUP01";

            ssisApplication = new Application();

            // Get packages stored in MSDB.    
            sqlFolder = "MSDB";
            sqlPackages = ssisApplication.GetDtsServerPackageInfos(sqlFolder, sqlServer);
            if (sqlPackages.Count > 0)
            {
                Console.WriteLine(string.Format("Packages stored in Sql Folder: {0}", sqlFolder));
                foreach (PackageInfo sqlPackage in sqlPackages)
                {
                    Console.WriteLine(sqlPackage.Name);
                    string folderPath = Path.Combine(sqlFolder, sqlPackage.Name);
                    if (ssisApplication.FolderExistsOnDtsServer(folderPath, sqlServer))
                    {
                        Console.WriteLine(string.Format("{0} is a folder", folderPath));
                    }
                }
                Console.WriteLine();
            }

            // Get packages stored in the File System.    
            sqlFolder = "File System";
            sqlPackages = ssisApplication.GetDtsServerPackageInfos(sqlFolder, sqlServer);
            if (sqlPackages.Count > 0)
            {
                Console.WriteLine("Packages stored in the File System:");
                foreach (PackageInfo sqlPackage in sqlPackages)
                {
                    Console.WriteLine(sqlPackage.Name);
                }
            }

            Console.Read();

        }
        static void test_MsdbReader()
        {
            string serverName = Environment.MachineName;
            Console.WriteLine("current MachineName: {0}", serverName);

            if (!serverName.Contains("DBDECSUP0"))
            {
                Console.WriteLine("current machine name does not contain \"DBDECSUP0\": defaulting to DEV Sql Server: STDBDECSUP01", serverName);
                serverName = "STDBDECSUP01";
            }
            string databaseName = "MSDB";
            MsdbReader rdr = new MsdbReader(serverName, databaseName);
            rdr.BreadthFirstCrawl();
            foreach (string key in rdr.PkgTablePairs.Keys)
            {
                Console.WriteLine(string.Format("Package Name: {0}\n\tTable Names:", key));
                foreach (var value in rdr.PkgTablePairs[key])
                {
                    Console.WriteLine(string.Format("\t{0}", value));
                }
            }
            //rdr.SaveToSqlScript();
            //rdr.InsertInto();
        }
        static void tableDdlDiff()
        {
            Table src_table = SchemaReader.getTable("STDBDECSUP01", "CommunityMart", "Dim", "Date");
            Table dst_table = SchemaReader.getTable("STDBDECSUP02", "CommunityMart", "Dim", "Date");
            Console.WriteLine(string.Format("{0}", src_table.CompareDefinition(dst_table)));
            //foreach (Column src_column in src_table.Columns)
            //{
            //    if (dst_table.Columns.Contains(src_column.Name))
            //    {
            //        if (dst_table.Columns[src_column.Name].DataType.ToString() == src_table.Columns[src_column.Name].DataType.ToString())
            //        {
            //            Console.WriteLine(string.Format("{2}-{3}\n\t{0} == {1}", src_table.ToString(), dst_table.ToString(), src_column.Name, dst_table.Columns[src_column.Name].DataType.ToString()));
            //        }
            //        else
            //        {
            //            Console.WriteLine(string.Format("{2}-{3}\n\t{0} != {1}", src_table.ToString(), dst_table.ToString(), src_column.Name, dst_table.Columns[src_column.Name].DataType.ToString()));
            //        }
            //    }
            //}
            //if (src_table.Columns.Equals(dst_table.Columns))
            //{
            //    Console.WriteLine(string.Format("{0} == {1}", src_table.ToString(), dst_table.ToString()));
            //}
            //else
            //{
            //    Console.WriteLine(string.Format("{0} != {1}", src_table.ToString(), dst_table.ToString()));
            //}
        }

        static void test_CopyObject()
        {
            CopyObject co = new CopyObject();
        }
        static void Main(string[] args)
        {
            try
            {
                Stopwatch stopWatch = new Stopwatch();
                stopWatch.Start();
                CommonUtils.CommonUtils.preExecutionSetup();

                //string databaseName = "CommunityMart";
                //test_Etl_Database(databaseName);
                //string table_name = "HoNOSFact";
                //test_Etl_Table(table_name);
                //test_scripting();
                //browseMsdb();
                test_CopyObject();
                //test_MsdbReader();
                //test_Etl_Table();
                //tableDdlDiff();
                //for (int i = 0; i < args.Count(); i++)
                //{
                //    Console.WriteLine(string.Format("args[{0}] = {1}", i, args[i]));
                //}

                        //test_Depend();
                        //test_EtlPackage_Reader();

                        stopWatch.Stop();
                // Get the elapsed time as a TimeSpan value.
                TimeSpan ts = stopWatch.Elapsed;
                // Format and display the TimeSpan value.
                string elapsedTime = String.Format("{0:00}:{1:00}:{2:00}.{3:00}",
                    ts.Hours, ts.Minutes, ts.Seconds,
                    ts.Milliseconds / 10);
                Console.WriteLine("RunTime " + elapsedTime);
                CommonUtils.CommonUtils.user_exit();
            }
            catch (Exception e)
            {
                Console.WriteLine("An exception ({0}) occurred.", e.GetType().Name);
                Console.WriteLine("Message:\n   {0}\n", e.Message);
                Console.WriteLine("Stack Trace:\n   {0}\n", e.StackTrace);
                Console.WriteLine("Unhandled Error");
                Console.WriteLine("Unhandled Error");
                Exception ie = e.InnerException;
                if (ie != null)
                {
                    Console.WriteLine("   The Inner Exception:");
                    Console.WriteLine("      Exception Name: {0}", ie.GetType().Name);
                    Console.WriteLine("      Message: {0}\n", ie.Message);
                    Console.WriteLine("      Stack Trace:\n   {0}\n", ie.StackTrace);
                }
                //throw;
            }
            finally
            {
                CommonUtils.CommonUtils.user_exit();
            }
        }
    }
}


