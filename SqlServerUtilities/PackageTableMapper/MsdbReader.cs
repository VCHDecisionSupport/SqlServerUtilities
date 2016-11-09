using Microsoft.SqlServer.Dts.Runtime;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.SqlServer.Management.Smo;
using System.Data.SqlClient;
using System.Data;

namespace PackageTableMapper
{
    public class MsdbReader
    {
        string ServerName { get; set; }
        string RootDtsFolder { get; set; }
        string Outfile { get; set; }
        Application SsisApplication { get; set; }
        public Dictionary<string, List<Tuple<string, string>>> PkgTablePairs { get; set; }
        public Dictionary<string, List<Tuple<string, string>>> TempPkgTablePairs { get; set; }
        int TestTypeId { get; set; }
        public MsdbReader()
        {
            this.ServerName = "STDBDECSUP01";
            this.RootDtsFolder = "MSDB";
            this.Outfile = Path.Combine(CommonUtils.CommonUtils.Cwd(), string.Format("PkgTableMapping_{0}_{1}.sql", ServerName, RootDtsFolder));
            SsisApplication = new Application();

        }
        public MsdbReader(string serverName)
        {
            this.ServerName = serverName;
            this.RootDtsFolder = "MSDB";
            this.Outfile = Path.Combine(CommonUtils.CommonUtils.Cwd(), string.Format("PkgTableMapping_{0}_{1}.sql", serverName, RootDtsFolder));
            SsisApplication = new Application();
        }
        public MsdbReader(string serverName, string rootDtsFolder)
        {
            this.ServerName = serverName;
            this.RootDtsFolder = rootDtsFolder;
            this.Outfile = Path.Combine(CommonUtils.CommonUtils.Cwd(), string.Format("PkgTableMapping_{0}_{1}.sql", serverName, rootDtsFolder));
            SsisApplication = new Application();
        }
        public MsdbReader(string serverName, string rootDtsFolder, int testTypeId)
        {
            this.ServerName = serverName;
            this.RootDtsFolder = rootDtsFolder;
            this.TestTypeId = testTypeId;
            this.Outfile = Path.Combine(CommonUtils.CommonUtils.Cwd(), string.Format("PkgTableMapping_{0}_{1}.sql", serverName, rootDtsFolder));
            SsisApplication = new Application();
        }
        public void BreadthFirstCrawl()
        {
            PkgTablePairs = new Dictionary<string, List<Tuple<string, string>>>();
            TempPkgTablePairs = new Dictionary<string, List<Tuple<string, string>>>();
            BreadthFirstCrawl(RootDtsFolder);
        }
        public void BreadthFirstCrawl(string dtsPath)
        {
            PackageInfos sqlPackages;
            sqlPackages = SsisApplication.GetDtsServerPackageInfos(dtsPath, this.ServerName);
            bool cont = true;
            if (sqlPackages.Count > 0 && cont)
            {
                Console.WriteLine(string.Format("\nPackages stored in {0}:", dtsPath));
                foreach (PackageInfo sqlPackage in sqlPackages)
                {
                    string ssisPath = Path.Combine(dtsPath, sqlPackage.Name);
                    if (!SsisApplication.FolderExistsOnDtsServer(ssisPath, this.ServerName))
                    {
                        Console.WriteLine(string.Format("{0} is a package", sqlPackage.Name));
                        //if (sqlPackage.Name == "PopulateCommunityMart")
                        {
                            try
                            {
                                EtlPackage etl = new EtlPackage(ssisPath, this.ServerName, null);
                                etl.GetDestinationTables();
                                Console.WriteLine(string.Format("{0} tables mapped.",etl.DestinationTables.Count));
                                if (PkgTablePairs.Keys.Contains<string>(sqlPackage.Name))
                                {
                                    PkgTablePairs[sqlPackage.Name].AddRange(etl.DestinationTables);
                                    TempPkgTablePairs[sqlPackage.Name].AddRange(etl.DestinationTables);

                                }
                                else
                                {
                                    PkgTablePairs.Add(sqlPackage.Name, etl.DestinationTables);
                                    TempPkgTablePairs.Add(sqlPackage.Name, etl.DestinationTables);
                                    //cont = false;
                                    //break;
                                }
                                
                                InsertInto(TempPkgTablePairs);
                                TempPkgTablePairs = new Dictionary<string, List<Tuple<string, string>>>();
                            }
                            catch (Exception)
                            {
                                Console.WriteLine(string.Format("Exception: unable to map package to table and/or populate DQMF.dbo.ETL_PackageObject."));
                            }

                        }
                        //cont = false;
                        if (!cont)
                        {
                            break;
                        }
                    }
                }
                foreach (PackageInfo sqlPackage in sqlPackages)
                {
                    string ssisPath = Path.Combine(dtsPath, sqlPackage.Name);
                    if (SsisApplication.FolderExistsOnDtsServer(ssisPath, this.ServerName) && cont)
                    {
                        Console.WriteLine(string.Format("\t{0} is a folder", ssisPath));
                        BreadthFirstCrawl(ssisPath);
                    }
                    if (!cont)
                    {
                        break;
                    }
                }
                Console.WriteLine();
            }
        }

        public void SaveToSqlScript()
        {
            SaveToSqlScript("tab");
        }
        public void SaveToSqlScript(string destinationTableName)
        {
            StreamWriter fout = new StreamWriter(this.Outfile);
            foreach (string key in PkgTablePairs.Keys)
            {
                foreach (var databaseTable in PkgTablePairs[key])
                {
                    string tableName = databaseTable.Item2;
                    string databaseName = databaseTable.Item1;
                    tableName = tableName.Replace("[", "");
                    tableName = tableName.Replace("]", "");
                    string sql = string.Format(
"\n\n;WITH pkg AS ( \n" +
"	SELECT PkgID \n" +
"	FROM DQMF.dbo.ETL_Package AS pkg \n" +
"	WHERE 1=1 \n" +
"	AND pkg.PkgName = '{0}' \n" +
") \n" +
", obj AS ( \n" +
"	SELECT ObjectID \n" +
"	FROM DQMF.dbo.MD_Object AS obj \n" +
"   JOIN DQMF.dbo.MD_Database AS db \n" +
"	ON obj.DatabaseId = db.DatabaseId \n" +
"	WHERE 1=1 \n" +
"	AND obj.ObjectPhysicalName = PARSENAME('{1}',1) \n" +
"	AND obj.ObjectSchemaName = PARSENAME('{1}',2) \n" +
"	AND db.DatabaseName = '{2}' \n" +
") \n" +
", map AS ( \n" +
"	SELECT pkg.PkgID, obj.ObjectID \n" +
"	FROM pkg  \n" +
"	CROSS JOIN obj \n" +
") \n" +
"MERGE INTO DQMF.dbo.ETL_PackageObject AS dst \n" +
"USING map  \n" +
"ON ( \n" +
"map.PkgID = dst.PackageID \n" +
"AND map.ObjectID = dst.ObjectID) \n" +
"WHEN MATCHED THEN  \n" +
"UPDATE SET  \n" +
"dst.PackageID = map.PkgID \n" +
"WHEN NOT MATCHED THEN \n" +
"INSERT VALUEs (map.PkgID, map.objectID, {3});", key, tableName, databaseName, TestTypeId);
                    fout.WriteLine(sql);
                }
            }
            fout.Close();
        }
        public void InsertInto(Dictionary<string, List<Tuple<string, string>>> pkgTablePairs)
        {
            if (pkgTablePairs.Count == 0)
            {
                if (this.PkgTablePairs.Count == 0)
                {
                    return;
                }
                pkgTablePairs = this.PkgTablePairs;
            }
            string connectionString = CommonUtils.CommonUtils.GetSqlConnectionString(this.ServerName, "DQMF");
            Console.WriteLine(string.Format("connecting to DQMF"));
            System.Data.SqlClient.SqlConnection sqlConnection1 = new System.Data.SqlClient.SqlConnection(connectionString);

            System.Data.SqlClient.SqlCommand cmd = new System.Data.SqlClient.SqlCommand();
            cmd.CommandType = System.Data.CommandType.Text;
            cmd.CommandText = string.Format("INSERT INTO DQMF.dbo.ETL_PackageObject (PackageName, ObjectName) VALUES (@PackageName, @ObjectName)");
            cmd.CommandText = string.Format(
";WITH pkg AS ( \n" +
"	SELECT PkgID \n" +
"	FROM DQMF.dbo.ETL_Package AS pkg \n" +
"	WHERE 1=1 \n" +
"	AND pkg.PkgName = @packageName \n" +
") \n" +
", obj AS ( \n" +
"	SELECT ObjectID \n" +
"	FROM DQMF.dbo.MD_Object AS obj \n" +
"   JOIN DQMF.dbo.MD_Database AS db \n" +
"	ON obj.DatabaseId = db.DatabaseId \n" +
"	WHERE 1=1 \n" +
"	AND obj.ObjectPhysicalName = PARSENAME(@objectName,1) \n" +
"	AND obj.ObjectSchemaName = PARSENAME(@objectName,2) \n" +
"	AND db.DatabaseName = @databaseName \n" +
") \n" +
", map AS ( \n" +
"	SELECT pkg.PkgID, obj.ObjectID \n" +
"	FROM pkg  \n" +
"	CROSS JOIN obj \n" +
") \n" +
"MERGE INTO DQMF.dbo.ETL_PackageObject AS dst \n" +
"USING map  \n" +
"ON ( \n" +
"map.PkgID = dst.PackageID \n" +
"AND map.ObjectID = dst.ObjectID) \n" +
"WHEN MATCHED THEN  \n" +
"UPDATE SET  \n" +
"dst.PackageID = map.PkgID \n" +
"WHEN NOT MATCHED THEN \n" +
"INSERT VALUES (map.PkgID, map.objectID, {0});", this.TestTypeId);
            Console.WriteLine(string.Format("{0}", cmd.CommandText));
            SqlParameter PackageName = new SqlParameter("@packageName", SqlDbType.VarChar, 200);
            cmd.Parameters.Add(PackageName);
            SqlParameter ObjectName = new SqlParameter("@objectName", SqlDbType.VarChar, 200);
            cmd.Parameters.Add(ObjectName);
            SqlParameter DatabaseName = new SqlParameter("@databaseName", SqlDbType.VarChar, 200);
            cmd.Parameters.Add(DatabaseName);

            cmd.Connection = sqlConnection1;
            sqlConnection1.Open();

            foreach (string packageName in PkgTablePairs.Keys)
            {
                foreach (var databaseTable in PkgTablePairs[packageName])
                {
                    string objectName = databaseTable.Item2;
                    string databaseName = databaseTable.Item1;
                    if (objectName != null && objectName.Length > 0 && databaseName != null && databaseName.Length > 0)
                    {
                        objectName = objectName.Replace("[", "");
                        objectName = objectName.Replace("]", "");
                        PackageName.Value = packageName;
                        ObjectName.Value = objectName;
                        DatabaseName.Value = databaseName;
                        Console.WriteLine(string.Format("PackageName: {0}\nDatabase Name: {1}\nTable Name: {2}", packageName, databaseName, objectName));
                        cmd.ExecuteNonQuery();
                    }
                }
            }
            sqlConnection1.Close();
        }
        public void InsertInto()
        {
            InsertInto(this.PkgTablePairs);
        }
    }
}
