using System;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Text.RegularExpressions;
using Microsoft.SqlServer.Management.Common;
using Microsoft.SqlServer.Management.Sdk.Sfc;
using Microsoft.SqlServer.Management.Smo;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using Microsoft.SqlServer.Dts.Runtime;
using System.Data;

namespace EtlPackage
{
    public static class SqlUtilities
    {
        public static string CleanSsisTaskName(string taskName)
        {
            return Regex.Replace(taskName, @"[\[\]\\\.]", "", RegexOptions.None, TimeSpan.FromSeconds(1.5));
        }

        public static string ExtractDatabaseName(string connectionString)
        {
            Regex regex = new Regex(@"Initial Catalog[ ]*=[ ]*(?<databaseName>[\w\..]+);");
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
                return groups["databaseName"].Value;
            }
            return null;
        }
        public static string ExtractServerName(string connectionString)
        {
            Regex regex = new Regex(@"Data Source[ ]*=[ ]*(?<serverName>[\w\..]+);");
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
                return groups["serverName"].Value;
            }
            return null;
        }
        public static string ExtractServerQualifiedObjectName(Urn urn)
        {
            Regex regex = new Regex(@"Server\[@Name='(?<ServerName>\w+)'\]/Database\[@Name='(?<DatabaseName>\w+)'\]/(?<ObjectTypeName>\w+)\[@Name='(?<ObjectName>\w+)' and @Schema='(?<ObjectSchemaName>\w+)'\].*");
            string qualifiedObjectName = "";
            MatchCollection matches = regex.Matches(urn);
            foreach (Match match in matches)
            {
                GroupCollection groups = match.Groups;

                Debug.WriteLine(groups["ServerName"].Value);
                Debug.WriteLine(groups["DatabaseName"].Value);
                Debug.WriteLine(groups["ObjectTypeName"].Value);
                Debug.WriteLine(groups["ObjectName"].Value);
                Debug.WriteLine(groups["ObjectSchemaName"].Value);
                qualifiedObjectName =
                    $"{groups["ServerName"].Value}.{groups["DatabaseName"].Value}.{groups["ObjectSchemaName"].Value}.{groups["ObjectName"].Value}";
            }
            return qualifiedObjectName;
        }
        public static string ExtractDatabaseQualifiedObjectName(Urn urn)
        {
            Regex regex = new Regex(@"Server\[@Name='(?<ServerName>\w+)'\]/Database\[@Name='(?<DatabaseName>\w+)'\]/(?<ObjectTypeName>\w+)\[@Name='(?<ObjectName>\w+)' and @Schema='(?<ObjectSchemaName>\w+)'\].*");
            string qualifiedObjectName = "";
            MatchCollection matches = regex.Matches(urn);
            foreach (Match match in matches)
            {
                GroupCollection groups = match.Groups;

                Debug.WriteLine(groups["ServerName"].Value);
                Debug.WriteLine(groups["DatabaseName"].Value);
                Debug.WriteLine(groups["ObjectTypeName"].Value);
                Debug.WriteLine(groups["ObjectName"].Value);
                Debug.WriteLine(groups["ObjectSchemaName"].Value);
                qualifiedObjectName =
                    $"{groups["DatabaseName"].Value}.{groups["ObjectSchemaName"].Value}.{groups["ObjectName"].Value}";
            }
            return qualifiedObjectName;
        }
        public static string ExtractSchemaQualifiedObjectName(Urn urn)
        {
            Regex regex = new Regex(@"Server\[@Name='(?<ServerName>\w+)'\]/Database\[@Name='(?<DatabaseName>\w+)'\]/(?<ObjectTypeName>\w+)\[@Name='(?<ObjectName>\w+)' and @Schema='(?<ObjectSchemaName>\w+)'\].*");
            string qualifiedObjectName = "";
            MatchCollection matches = regex.Matches(urn);
            foreach (Match match in matches)
            {
                GroupCollection groups = match.Groups;

                Debug.WriteLine(groups["ServerName"].Value);
                Debug.WriteLine(groups["DatabaseName"].Value);
                Debug.WriteLine(groups["ObjectTypeName"].Value);
                Debug.WriteLine(groups["ObjectName"].Value);
                Debug.WriteLine(groups["ObjectSchemaName"].Value);
                qualifiedObjectName =
                    $"{groups["ObjectSchemaName"].Value}.{groups["ObjectName"].Value}";
            }
            return qualifiedObjectName;
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
            string connectionString = String.Format(confmt, serverName, databaseName);
            return connectionString;
        }
        public static SqlConnection GetSqlConnection(string server)
        {
            try
            {
                SqlConnectionStringBuilder builder = new SqlConnectionStringBuilder();
                builder.DataSource = server;
                builder.IntegratedSecurity = true;
                SqlConnection sqlConnection = new SqlConnection(builder.ConnectionString);
                sqlConnection.Open();
                ServerConnection serverConnection = new ServerConnection(sqlConnection);
                return sqlConnection;
            }
            catch (Exception e)
            {
                Debug.WriteLine($"ERROR: unable to connection server: {server}");
                Environment.Exit(0);
                return null;
            }
        }
        public static Server GetServer(string serverName)
        {
            SqlConnectionStringBuilder builder = new SqlConnectionStringBuilder();
            builder.DataSource = serverName;
            builder.IntegratedSecurity = true;
            SqlConnection sqlConnection = new SqlConnection(builder.ConnectionString);
            sqlConnection.Open();
            ServerConnection serverConnection = new ServerConnection(sqlConnection);
            Server server = new Server(serverConnection);
            if (server == null)
            {
                throw new NullReferenceException();
            }
            return server;
        }
        public static Database GetDatabase(string serverName, string databaseName)
        {
            Server server = GetServer(serverName);
            Database database = server.Databases[databaseName];
            if (database == null)
            {
                throw new NullReferenceException();
            }
            return database;
        }

        public static Table GetTable(string serverName, string databaseName, string schemaName, string tableName)
        {
            Database database = GetDatabase(serverName, databaseName);
            Table table = database.Tables[tableName, schemaName];
            if (table == null)
            {
                throw new NullReferenceException();
            }
            return table;
        }
        public static View GetView(string serverName, string databaseName, string schemaName, string viewName)
        {
            Database database = GetDatabase(serverName, databaseName);
            View view = database.Views[viewName, schemaName];
            if (view == null)
            {
                throw new NullReferenceException();
            }
            return view;
        }

        public static ColumnCollection GetColumns(string serverName, string databaseName, string schemaName, string objectName)
        {
            Database database = GetDatabase(serverName, databaseName);
            View view = database.Views[objectName, schemaName];
            if (view == null)
            {
                Table table = database.Tables[objectName, schemaName];
                if (table == null)
                {
                    throw new NullReferenceException();
                }
                else
                {
                    return table.Columns;
                }
            }
            else
            {
                return view.Columns;
            }
        }
        // recursive search of package files in/under rootPath; add to ref pathDictionary
        public static void AddPackageFilePaths(string rootPath, ref Dictionary<string, string> pathDictionary)
        {
            // add packages in rootPath
            if (Directory.Exists(rootPath) && Directory.GetFiles(rootPath, "*.dtsx").Length > 0)
            {
                foreach (var entry in Directory.GetFiles(rootPath, "*.dtsx"))
                {
                    pathDictionary.Add(Path.GetFileName(Path.GetFileNameWithoutExtension(entry)), entry);
                }
            }
            // recurse into child folders
            if (Directory.GetDirectories(rootPath).Length > 0)
            {
                foreach (var entry in Directory.EnumerateFileSystemEntries(rootPath))
                {
                    AddPackageFilePaths(entry, ref pathDictionary);
                }
            }
        }
        // recursive search of package files in/under rootPath
        public static Dictionary<string, string> GetPackageFilePaths(string rootPath)
        {
            if (rootPath == null)
            {
                rootPath = Utilities.Cwd();
            }
            Dictionary<string, string> pathDictionary = new Dictionary<string, string>();
            // add packages at rootPath
            if (File.Exists(rootPath))
            {
                if (Path.GetExtension(rootPath) == ".dtsx")
                {
                    Application app = new Application();
                    pathDictionary.Add(Path.GetFileNameWithoutExtension(rootPath), rootPath);
                }
            }
            // add packages in rootPath
            else if (Directory.Exists(rootPath) && Directory.GetFiles(rootPath, "*.dtsx").Length > 0)
            {
                foreach (var entry in Directory.GetFiles(rootPath, "*.dtsx"))
                {
                    pathDictionary.Add(Path.GetFileName(Path.GetFileNameWithoutExtension(entry)), entry);
                }
            }
            // recurse into child folders
            if (Directory.GetDirectories(rootPath).Length > 0)
            {
                foreach (var entry in Directory.EnumerateFileSystemEntries(rootPath))
                {
                    AddPackageFilePaths(entry, ref pathDictionary);
                }
            }
            return pathDictionary;
        }

        // recursive search of MSDB for all packages in/under rootPath
        public static Dictionary<string, string> GetPackageMsdbPaths(string serverName, string rootPath)
        {
            if (rootPath == null)
            {
                rootPath = "";
            }
            // query msdb sys tables...
            string ssis_package_query = @"
WITH msdb_folders AS
(
	SELECT rootpkg.parentfolderid, 
		rootpkg.folderid, 
		rootpkg.foldername, 
		0 AS level, 
		CAST('' AS varchar(500)) as full_path 
	FROM msdb.dbo.sysssispackagefolders AS rootpkg 
	WHERE parentfolderid IS NULL
	UNION ALL
	SELECT 
		childpkg.parentfolderid, 
		childpkg.folderid, 
		childpkg.foldername, 
		recurse.level + 1 as level, 
		CAST(recurse.full_path + '\' + childpkg.foldername AS varchar(500)) AS full_path 
	FROM msdb.dbo.sysssispackagefolders AS childpkg 
	JOIN msdb_folders as recurse
	ON childpkg.ParentFolderID = recurse.folderid
	WHERE childpkg.parentfolderid IS NOT NULL
)
SELECT 
	msdb_folders.full_path AS PackageFolderPath
	,msdb_folders.full_path + '\' + pkgs.name AS PackageFullPath
    ,pkgs.name AS PackageName
FROM msdb_folders
JOIN msdb.dbo.sysssispackages as pkgs
ON msdb_folders.folderid = pkgs.folderid
GO
";
            Application app = new Application();
            PackageInfos pkgs = app.GetPackageInfos("\\", serverName, null, null);
            foreach (var pkg in pkgs)
            {
                Debug.WriteLine($"{pkg.Name} in {pkg.Folder}");
            }
            SqlConnection sqlconn = SqlUtilities.GetSqlConnection(serverName);
            SqlCommand cmd = sqlconn.CreateCommand();
            cmd.CommandText = ssis_package_query;
            SqlDataReader rdr = cmd.ExecuteReader();
            Dictionary<string, string> pathDictionary = new Dictionary<string, string>();
            foreach (IDataRecord item in rdr)
            {
                Debug.WriteLine(item.GetValue(1));
                if ((item.GetValue(0) as string).StartsWith(rootPath))
                {
                    pathDictionary.Add(item.GetValue(2) as string, item.GetValue(1) as string);
                }
                
            }
            return pathDictionary;
        }
    }
}
