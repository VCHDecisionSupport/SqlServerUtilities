using System;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Text.RegularExpressions;
using Microsoft.SqlServer.Management.Common;
using Microsoft.SqlServer.Management.Sdk.Sfc;
using Microsoft.SqlServer.Management.Smo;

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
            SqlConnectionStringBuilder builder = new SqlConnectionStringBuilder();
            builder.DataSource = server;
            builder.IntegratedSecurity = true;
            SqlConnection sqlConnection = new SqlConnection(builder.ConnectionString);
            sqlConnection.Open();
            ServerConnection serverConnection = new ServerConnection(sqlConnection);
            return sqlConnection;
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
    }
}
