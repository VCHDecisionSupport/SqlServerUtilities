using Microsoft.SqlServer.Management.Common;
using Microsoft.SqlServer.Management.Smo;
using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SqlServerUtilities
{
    public static class SchemaReader
    {
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
                if(table == null)
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
