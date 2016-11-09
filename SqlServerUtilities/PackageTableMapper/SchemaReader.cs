using Microsoft.SqlServer.Management.Smo;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace PackageTableMapper
{
    public static class SchemaReader
    {
        public static Server GetServer(string serverName)
        {
            Server server = new Server(serverName);
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

    }
}
