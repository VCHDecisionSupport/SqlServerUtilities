using Microsoft.SqlServer.Management.Smo;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SqlServerUtilities
{
    public static class SchemaReader
    {
        public static Server getServer(string server_name)
        {
            Server server = new Server(server_name);
            if (server == null)
            {
                throw new NullReferenceException();
            }
            return server;
        }
        public static Database getDatabase(string server_name, string database_name)
        {
            Server server = getServer(server_name);
            Database database = server.Databases[database_name];
            if (database == null)
            {
                throw new NullReferenceException();
            }
            return database;
        }
        public static Table getTable(string server_name, string database_name, string schema_name, string table_name)
        {
            Database database = getDatabase(server_name, database_name);
            Table table = database.Tables[table_name, schema_name];
            if (table == null)
            {
                throw new NullReferenceException();
            }
            return table;
        }

    }
}
