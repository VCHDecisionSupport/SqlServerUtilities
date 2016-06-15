
using Microsoft.SqlServer.Management.Smo;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SqlServerUtilities
{
    public class SchemaWriter
    {
        public Database dst_database { get; private set; }
        public Server dst_server { get; private set; }

        public SchemaWriter(Server server, Database database)
        {
            dst_server = server;
            dst_database = database;
        }
        public SchemaWriter(Database database)
        {
            dst_server = database.Parent;
            dst_database = database;
        }
        public SchemaWriter(Server server, string database_name)
        {
            dst_server = server;
            dst_database = new Database(dst_server, database_name);
        }
        public Table AddTable(Table src_table)
        {
            Table dst_table = new Table(dst_database, src_table.Name);
            foreach (Column src_column in src_table.Columns)
            {
                Column dst_co
            }
            dst_database.Tables.Add
        }
    }
}
