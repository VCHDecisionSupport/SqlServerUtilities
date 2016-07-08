
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
        /// <summary>
        /// dst_database, dst_server given.
        /// objects will be created on server and database
        /// </summary>
        /// <param name="server"></param>
        /// <param name="database"></param>
        public SchemaWriter(Server server, Database database)
        {
            dst_server = server;
            dst_database = database;
        }
        public SchemaWriter(string server_name, string database_name)
        {
            dst_server = new Server(server_name);
            dst_database = new Database(dst_server, database_name);
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
        public string CreateAll()
        {
            string script = "";
            script += dst_database.ToScript();
            return script;
        }
        public Schema AddSchema(Schema src_schema)
        {
             Schema dst_schema;
            if (!dst_database.Schemas.Contains(src_schema.Name))
            {
                dst_schema = new Schema(dst_database, src_schema.Name);
                dst_schema.Create();
            }
            else
            {
                dst_schema = dst_database.Schemas[src_schema.Name];
            }
            return dst_schema;
        }
        public Schema AddSchema(string src_schema_name)
        {
            Schema dst_schema;
            if (!dst_database.Schemas.Contains(src_schema_name))
            {
                dst_schema = new Schema(dst_database, src_schema_name);
                //dst_schema.Create();
            }
            else
            {
                dst_schema = dst_database.Schemas[src_schema_name];
            }
            return dst_schema;
        }
        public Table AddTable(Table src_table)
        {
            AddSchema(src_table.Schema);
            Table dst_table = new Table(dst_database, src_table.Name, src_table.Schema);
            foreach (Column src_column in src_table.Columns)
            {
                Column dst_column = new Column(dst_table, src_column.Name, src_column.DataType);
                dst_column.Nullable = src_column.Nullable;
                dst_column.Identity = src_column.Identity;
                dst_column.Default = src_column.Default;
                if (src_column.Identity)
                {
                    dst_column.IdentitySeed = src_column.IdentitySeed;
                    dst_column.IdentityIncrement = src_column.IdentityIncrement;
                }
                dst_table.Columns.Add(dst_column);
                //dst_table.Create();
            }
            if (dst_database.Tables.Contains(dst_table.Name, dst_table.Schema))
            {
                Console.WriteLine("CREATE added it to a collection that had already be created");
            }
            Console.WriteLine(dst_table.ToScript());
            return dst_table;
        }
    }
}
