
using Microsoft.SqlServer.Management.Smo;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SqlServerUtilities
{
    public class SchemaTemplateWriter
    {
        public Database DstDatabase { get; private set; }
        public Server DstServer { get; private set; }
        /// <summary>
        /// dst_database, dst_server given.
        /// objects will be created on server and database
        /// </summary>
        /// <param name="server"></param>
        /// <param name="database"></param>
        public SchemaTemplateWriter(Server server, Database database)
        {
            DstServer = server;
            DstDatabase = database;
        }
        public SchemaTemplateWriter(string serverName, string databaseName)
        {
            DstServer = new Server(serverName);
            DstDatabase = new Database(DstServer, databaseName);
        }
        public SchemaTemplateWriter(Database database)
        {
            DstServer = database.Parent;
            DstDatabase = database;
        }
        public SchemaTemplateWriter(Server server, string databaseName)
        {
            DstServer = server;
            DstDatabase = new Database(DstServer, databaseName);
        }
        //public string CreateAll()
        //{
        //    string script = "";
        //    script += dst_database.ToScript();
        //    return script;
        //}
        public Schema AddSchema(Schema srcSchema)
        {
             Schema dstSchema;
            if (!DstDatabase.Schemas.Contains(srcSchema.Name))
            {
                dstSchema = new Schema(DstDatabase, srcSchema.Name);
                dstSchema.Create();
            }
            else
            {
                dstSchema = DstDatabase.Schemas[srcSchema.Name];
            }
            return dstSchema;
        }
        public Schema AddSchema(string srcSchemaName)
        {
            Schema dstSchema;
            if (!DstDatabase.Schemas.Contains(srcSchemaName))
            {
                dstSchema = new Schema(DstDatabase, srcSchemaName);
                //dst_schema.Create();
            }
            else
            {
                dstSchema = DstDatabase.Schemas[srcSchemaName];
            }
            return dstSchema;
        }
        public Table AddTable(Table srcTable)
        {
            AddSchema(srcTable.Schema);
            Table dstTable = new Table(DstDatabase, srcTable.Name, srcTable.Schema);
            foreach (Column srcColumn in srcTable.Columns)
            {
                Column dstColumn = new Column(dstTable, srcColumn.Name, srcColumn.DataType);
                dstColumn.Nullable = srcColumn.Nullable;
                dstColumn.Identity = srcColumn.Identity;
                dstColumn.Default = srcColumn.Default;
                if (srcColumn.Identity)
                {
                    dstColumn.IdentitySeed = srcColumn.IdentitySeed;
                    dstColumn.IdentityIncrement = srcColumn.IdentityIncrement;
                }
                dstTable.Columns.Add(dstColumn);
                //dst_table.Create();
            }
            if (DstDatabase.Tables.Contains(dstTable.Name, dstTable.Schema))
            {
                Console.WriteLine("CREATE added it to a collection that had already be created");
            }
            Console.WriteLine(dstTable.ToScript());
            return dstTable;
        }
    }
}
