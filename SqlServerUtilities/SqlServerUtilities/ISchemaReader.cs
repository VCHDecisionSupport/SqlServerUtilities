using Microsoft.SqlServer.Management.Smo;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DatabaseUtilities
{
    interface ISchemaReader
    {
        Server getServer(string server_name);
        Database getDatabase(string server_name, string database_name);
        Table getTable(string server_name, string database_name, string schema_name, string table_name);
    }
}
