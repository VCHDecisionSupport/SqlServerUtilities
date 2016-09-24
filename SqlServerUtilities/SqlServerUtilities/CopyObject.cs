using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.SqlServer.Management.Smo;


namespace SqlServerUtilities
{
    public class CopyObject
    {
        string dst_name { get; set; }
        Table src_table { get; set; }
        Database dst_db { get; set; }


        public CopyObject()
        {
            string sqlServer = "STDBDECSUP02";

            dst_db = SchemaReader.getDatabase(sqlServer, "DSDW");
            DestinationObjectExists();
        }

        public bool DestinationObjectExists()
        {
            foreach(var item in dst_db.EnumObjects(DatabaseObjectTypes.Table, SortOrder.Schema).Rows)
            {
                Console.WriteLine(item.ToString());
            }
            return false;
        }
    }
}
