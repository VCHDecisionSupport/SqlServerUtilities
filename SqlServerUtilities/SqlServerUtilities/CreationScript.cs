using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.SqlServer.Management.Smo;

namespace SqlServerUtilities
{
    public class CreationScript
    {
        public static string getCreate(Server server, int object_id)
        {

            Scripter x = new Scripter(server);
            x.Options.DriAll = true;
            //Table table = SchemaReader.getTable("
            return "";
        }
    }
}
