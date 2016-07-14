using Microsoft.SqlServer.Management.Smo;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using CommonUtils;

namespace SqlServerUtilities
{
    public class ScriptWriter
    {
        public static string GetIfExists(Database database, string schema_object_name, string type)
        {
            switch (type)
            {
                case "U":
                    string.Format("")
                    "Table".Print();
                    break;
                case "V":
                    "View".Print();
                    break;
                default:
                    break;
            }

            return "";

        }
    }
}
