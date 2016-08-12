using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.SqlServer.Management.Smo;

namespace SqlServerUtilities
{
    public class SameTable : IEqualityComparer<Table>
    {

        public bool Equals(Table x, Table y)
        {
            throw new NotImplementedException();
        }

        public int GetHashCode(Table obj)
        {
            throw new NotImplementedException();
        }
    }
    public class SameColumn : IEqualityComparer<Column>
    {

        public bool Equals(Column x, Column y)
        {
            //Console.WriteLine(string.Format("{0}", x.DataType.SqlDataType));
            return true;
            //throw new NotImplementedException();
        }

        public int GetHashCode(Column obj)
        {
            return obj.GetHashCode();
            //throw new NotImplementedException();
        }
    }
    public static class DiffExtensions
    {

        public static bool CompareDefinition(this Table src_table, Table dst_table)
        {
            bool isSame = true;
            Column[] src_col_arr = new Column[src_table.Columns.Count];
            src_table.Columns.CopyTo(src_col_arr, 0);

            Column[] dst_col_arr = new Column[dst_table.Columns.Count];
            dst_table.Columns.CopyTo(dst_col_arr, 0);

            var x = from src_col in src_col_arr
                    join dst_col in dst_col_arr
                    on new {src_col.DataType.SqlDataType, src_col.Name} equals new {dst_col.DataType.SqlDataType,
                    dst_col.Name}
                    select src_col;

            return  x.Count() == src_col_arr.Count() && x.Count() == dst_col_arr.Count() ? true : false;

        }
    }
}
