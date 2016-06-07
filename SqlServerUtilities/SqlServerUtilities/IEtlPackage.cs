using Microsoft.SqlServer.Dts.Runtime;
using Microsoft.SqlServer.Management.Smo;

namespace DatabaseUtilities
{
    public interface IEtlPackage
    {
        bool IsSaved { get; set; }
        string addDataFlowTask(Table src_table, Table dst_table);
        string addDataFlowTask(Executables execs, Table src_table, Table dst_table);
        string addDataFlowTask(string src_server_name, string src_database_name, string src_schema_name, string src_table_name, string dst_server_name, string dst_database_name, string dst_schema_name, string dst_table_name);
        string addDataFlowTasksBySchema(string database_name, string schema_name, string src_server_name, string dst_server_name);
        void addDataFlowTasksFromExcel(string excel_path);
        string cleanSsisTaskName(string task_name);
        string getConnection(Table table);
        string getConnection(string server, string database);
        string getPackageName();
        string savePackage();
        string savePackage(string pkg_path);
    }
}