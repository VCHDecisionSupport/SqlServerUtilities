using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SsisUtility
{
    // inserts rows into AutoTest.Map.PackageTable by called stored proc set in AutoTestMappingStoredProcedure property
    public class PackageTableSqlInserter
    {
        public int NestedLevel { get; set; }
        public string PackageName { get; private set; }
        private SqlConnection destSqlConnection { get; set; }
        public string DestinationMappingTableFqName { get; set; } = "";
        // this stored procedure must exist on Sql Server instance
        public string AutoTestMappingStoredProcedure { get; set; } = "AutoTest.dbo.uspInsMapPackageTable";
        public PackageTableSqlInserter(SqlConnection destSqlConnection, EtlPackageReader etlPackageReader)
        {
            Console.WriteLine($"\t\tPackageTableSqlInserter({destSqlConnection.ConnectionString}, EtlPackageReader)");
            NestedLevel = 1;
            etlPackageReader.RaiseDataFlowEvent += HandleDataFlowEvent;
            this.destSqlConnection = destSqlConnection;
            this.PackageName = etlPackageReader.PackageName;
            //this.destSqlConnection.Open();
        }
        public PackageTableSqlInserter(SqlConnection destSqlConnection)
        {
            Console.WriteLine($"\t\tPackageTableSqlInserter({destSqlConnection.ConnectionString})");
            NestedLevel = 1;
            //etlPackageReader.RaiseDataFlowEvent += HandleDataFlowEvent;
            this.destSqlConnection = destSqlConnection;
            //this.PackageName = etlPackageReader.PackageName;
            //this.destSqlConnection.Open();
        }
        public void SetEtlPackageReader(EtlPackageReader etlPackageReader)
        {
            etlPackageReader.RaiseDataFlowEvent += HandleDataFlowEvent;
            if (etlPackageReader.PackageName == null)
            {
                throw new Exception($"unknown package name");
            }
            this.PackageName = etlPackageReader.PackageName;
        }
        // Define what actions to take when the event is raised.
        void HandleDataFlowEvent(object sender, DataFlowEventArgs dataFlowEventArgs)
        {
            SqlCommand sqlCmd = destSqlConnection.CreateCommand();
            sqlCmd.CommandType = CommandType.StoredProcedure;
            sqlCmd.CommandText = AutoTestMappingStoredProcedure;
            try
            {
                
                SqlParameter parameterPackageName = new SqlParameter("@pPackageName", SqlDbType.VarChar);
                SqlParameter parameterDatabaseName = new SqlParameter("@pDatabaseName", SqlDbType.VarChar);
                SqlParameter parameterSchemaName = new SqlParameter("@pSchemaName", SqlDbType.VarChar);
                SqlParameter parameterTableName = new SqlParameter("@pTableName", SqlDbType.VarChar);
                    
                parameterPackageName.SqlValue = PackageName;
                parameterDatabaseName.SqlValue = dataFlowEventArgs.DestinationDatabaseName;
                parameterSchemaName.SqlValue = dataFlowEventArgs.DestinationTableName.Split('.')[0];
                parameterTableName.SqlValue = dataFlowEventArgs.DestinationTableName.Split('.')[1];

                sqlCmd.Parameters.Add(parameterPackageName);
                sqlCmd.Parameters.Add(parameterDatabaseName);
                sqlCmd.Parameters.Add(parameterSchemaName);
                sqlCmd.Parameters.Add(parameterTableName);
                sqlCmd.ExecuteNonQuery();
            }
            catch (SqlException e)
            {
                Exception exception = new Exception($"unable to insert mapping: {PackageName} -> {dataFlowEventArgs.DestinationTableName} into table: {DestinationMappingTableFqName}\n{e.GetType()}\n{sqlCmd.CommandText}\n\n{sqlCmd.Connection.ConnectionString}\n\n", e);
                Debug.WriteLine($"{exception.Message}");
                Debug.WriteLine($"{e.Message}");
                throw exception;


            }
            catch(Exception e)
            {
                Debug.WriteLine("ERROR");
            }
        }

    }
}
