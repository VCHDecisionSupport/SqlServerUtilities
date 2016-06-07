
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using DocumentFormat.OpenXml.Packaging;
using DocumentFormat.OpenXml.Spreadsheet;
using DatabaseUtilities;

namespace DatabaseUtilities
{
    class Program
    {
        static void ssis_foo()
        {
            EtlPackage pkg = new EtlPackage();
            //pkg.getConnection("localhost", "CommunityMart");
            //pkg.getConnection(@"localhost\PROD", "DSDW");
            //pkg.addDataFlowTask("STDBDECSUP02", "DSDW", "Community", "YouthClinicActivityFact", "STDBDECSUP01", "DSDW", "Community", "YouthClinicActivityFact");
            pkg.addDataFlowTasksBySchema("CommunityMart", "dbo", @"STDBDECSUP02", @"STDBDECSUP01");
            pkg.addDataFlowTasksBySchema("CommunityMart", "Dim", @"STDBDECSUP02", @"STDBDECSUP01");
            pkg.savePackage();
        }
       static void excel_foo()
        {
            const string fileName = @"data_copy_params.xlsx";
            //string A1 = ExcelHandler.GetCellValue(fileName, "Sheet1", "A1");
            //Console.WriteLine(A1);
            ExcelHandler.InsertText(fileName, "Sheet1");

            //Console.WriteLine(ExcelHandler.ParseAddressName("GH43"));
            //ExcelHandler.InsertText(fileName, "F5", "hi there");
        }
        static void createEtl()
        {
            string src_server = "STDBDECSUP03";
            string dst_server = "STDBDECSUP01";
            EtlPackage pkg = new EtlPackage(string.Format("CommunityMart {0}-{1}.dtsx", src_server, dst_server));
            pkg.addDataFlowTasksBySchema("CommunityMart", "dbo", src_server,dst_server);
            pkg.addDataFlowTasksBySchema("CommunityMart", "Dim", src_server,dst_server);
            pkg.savePackage();
            pkg = new EtlPackage(string.Format("DSDW {0}-{1}.dtsx",src_server,dst_server));
            pkg.addDataFlowTasksBySchema("DSDW", "dbo", src_server, dst_server);
            pkg.addDataFlowTasksBySchema("DSDW", "Dim", src_server, dst_server);
            pkg.savePackage();
        }
        static void createEtlFromExcel()
        {
            const string excel_file_name = @"data_copy_params.xlsx";
            EtlPackage pkg = new EtlPackage("autoEtl_FromExcel" + DateTime.Today.ToString("YYYY-MM-DD") + ".dtsx");
            pkg.addDataFlowTasksFromExcel(excel_file_name);
        }
        static void Main(string[] args)
        {
            //ssis_foo();
            //createEtl();
            //createEtlFromExcel();
            CommonUtils.CommonUtils.user_exit();
        }
    }
}


