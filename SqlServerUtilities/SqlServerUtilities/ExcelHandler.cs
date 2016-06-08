// https://msdn.microsoft.com/en-us/library/office/cc861607.aspx
// https://www.microsoft.com/en-us/download/confirmation.aspx?id=23734
using DocumentFormat.OpenXml;
using DocumentFormat.OpenXml.Packaging;
using DocumentFormat.OpenXml.Spreadsheet;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

using System.Data;
using System.Data.OleDb;


namespace SqlServerUtilities
{
    using ExcelCellAddress = Tuple<string, uint>;
    class ExcelHandler
    {
        private static Regex excelAddressRegex = new Regex(@"(?<column_index>[A-Za-z]+)(?<row_index>\d+)");

        public static ExcelCellAddress ParseAddressName(string addressName)
        {
            Match regex_match = excelAddressRegex.Match(addressName);
            ExcelCellAddress address = new ExcelCellAddress(regex_match.Groups["column_index"].Value, Convert.ToUInt32(regex_match.Groups["row_index"].Value));
            Console.WriteLine(string.Format("{0}{1}", address.Item1, address.Item2));
            return address;
        }
        // Given a document name and text, 
        // inserts a new work sheet and writes the text to cell "A1" of the new worksheet.
        public static void InsertText(string docName, string text)
        {
            // Open the document for editing.
            using (SpreadsheetDocument spreadSheet = SpreadsheetDocument.Open(docName, true))
            {
                // Get the SharedStringTablePart. If it does not exist, create a new one.
                SharedStringTablePart shareStringPart;
                if (spreadSheet.WorkbookPart.GetPartsOfType<SharedStringTablePart>().Count() > 0)
                {
                    shareStringPart = spreadSheet.WorkbookPart.GetPartsOfType<SharedStringTablePart>().First();
                }
                else
                {
                    shareStringPart = spreadSheet.WorkbookPart.AddNewPart<SharedStringTablePart>();
                }

                // Insert the text into the SharedStringTablePart.
                int index = InsertSharedStringItem(text, shareStringPart);

                // Insert a new worksheet.
                WorksheetPart worksheetPart = InsertWorksheet(spreadSheet.WorkbookPart);

                // Insert cell A1 into the new worksheet.
                Cell cell = InsertCellInWorksheet("A", 1, worksheetPart);

                // Set the value of cell A1.
                cell.CellValue = new CellValue(index.ToString());
                cell.DataType = new EnumValue<CellValues>(CellValues.SharedString);

                // Save the new worksheet.
                worksheetPart.Worksheet.Save();
            }
        }
        public static void InsertListOfText(string excel_path, List<Tuple<string, string>> entries)
        {
            // Open the document for editing.
            using (SpreadsheetDocument spreadSheet = SpreadsheetDocument.Open(excel_path, true))
            {
                // Get the SharedStringTablePart. If it does not exist, create a new one.
                SharedStringTablePart shareStringPart;
                if (spreadSheet.WorkbookPart.GetPartsOfType<SharedStringTablePart>().Count() > 0)
                {
                    shareStringPart = spreadSheet.WorkbookPart.GetPartsOfType<SharedStringTablePart>().First();
                }
                else
                {
                    shareStringPart = spreadSheet.WorkbookPart.AddNewPart<SharedStringTablePart>();
                }
                WorksheetPart worksheetPart = spreadSheet.WorkbookPart.WorksheetParts.First();
                foreach (Tuple<string, string> entry in entries)
                {
                    // Insert the text into the SharedStringTablePart.
                    int index = InsertSharedStringItem(entry.Item2, shareStringPart);
                    ExcelCellAddress address = ExcelHandler.ParseAddressName(entry.Item1);
                    // Insert cell A1 into the new worksheet.
                    Cell cell = InsertCellInWorksheet(address.Item1, address.Item2, worksheetPart);

                    // Set the value of cell A1.
                    cell.CellValue = new CellValue(index.ToString());
                    cell.DataType = new EnumValue<CellValues>(CellValues.SharedString);
                }





                // Save the new worksheet.
                worksheetPart.Worksheet.Save();
            }
        }

        // Given text and a SharedStringTablePart, creates a SharedStringItem with the specified text 
        // and inserts it into the SharedStringTablePart. If the item already exists, returns its index.
        private static int InsertSharedStringItem(string text, SharedStringTablePart shareStringPart)
        {
            // If the part does not contain a SharedStringTable, create one.
            if (shareStringPart.SharedStringTable == null)
            {
                shareStringPart.SharedStringTable = new SharedStringTable();
            }

            int i = 0;

            // Iterate through all the items in the SharedStringTable. If the text already exists, return its index.
            foreach (SharedStringItem item in shareStringPart.SharedStringTable.Elements<SharedStringItem>())
            {
                if (item.InnerText == text)
                {
                    return i;
                }

                i++;
            }

            // The text does not exist in the part. Create the SharedStringItem and return its index.
            shareStringPart.SharedStringTable.AppendChild(new SharedStringItem(new DocumentFormat.OpenXml.Spreadsheet.Text(text)));
            shareStringPart.SharedStringTable.Save();

            return i;
        }

        // Given a WorkbookPart, inserts a new worksheet.
        private static WorksheetPart InsertWorksheet(WorkbookPart workbookPart)
        {
            // Add a new worksheet part to the workbook.
            WorksheetPart newWorksheetPart = workbookPart.AddNewPart<WorksheetPart>();
            newWorksheetPart.Worksheet = new Worksheet(new SheetData());
            newWorksheetPart.Worksheet.Save();

            Sheets sheets = workbookPart.Workbook.GetFirstChild<Sheets>();
            string relationshipId = workbookPart.GetIdOfPart(newWorksheetPart);

            // Get a unique ID for the new sheet.
            uint sheetId = 1;
            if (sheets.Elements<Sheet>().Count() > 0)
            {
                sheetId = sheets.Elements<Sheet>().Select(s => s.SheetId.Value).Max() + 1;
            }

            string sheetName = "Sheet" + sheetId;

            // Append the new worksheet and associate it with the workbook.
            Sheet sheet = new Sheet() { Id = relationshipId, SheetId = sheetId, Name = sheetName };
            sheets.Append(sheet);
            workbookPart.Workbook.Save();

            return newWorksheetPart;
        }

        // Given a column name, a row index, and a WorksheetPart, inserts a cell into the worksheet. 
        // If the cell already exists, returns it. 
        private static Cell InsertCellInWorksheet(string columnName, uint rowIndex, WorksheetPart worksheetPart)
        {
            Worksheet worksheet = worksheetPart.Worksheet;
            SheetData sheetData = worksheet.GetFirstChild<SheetData>();
            string cellReference = columnName + rowIndex;

            // If the worksheet does not contain a row with the specified row index, insert one.
            Row row;
            if (sheetData.Elements<Row>().Where(r => r.RowIndex == rowIndex).Count() != 0)
            {
                row = sheetData.Elements<Row>().Where(r => r.RowIndex == rowIndex).First();
            }
            else
            {
                row = new Row() { RowIndex = rowIndex };
                sheetData.Append(row);
            }

            // If there is not a cell with the specified column name, insert one.  
            if (row.Elements<Cell>().Where(c => c.CellReference.Value == columnName + rowIndex).Count() > 0)
            {
                return row.Elements<Cell>().Where(c => c.CellReference.Value == cellReference).First();
            }
            else
            {
                // Cells must be in sequential order according to CellReference. Determine where to insert the new cell.
                Cell refCell = null;
                foreach (Cell cell in row.Elements<Cell>())
                {
                    if (string.Compare(cell.CellReference.Value, cellReference, true) > 0)
                    {
                        refCell = cell;
                        break;
                    }
                }

                Cell newCell = new Cell() { CellReference = cellReference };
                row.InsertBefore(newCell, refCell);

                worksheet.Save();
                return newCell;
            }
        }
        public static string GetCellValue(string fileName, string sheetName, string addressName)
        {
            string value = null;

            // Open the spreadsheet document for read-only access.
            using (SpreadsheetDocument document = SpreadsheetDocument.Open(fileName, false))
            {
                // Retrieve a reference to the workbook part.
                WorkbookPart wbPart = document.WorkbookPart;

                // Find the sheet with the supplied name, and then use that 
                // Sheet object to retrieve a reference to the first worksheet.
                Sheet theSheet = wbPart.Workbook.Descendants<Sheet>().
                  Where(s => s.Name == sheetName).FirstOrDefault();

                // Throw an exception if there is no sheet.
                if (theSheet == null)
                {
                    throw new ArgumentException("sheetName");
                }

                // Retrieve a reference to the worksheet part.
                WorksheetPart wsPart = (WorksheetPart)(wbPart.GetPartById(theSheet.Id));

                // Use its Worksheet property to get a reference to the cell 
                // whose address matches the address you supplied.
                Cell theCell = wsPart.Worksheet.Descendants<Cell>().
                  Where(c => c.CellReference == addressName).FirstOrDefault();

                // If the cell does not exist, return an empty string.
                if (theCell != null)
                {
                    value = theCell.InnerText;

                    // If the cell represents an integer number, you are done. 
                    // For dates, this code returns the serialized value that 
                    // represents the date. The code handles strings and 
                    // Booleans individually. For shared strings, the code 
                    // looks up the corresponding value in the shared string 
                    // table. For Booleans, the code converts the value into 
                    // the words TRUE or FALSE.
                    if (theCell.DataType != null)
                    {
                        switch (theCell.DataType.Value)
                        {
                            case CellValues.SharedString:

                                // For shared strings, look up the value in the
                                // shared strings table.
                                var stringTable =
                                    wbPart.GetPartsOfType<SharedStringTablePart>()
                                    .FirstOrDefault();

                                // If the shared string table is missing, something 
                                // is wrong. Return the index that is in
                                // the cell. Otherwise, look up the correct text in 
                                // the table.
                                if (stringTable != null)
                                {
                                    value =
                                        stringTable.SharedStringTable
                                        .ElementAt(int.Parse(value)).InnerText;
                                }
                                break;

                            case CellValues.Boolean:
                                switch (value)
                                {
                                    case "0":
                                        value = "FALSE";
                                        break;
                                    default:
                                        value = "TRUE";
                                        break;
                                }
                                break;
                        }
                    }
                }
            }
            return value;
        }
        //List<List<string>> readExcel(string excel_path, string sheet_name)
        //{
        //    List<List<string>> entries = new List<List<string>>();
        //    using (SpreadsheetDocument document = SpreadsheetDocument.Open(excel_path, false))
        //    {
        //        WorkbookPart wbPart = document.WorkbookPart;
        //        Sheet theSheet = wbPart.Workbook.Descendants<Sheet>().Where(s => s.Name == sheet_name).FirstOrDefault();
        //        WorksheetPart wsPart = (WorksheetPart)(wbPart.GetPartById(theSheet.Id));

        //        //Column column = wsPart.Worksheet.Descendants<Columns>().First();
        //        Regex colA = new Regex(@"A(\d+)");
        //        Cell theCell = wsPart.Worksheet.Descendants<Cell>().Where(c => colA.Match(c.CellReference).Success).FirstOrDefault();

        //        // If the cell does not exist, return an empty string.
        //        if (theCell != null)
        //        {
        //            string value = theCell.InnerText;

        //            // If the cell represents an integer number, you are done. 
        //            // For dates, this code returns the serialized value that 
        //            // represents the date. The code handles strings and 
        //            // Booleans individually. For shared strings, the code 
        //            // looks up the corresponding value in the shared string 
        //            // table. For Booleans, the code converts the value into 
        //            // the words TRUE or FALSE.
        //            if (theCell.DataType != null)
        //            {
        //                switch (theCell.DataType.Value)
        //                {
        //                    case CellValues.SharedString:

        //                        // For shared strings, look up the value in the
        //                        // shared strings table.
        //                        var stringTable =
        //                            wbPart.GetPartsOfType<SharedStringTablePart>()
        //                            .FirstOrDefault();

        //                        // If the shared string table is missing, something 
        //                        // is wrong. Return the index that is in
        //                        // the cell. Otherwise, look up the correct text in 
        //                        // the table.
        //                        if (stringTable != null)
        //                        {
        //                            value =
        //                                stringTable.SharedStringTable
        //                                .ElementAt(int.Parse(value)).InnerText;
        //                        }
        //                        break;

        //                    case CellValues.Boolean:
        //                        switch (value)
        //                        {
        //                            case "0":
        //                                value = "FALSE";
        //                                break;
        //                            default:
        //                                value = "TRUE";
        //                                break;
        //                        }
        //                        break;
        //                }
        //            }
        //        }
        //    }
        //    return entries;
        //}
        public static DataTable readExcel(string excel_file_name, string excel_sheet_name)
        {
            string conn = "Provider=Microsoft.ACE.OLEDB.12.0;Data Source='" + excel_file_name + "';Extended Properties='Excel 12.0;HDR=Yes';";
            OleDbConnection objConn = new OleDbConnection(conn);
            objConn.Open();
            OleDbCommand objCmdSelect = new OleDbCommand(string.Format("SELECT * FROM [{0}$]",excel_sheet_name), objConn);
            OleDbDataAdapter objAdapter = new OleDbDataAdapter();
            objAdapter.SelectCommand = objCmdSelect;
            DataSet objDataset = new DataSet();
            objAdapter.Fill(objDataset);
            objConn.Close();
            DataTable objTable = objDataset.Tables[0];
            //objTable contains your excel data
            foreach (DataColumn column in objTable.Columns)
            {
                Console.WriteLine("column: " + column.ColumnName);
                foreach (DataRow row in objTable.Rows)
                {
                    Console.WriteLine(string.Format("{0}", row[column]));
                }
            }
            return objTable;
        }
        public static DataTable readExcel(string excel_file_name)
        {
            return readExcel(excel_file_name, "Sheet1");
        }
    }
}
