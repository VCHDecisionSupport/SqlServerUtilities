using System;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.SqlServer.Management.Smo;

namespace SqlServerUtilities
{
    public static class ScriptExtensions
    {

        public static ScriptingOptions GetScriptingOptions()
        {
            ScriptingOptions opts = new ScriptingOptions();
            //scripter.Options.DriAll = true;
            opts.ScriptDrops = false;// script drops of objects that depend on this
            opts.WithDependencies = true; // if ScriptDrops = true than any procs 
            opts.ClusteredIndexes = false;
            opts.IncludeIfNotExists = true;
            opts.Indexes = false;// To include indexes  
            opts.NoCollation = true; // stupid collation for character columns
            opts.NonClusteredIndexes = false;   // to include referential constraints in the script 
            opts.ScriptSchema = true;
            opts.NoFileGroup = true;
            opts.ScriptDrops = true;// script drop if exists instead of create
            return opts;
        }
        public static Scripter GetScripter(Server server)
        {
            Scripter scripter = new Scripter(server);
            scripter.Options = GetScriptingOptions();
            ScriptingOptions opts = new ScriptingOptions();
            return scripter;
        }
        public static string AsString(this StringCollection stringCollection)
        {
            string ret = "";
            foreach (string str in stringCollection)
            {
                ret += "\n" + str;
            }
            return ret;
        }
        public static string GetSchemaObjectName(this SqlSmoObject smoObject)
        {
            if (typeof(Table) == smoObject.GetType())
            {
                Table table = smoObject as Table;
                return string.Format("{0}.{1}", table.Schema, table.Name);
            }
            if (typeof(StoredProcedure) == smoObject.GetType())
            {
                StoredProcedure proc = smoObject as StoredProcedure;
                return string.Format("{0}.{1}", proc.Schema, proc.Name);
            }
            if (typeof(View) == smoObject.GetType())
            {
                View view = smoObject as View;
                return string.Format("{0}.{1}", view.Schema, view.Name);
            }
            if (typeof(UserDefinedFunction) == smoObject.GetType())
            {
                UserDefinedFunction func = smoObject as UserDefinedFunction;
                return string.Format("{0}.{1}", func.Schema, func.Name);
            }
            return "";
        }
        public static string ToScript(this Database database)
        {
            string sql = "";
            Scripter scrtr = GetScripter(database.Parent);
            sql = scrtr.Script(new[] { database as SqlSmoObject }).AsString();
            return sql;
        }
        public static string ToScript(this Schema schema)
        {
            string sql = "";
            Scripter scrtr = GetScripter(schema.Parent.Parent);
            sql = scrtr.Script(new[] { schema as SqlSmoObject }).AsString();
            return sql;
        }
        public static string ToScript(this Table table)
        {
            string sql = "";
            Scripter scrtr = GetScripter(table.Parent.Parent);
            sql = scrtr.Script(new[] { table as SqlSmoObject }).AsString();
            return sql;
        }
        public static string ToScript(this Column column)
        {
            string desc = "";
            // precision and 
            if (new List<SqlDataType> { SqlDataType.Char, SqlDataType.VarChar, SqlDataType.NVarChar }.Contains(column.DataType.SqlDataType))
            {
                desc = string.Format("{0}({1})", column.DataType.ToString(), column.DataType.MaximumLength);
            }
            else if (new List<SqlDataType> { SqlDataType.Decimal, SqlDataType.Float, SqlDataType.Numeric }.Contains(column.DataType.SqlDataType))
            {
                if (column.DataType.NumericScale == 0)
                {
                    desc = string.Format("{0}({1})", column.DataType.ToString(), column.DataType.NumericPrecision);
                }
                else
                {
                    desc = string.Format("{0}({1},{2})", column.DataType.ToString(), column.DataType.NumericPrecision, column.DataType.NumericScale);
                }
            }
            else
            {
                desc = column.DataType.ToString();
            }
            return desc;
        }
        public static string ToTableScript(this View view, string tableName)
        {
            Column col = view.Columns[0];
            string sql = string.Format("CREATE TABLE {0} (\n\t{1}", tableName, string.Format("{0} {1}", col.Name, col.ToScript()));

            for (int i = 1; i < view.Columns.Count; i++)
            {
                sql += string.Format("\n\t,{0} {1}", view.Columns[i].Name, view.Columns[i].ToScript());
            }
            sql += string.Format("\n);");
            return sql;
        }
        public static string ToTableScript(this View view, string tableName, string schemaName)
        {
            Column col = view.Columns[0];
            string sql = string.Format("CREATE TABLE {2}.{0} (\n\t{1}", schemaName, tableName, string.Format("{0} {1}", col.Name, col.ToScript()));

            for (int i = 1; i < view.Columns.Count; i++)
            {
                sql += string.Format("\n\t,{0} {1}", view.Columns[i].Name, view.Columns[i].ToScript());
            }
            sql += string.Format("\n);");
            return sql;
        }
    }



    //    public static class ScriptTemplate
    //    {

    //public static string ToScript(this Database database)
    //{
    //    // if DNE then create else nothing
    //    string ddl = GetDatabaseDdl.Replace(GetDatabaseNamePattern, database.Name);
    //    return ddl;
    //}
    //public static string ToScript(this Schema schema)
    //{
    //    // if DNE then create else nothing
    //    string ddl = GetSchemaDdl.Replace(GetDatabaseNamePattern, schema.Parent.Name);
    //    ddl = ddl.Replace(GetSchemaNamePattern, schema.Name);
    //    return ddl;
    //}
    //public static string ToScript(this Table table)
    //{
    //    // if exists then drop GO CREATE
    //    string ddl = GetTableDdl.Replace(GetDatabaseNamePattern, table.Parent.Name);
    //    ddl = ddl.Replace(GetTableNamePattern, table.GetSchemaObjectName());
    //    Scripter scripter = GetScripter(table.Parent.Parent);
    //    ddl = ddl.Replace(GetScripterPattern, scripter.Script(new SqlSmoObject[] { table as SqlSmoObject }).AsString());
    //    return ddl;
    //}
    //public static string ToScript(this StoredProcedure proc)
    //{
    //    // if DNE then create placeholder GO ALTER
    //    string ddl = GetProcDdl.Replace(GetDatabaseNamePattern, proc.Parent.Name);
    //    ddl = ddl.Replace(GetProcNamePattern, proc.GetSchemaObjectName());
    //    Scripter scripter = GetScripter(proc.Parent.Parent);
    //    ddl = ddl.Replace(GetScripterPattern, scripter.Script(new SqlSmoObject[] { proc as SqlSmoObject }).AsString());
    //    return ddl;
    //}
    //public static string ToScript(this View view)
    //{
    //    // if DNE then create placeholder GO ALTER
    //    string ddl = GetProcDdl.Replace(GetDatabaseNamePattern, view.Parent.Name);
    //    ddl = ddl.Replace(GetViewNamePattern, view.GetSchemaObjectName());
    //    Scripter scripter = GetScripter(view.Parent.Parent);
    //    ddl = ddl.Replace(GetScripterPattern, scripter.Script(new SqlSmoObject[] { view as SqlSmoObject }).AsString());
    //    return ddl;
    //}
    //public static string ToScript(this UserDefinedFunction func)
    //{
    //    if (func.FunctionType == UserDefinedFunctionType.Scalar)
    //    {

    //    }
    //    else if (func.FunctionType == UserDefinedFunctionType.Table)
    //    {

    //    }
    //    else
    //    {
    //        Console.WriteLine(string.Format("{0} has type {1}", func.GetSchemaObjectName(), func.FunctionType.ToString()));
    //    }
    //    // if DNE then create placeholder GO ALTER
    //    return string.Format("USE {0}\nGO\n\n\nIF OBJECT_ID('{1}.{2}','FN') IS NULL\nBEGIN\n\tSET @sql = 'CREATE FUNCTION {1}.{2}() RETURNS INT BEGIN RETURN 0 END;';\n\tPRINT @sql;\n\tEXEC(@sql);\nEND\nGO\n\n\n", func.Parent.Name, func.Schema, func.Name);
    //}
    //public static string ToScript(this Index index)
    //{
    //    throw new NotImplementedException();
    //    //return "CREATE DATABASE abc;";
    //}


    //        private static string GetDatabaseNamePattern
    //        {
    //            get
    //            {
    //                string pattern = @"<dst_database_name,,>";
    //                return pattern;
    //            }
    //        }
    //        private static string GetSchemaNamePattern
    //        {
    //            get
    //            {
    //                string pattern = @"<dst_schema_name,,>";
    //                return pattern;
    //            }
    //        }
    //        private static string GetTableNamePattern
    //        {
    //            get
    //            {
    //                string pattern = @"<dst_table_name,,>";
    //                return pattern;
    //            }
    //        }
    //        private static string GetProcNamePattern
    //        {
    //            get
    //            {
    //                string pattern = @"<dst_proc_name,,>";
    //                return pattern;
    //            }
    //        }
    //        private static string GetViewNamePattern
    //        {
    //            get
    //            {
    //                string pattern = @"<dst_view_name,,>";
    //                return pattern;
    //            }
    //        }


    //        private static string GetScripterPattern
    //        {
    //            get
    //            {
    //                string pattern = @"<scripter,,>";
    //                return pattern;
    //            }
    //        }

    //        private static string GetDatabaseDdl
    //        {
    //            get
    //            {
    //                string sql = @"
    //--#region CREATE DATABASE
    //USE master
    //GO

    //DECLARE @name nvarchar(max);
    //DECLARE @sql nvarchar(max);

    //SET @name = '<dst_database_name,,>';
    //SET @sql = FORMATMESSAGE('CREATE DATABASE %s;', @name);

    //IF DB_ID(@name) IS NULL
    //BEGIN
    //	IF EXISTS(SELECT * FROM sys.master_files WHERE name = @name)
    //	BEGIN
    //        RAISERROR('ERROR: database file %s already exists', 11, 1, @name) WITH NOWAIT;
    //		SELECT DB_NAME(database_id) AS database_name, * FROM sys.master_files WHERE name = @name
    //	END
    //	ELSE
    //	BEGIN
    //		RAISERROR(@sql, 0, 10) WITH NOWAIT;
    //        EXEC(@sql);
    //	END
    //END
    //GO
    //--#endregion CREATE DATABASE
    //";
    //                return sql;
    //            }
    //        }
    //        private static string GetSchemaDdl
    //        {
    //            get
    //            {
    //                string sql = @"
    //--#region CREATE DATABASE
    //USE master
    //GO

    //DECLARE @name nvarchar(max);
    //DECLARE @sql nvarchar(max);

    //SET @name = '<dst_database_name,,>';
    //SET @sql = FORMATMESSAGE('CREATE DATABASE %s;', @name);

    //IF DB_ID(@name) IS NULL
    //BEGIN
    //	IF EXISTS(SELECT * FROM sys.master_files WHERE name = @name)
    //	BEGIN
    //        RAISERROR('ERROR: database file %s already exists', 11, 1, @name) WITH NOWAIT;
    //		SELECT DB_NAME(database_id) AS database_name, * FROM sys.master_files WHERE name = @name
    //	END
    //	ELSE
    //	BEGIN
    //		RAISERROR(@sql, 0, 10) WITH NOWAIT;
    //        EXEC(@sql);
    //	END
    //END
    //GO
    //--#endregion CREATE DATABASE
    //";
    //                return sql;
    //            }
    //        }
    //        private static string GetTableDdl
    //        {
    //            get
    //            {
    //                string sql = @"
    //--#region CREATE TABLE
    //USE <dst_database_name,,>
    //GO

    //DECLARE @name nvarchar(max);
    //DECLARE @sql nvarchar(max);

    //SET @name = '<dst_table_name,,>';
    //SET @sql = FORMATMESSAGE('DROP TABLE %s;',@name);

    //IF OBJECT_ID(@name,'U') IS NOT NULL
    //BEGIN
    //	RAISERROR(@sql, 0, 0) WITH NOWAIT;
    //	EXEC(@sql);
    //END

    //<scripter,,>
    //GO
    //--#endregion CREATE TABLE
    //";
    //                return sql;
    //            }
    //        }
    //        private static string GetProcDdl
    //        {
    //            get
    //            {
    //                string sql = @"
    //--#region CREATE/ALTER PROC
    //USE <dst_database_name,,>
    //GO

    //DECLARE @name nvarchar(max);
    //DECLARE @sql nvarchar(max);

    //SET @name = '<dst_proc_name,,>';
    //SET @sql = FORMATMESSAGE('CREATE PROC %s AS BEGIN SELECT 1 AS [one] END;',@name);

    //IF OBJECT_ID(@name,'P') IS NULL
    //BEGIN
    //	RAISERROR(@sql, 0, 0) WITH NOWAIT;
    //	EXEC(@sql);
    //END

    //<scripter,,>
    //GO
    //--#endregion CREATE/ALTER PROC
    //";
    //                return sql;
    //            }
    //        }
    //        private static string GetViewDdl
    //        {
    //            get
    //            {
    //                string sql = @"
    //--#region CREATE/ALTER VIEW
    //USE <dst_database_name,,>
    //GO

    //DECLARE @name nvarchar(max);
    //DECLARE @sql nvarchar(max);

    //SET @name = '<dst_view_name,,>';
    //SET @sql = FORMATMESSAGE('CREATE VIEW %s AS SELECT 1 AS [one];',@name);

    //IF OBJECT_ID(@name,'V') IS NULL
    //BEGIN
    //	RAISERROR(@sql, 0, 0) WITH NOWAIT;
    //	EXEC(@sql);
    //END

    //<scripter,,>
    //GO
    //--#endregion CREATE/ALTER VIEW
    //";
    //                return sql;
    //            }
    //        }
    //        private static string GetFuncTableDdl
    //        {
    //            get
    //            {
    //                string sql = @"
    //--#region CREATE/ALTER TABLE FUNC
    //USE <dst_database_name,,>
    //GO

    //DECLARE @name nvarchar(max);
    //DECLARE @sql nvarchar(max);

    //SET @name = '<dst_table_func_name,,>';
    //SET @sql = FORMATMESSAGE('CREATE FUNC %s() RETURNS TABLE AS BEGIN RETURN SELECT 1 AS [one] END;',@name);

    //IF OBJECT_ID(@name,'IF') IS NULL
    //BEGIN
    //	RAISERROR(@sql, 0, 0) WITH NOWAIT;
    //	EXEC(@sql);
    //END

    //ALTER FUNCTION <dst_table_func_name,,>
    //AS
    //	<function_definition,,>
    //;
    //GO
    //--#endregion CREATE/ALTER TABLE FUNC
    //";
    //                return sql;
    //            }
    //        }
    //    } 
}
