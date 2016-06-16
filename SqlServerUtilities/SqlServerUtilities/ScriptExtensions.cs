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
        private static string GetDatabaseNamePattern
        {
            get
            {
                string pattern = @"<dst_database_name,,>";
                return pattern;
            }
        }
        private static string GetSchemaNamePattern
        {
            get
            {
                string pattern = @"<dst_schema_name,,>";
                return pattern;
            }
        }
        private static string GetTableNamePattern
        {
            get
            {
                string pattern = @"<dst_table_name,,>";
                return pattern;
            }
        }
        private static string GetScripterPattern
        {
            get
            {
                string pattern = @"<scripter,,>";
                return pattern;
            }
        }

        private static string GetDatabaseDdl
        {
            get
            {
                string sql = @"
--#region CREATE DATABASE
USE master
GO

DECLARE @name nvarchar(max);
DECLARE @sql nvarchar(max);

SET @name = '<dst_database_name,,>';
SET @sql = FORMATMESSAGE('CREATE DATABASE %s;', @name);

IF DB_ID(@name) IS NULL
BEGIN
	IF EXISTS(SELECT * FROM sys.master_files WHERE name = @name)
	BEGIN
        RAISERROR('ERROR: database file %s already exists', 11, 1, @name) WITH NOWAIT;
		SELECT DB_NAME(database_id) AS database_name, * FROM sys.master_files WHERE name = @name
	END
	ELSE
	BEGIN
		RAISERROR(@sql, 0, 10) WITH NOWAIT;
        EXEC(@sql);
	END
END
GO
--#endregion CREATE DATABASE
";
                return sql;
            }
        }
        private static string GetSchemaDdl
        {
            get
            {
                string sql = @"
--#region CREATE DATABASE
USE master
GO

DECLARE @name nvarchar(max);
DECLARE @sql nvarchar(max);

SET @name = '<dst_database_name,,>';
SET @sql = FORMATMESSAGE('CREATE DATABASE %s;', @name);

IF DB_ID(@name) IS NULL
BEGIN
	IF EXISTS(SELECT * FROM sys.master_files WHERE name = @name)
	BEGIN
        RAISERROR('ERROR: database file %s already exists', 11, 1, @name) WITH NOWAIT;
		SELECT DB_NAME(database_id) AS database_name, * FROM sys.master_files WHERE name = @name
	END
	ELSE
	BEGIN
		RAISERROR(@sql, 0, 10) WITH NOWAIT;
        EXEC(@sql);
	END
END
GO
--#endregion CREATE DATABASE
";
                return sql;
            }
        }
        private static string GetTableDdl
        {
            get
            {
                string sql = @"
--#region CREATE TABLE
USE <dst_database_name,,>
GO

DECLARE @name nvarchar(max);
DECLARE @sql nvarchar(max);

SET @name = '<dst_table_name,,>';
SET @sql = FORMATMESSAGE('DROP TABLE %s;',@name);

IF OBJECT_ID(@name,'U') IS NOT NULL
BEGIN
	RAISERROR(@sql, 0, 0) WITH NOWAIT;
	EXEC(@sql);
END

<scripter,,>
GO
--#endregion CREATE TABLE
";
                return sql;
            }
        }
        private static string GetProcDdl
        {
            get
            {
                string sql = @"
--#region CREATE/ALTER PROC
USE <dst_database_name,,>
GO

DECLARE @name nvarchar(max);
DECLARE @sql nvarchar(max);

SET @name = '<dst_proc_name,,>';
SET @sql = FORMATMESSAGE('CREATE PROC %s AS BEGIN SELECT 1 AS [one] END;',@name);

IF OBJECT_ID(@name,'P') IS NULL
BEGIN
	RAISERROR(@sql, 0, 0) WITH NOWAIT;
	EXEC(@sql);
END

<scripter,,>
GO
--#endregion CREATE/ALTER PROC
";
                return sql;
            }
        }


        public static ScriptingOptions GetScriptingOptions()
        {
            ScriptingOptions opts = new ScriptingOptions();
            //scripter.Options.DriAll = true;
            opts.ScriptDrops = false;// script drops of objects that depend on this
            opts.WithDependencies = false;
            opts.ClusteredIndexes = false;
            opts.IncludeIfNotExists = false;
            opts.Indexes = false;// To include indexes  
            opts.NoCollation = true; // stupid collation for character columns
            opts.NonClusteredIndexes = false;   // to include referential constraints in the script 
            opts.ScriptSchema = true;
            opts.NoFileGroup = true;
            return opts;
        }
        public static Scripter GetScripter(Server server)
        {
            Scripter scripter = new Scripter(server);
            scripter.Options = GetScriptingOptions();
            ScriptingOptions opts = new ScriptingOptions();
            return scripter;
        }
        public static string AsString(this StringCollection string_collection)
        {
            string ret = "";
            foreach (string str in string_collection)
            {
                ret += "\n" + str;
            }
            return ret;
        }
        public static string GetSchemaObjectName(this SqlSmoObject smo_object)
        {
            if (typeof(Table) == smo_object.GetType())
            {
                Table table = smo_object as Table;
                return string.Format("{0}.{1}", table.Schema, table.Name);
            } if (typeof(StoredProcedure) == smo_object.GetType())
            {
                StoredProcedure proc = smo_object as StoredProcedure;
                return string.Format("{0}.{1}", proc.Schema, proc.Name);
            } if (typeof(View) == smo_object.GetType())
            {
                View view = smo_object as View;
                return string.Format("{0}.{1}", view.Schema, view.Name);
            } if (typeof(UserDefinedFunction) == smo_object.GetType())
            {
                UserDefinedFunction func = smo_object as UserDefinedFunction;
                return string.Format("{0}.{1}", func.Schema, func.Name);
            }
            return "";
        }
        public static string ToScript(this Database database)
        {
            // if DNE then create else nothing
            string ddl = GetDatabaseDdl.Replace(GetDatabaseNamePattern, database.Name);
            return ddl;
        }
        public static string ToScript(this Schema schema)
        {
            // if DNE then create else nothing
            string ddl = GetSchemaDdl.Replace(GetDatabaseNamePattern, schema.Parent.Name);
            ddl = ddl.Replace(GetSchemaNamePattern, schema.Name);
            return ddl;
        }
        public static string ToScript(this Table table)
        {
            // if exists then drop GO CREATE
            string ddl = GetTableDdl.Replace(GetDatabaseNamePattern, table.Parent.Name);
            ddl = ddl.Replace(GetTableNamePattern, table.GetSchemaTableName());
            Scripter scripter = GetScripter(table.Parent.Parent);
            ddl = ddl.Replace(GetScripterPattern, scripter.Script(new SqlSmoObject[] { table as SqlSmoObject }).AsString());
            return ddl;
        }
        public static string ToScript(this StoredProcedure proc)
        {

            // if DNE then create placeholder GO ALTER
            string ddl = GetProcDdl.Replace(GetDatabaseNamePattern, proc.Parent.Name);
            ddl = ddl.Replace(GetTableNamePattern, table.GetSchemaTableName());
            Scripter scripter = GetScripter(table.Parent.Parent);
            ddl = ddl.Replace(GetScripterPattern, scripter.Script(new SqlSmoObject[] { table as SqlSmoObject }).AsString());
            return ddl;
        }
        public static string ToScript(this View view)
        {

            // if DNE then create placeholder GO ALTER
            return string.Format("USE {0}\nGO\nIF OBJECT_ID('{1}.{2}','V') IS NULL \nBEGIN\n\tDECLARE @sql varchar(max);\n\tSET @sql = 'CREATE VIEW {1}.{2} AS SELECT 1 AS one;';\n\tPRINT @sql;\n\tEXEC(@sql);\nEND\nGO\n\n\n", view.Parent.Name, view.Schema, view.Name);
        }
        public static string ToScript(this UserDefinedFunction func)
        {

            // if DNE then create placeholder GO ALTER
            return string.Format("USE {0}\nGO\n\n\nIF OBJECT_ID('{1}.{2}','FN') IS NULL\nBEGIN\n\tSET @sql = 'CREATE FUNCTION {1}.{2}() RETURNS INT BEGIN RETURN 0 END;';\n\tPRINT @sql;\n\tEXEC(@sql);\nEND\nGO\n\n\n", func.Parent.Name, func.Schema, func.Name);
        }
        public static string ToScript(this Index index)
        {
            throw new NotImplementedException();
            //return "CREATE DATABASE abc;";
        }
    }
}
