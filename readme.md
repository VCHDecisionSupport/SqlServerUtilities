# ETLPackage

Reads, writes, and creates SSIS packages to and from a filesystem or a Sql server MSDB.

## Command Line Interface
(created using [docopt.net](https://github.com/docopt/docopt.net))

    SqlServerUtilities.

        Usage:
          SqlServerUtilities.exe [--markdown=<markdown_filename>] [--packagepath=<packagepath>] [-server=<server_name>] (--msdb|--local)
          SqlServerUtilities.exe [--map] [--packagepath=<packagepath>] [--server=<server_name>] (--msdb|--local)

        Options:
          -h --help                        Show this screen.
          --markdown=<markdown_filename>   readme markdown full path filename [default: readme]
          --map                            indicates Map.PackageTable table will be populated from package dataflow destination tables.
          --msdb                           indicates package is deployed to SSIS MSDB folder.  (packagepath specifies MSDB path)
          --local                          indicates package exists on file system.  (packagepath specifies file system path)
          --packagepath=<packagepath>      path of package or folder containing package(s).  can be local file system path or SSIS MSDB path.
          --server=<server_name>           name of SQL server instance.

## Namespaces used:
- `Microsoft.SqlServer.Dts` SSIS object model
- `Microsoft.SqlServer.Management.Smo` SQL Server management objects provides a __object relational mapper__ for C# 

## Classes:

### `EtlPackageReader`
- reads *.dtsx files and MSDB 
- acts a event provider for listeners that consume SSIS executables

#### `PackageTableSqlInserter`
- listens to `EtlPackageReader` and inserts into `Map.PackageTable` used by [`AutoTest`](https://github.com/VCHDecisionSupport/AutoTest) repo

#### `MarkDownWriter`
- listens to `EtlPackageReader` and writes Markdown documentation

### `EtlPackageBuilder`
provides C# API to create SSIS packages programmically. 

## NuGet dependancies: 

- [Costura.Fody](http://stackoverflow.com/questions/189549/embedding-dlls-in-a-compiled-executable) (to embed required dll in exe)
- [docopt.net](https://github.com/docopt/docopt.net) (for CLI)