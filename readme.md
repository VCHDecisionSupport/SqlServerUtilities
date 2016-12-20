# ETLPackage

Reads, writes, and creates SSIS packages to and from a filesystem or a Sql server MSDB.

## Command Line Interface

__Usage__ `SqlServerUtilities`
Package Source: `local`, `msdb`

Server Name: <name>

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