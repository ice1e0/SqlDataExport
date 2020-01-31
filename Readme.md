SQL Data Export
===============
A shell application which can export data from an SQL Server to another file format.

Currently supported formats:
* [Apache Avro](https://avro.apache.org/)

Dependencies
------------
The app is build on .NET Core 2.0, but the following libraries require .NET Framework:
* Microsoft.Hadoop.Avro, 1.5.6
* NDesk.Options 0.2.1 (Open source, could ported to .NET Standard)

These dependecies require then to run the code on an windows mashine.

Build
-----
Run the PowerShell script `Build.ps`.
