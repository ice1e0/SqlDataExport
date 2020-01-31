using Microsoft.Hadoop.Avro;
using Microsoft.Hadoop.Avro.Container;
using System;
using System.IO;
using System.Linq;
using System.Data.SqlClient;
using Newtonsoft.Json.Linq;
using NDesk.Options;
using System.Collections.Generic;

namespace SqlDataExport
{
    public static class Extensions
    {
        public static Int32 GetUnixTimestamp(this DateTime dateTime)
        {
            return (Int32)(dateTime.Subtract(new DateTime(1970, 1, 1))).TotalSeconds;
        }
    }

    class Program
    {
        private const string ApplicationName = "SqlDataExport.exe";
        private const int statusReportPerRows = 10000;

        static int Main(string[] args)
        {
            bool showHelp = false;
            bool breakWrongParameterUsage = false;
            string connectionString = string.Empty;
            string sqlQuery = string.Empty;
            string sqlQueryFile = string.Empty;
            string outputFile = string.Empty;
            string format = string.Empty;
            string schema = string.Empty;
            string schemaFile = string.Empty;
            
            var p = new OptionSet() {
                { "c|connection=", "the {SQL CONNECTION STRING} for the SQL Server to connect with.", v => connectionString = v},
                { "q|query=", "the {SQL QUERY} to be used.", v => sqlQuery = v },
                { "qf|queryFile=", "the {SQL QUERY FILE} to beh used.", v => sqlQueryFile = v },
                { "of|outputFile=", "the fiel where to save the export file.", v => outputFile = v },
                { "f|format=", "the {FORMAT} to use for the export file.", v => format = v },
                { "s|schema=", "the schema; required for the AVRO file format", v => schema = v },
                { "sf|schemaFile=", "the schema file; required for the AVRO file format", v => schemaFile = v },
                { "h|help",  "show this message and exit", v => showHelp = v != null },
            };

            // parse args
            List<string> extra;
            try
            {
                extra = p.Parse(args);
            }
            catch (OptionException e)
            {
                Console.Write($"{ApplicationName}: ");
                Console.WriteLine(e.Message);
                Console.WriteLine("Try `sqldataexport --help` for more information.");
                return 1; // Exit Error
            }

            if (showHelp)
            {
                ShowHelp(p);
                return 0; // Exit OK
            }

            // validate args

            if (string.IsNullOrEmpty(connectionString))
            {
                Console.WriteLine("SQL connection string not given");
                breakWrongParameterUsage = true;
            }

            if (string.IsNullOrEmpty(outputFile))
            {
                Console.WriteLine("Output file not given");
                breakWrongParameterUsage = true;
            }

            if (!string.IsNullOrEmpty(sqlQueryFile))
            {
                if (!File.Exists(sqlQueryFile))
                {
                    Console.WriteLine("SQL query file {0} not found", sqlQueryFile);
                    breakWrongParameterUsage = true;
                }

                try
                {
                    sqlQuery = File.ReadAllText(sqlQueryFile);
                }
                catch (Exception e)
                {
                    Console.WriteLine("Error while loading SQL query file: " + e.Message);
                    return 1; // Exit Error
                }
            }

            if (string.IsNullOrEmpty(sqlQuery))
            {
                Console.WriteLine("SQL query not given or empty");
            }

            switch (format.ToLower())
            {
                case "avro":
                    if (!string.IsNullOrEmpty(schemaFile))
                    {
                        if (!File.Exists(schemaFile))
                        {
                            Console.WriteLine("Schema file {0} not found", schemaFile);
                            breakWrongParameterUsage = true;
                        }

                        try
                        {
                            schema = File.ReadAllText(schemaFile);
                        }
                        catch (Exception e)
                        {
                            Console.WriteLine("Error while loading schema file: " + e.Message);
                            return 1; // Exit Error
                        }
                    }

                    if (string.IsNullOrEmpty(schema))
                    {
                        Console.WriteLine("Schema not given or empty");
                        breakWrongParameterUsage = true;
                    }

                    break;

                case "orc":
                    throw new NotImplementedException("https://github.com/ddrinka/ApacheOrcDotNet/blob/master/src/ApacheOrcDotNet.WriterTest.App/Program.cs");
                    break;

                default:
                    Console.WriteLine("Invalid format {0}", format);
                    breakWrongParameterUsage = true;
                    break;
            }

            if (breakWrongParameterUsage)
            {
                Console.WriteLine($"Try `{ApplicationName} --help` for more information.");
                return 1; // Exit Error
            }

            // run actual application
            var connection = new SqlConnection(connectionString);
            var command = new SqlCommand(sqlQuery, connection);

            ReportStatus("Open connection ...", true);

            connection.Open();

            try
            {
                ReportStatus("Execute SQL command ...");
                var dataReader = command.ExecuteReader();

                if (dataReader.Read())
                {
                    ReportStatus("Process rows ...");

                    using (var file = File.Create(outputFile))
                    {
                        // see also: https://code.msdn.microsoft.com/Serialize-data-with-the-86055923/sourcecode?fileId=111532&pathId=931764448
                        var serializer = AvroSerializer.CreateGeneric(schema);
                        var rootSchema = serializer.WriterSchema;

                        var container = AvroContainer.CreateGenericWriter(schema, file, Codec.Null);

                        var schemaFields = GetSchemaFields(schema);

                        using (var wr = new SequentialWriter<object>(container, 24))
                        {
                            int rowCount = 0;
                            do
                            {
                                var record = new AvroRecord(rootSchema);

                                for (int i = 0; i < dataReader.FieldCount; i++)
                                {
                                    object newValue = dataReader.IsDBNull(i) ? null : dataReader.GetValue(i);

                                    dataReader.GetFieldType(i);

                                    record[i] = CastToAvroValue(newValue, schemaFields[i]);
                                }

                                wr.Write(record);

                                if (++rowCount % statusReportPerRows == 0)
                                {
                                    ReportStatus($"Process rows ({rowCount:G}) ...");
                                }
                            } while (dataReader.Read());

                            ReportStatus($"Processed {rowCount:G} rows");
                        }
                    }
                }
            }
            catch(Exception e)
            {
                Console.Write("Application Exception: ");
                Console.WriteLine(e.Message);
                return 1; // Exit Error
            }
            finally
            {
                connection.Close();
            }

            Console.WriteLine("Done");

            return 0; // Exit OK
        }

        private static void ShowHelp(OptionSet p)
        {
            Console.WriteLine($"Usage: {ApplicationName} [OPTIONS]");
            Console.WriteLine("Export a SQL Query to a defined file format.");
            Console.WriteLine();
            Console.WriteLine("Options:");
            p.WriteOptionDescriptions(Console.Out);
            Console.WriteLine();
            Console.WriteLine("Supported formats:");
            Console.WriteLine(" - AVRO - requires a schema");
        }

        private static void ReportStatus(string status, bool firstRow = false)
        {
            if (status == null)
                return;

            if (!firstRow)
            {
                Console.SetCursorPosition(0, Console.CursorTop - 1);
            }

            Console.WriteLine(status.PadRight(79, ' ').Substring(0, 79));
        }

        /*private static JObject CreateSchemaFromDataReader(SqlDataReader dataReader, string schemaName)
        {
            var fields = new JArray();

            for(int i = 0; i < dataReader.FieldCount; i++)
            {
                var fieldName = dataReader.GetName(i);
                var fieldType = dataReader.GetFieldType(i);

                throw new NotImplementedException("We have the problem here that we can't identify when an object is null-able on db-level, e.g. is fieldType == typeof(string) now null-able or not??");
            }

            var schema = new JObject();
            schema["type"] = "record";
            schema["name"] = schemaName;
            schema["fields"] = fields;

            return schema;
        }*/

        private static JObject[] GetSchemaFields(string schema)
        {
            JObject schemaObject = JObject.Parse(schema);

            return (schemaObject["fields"] as JArray)
                .Select(t => t as JObject)
                .ToArray();
        }

        private static bool SchemaFieldSupportsType(JObject schemaField, string avroTypeName)
        {
            var fieldTypeToken = schemaField["type"];

            if (fieldTypeToken is JArray fieldTypeArray)
            {
                return fieldTypeArray.Any(t => t.Value<string>() == avroTypeName);
            }

            return fieldTypeToken.Value<string>() == avroTypeName;
        }

        private static object CastToAvroValue(object value, JObject schemaField)
        {
            var fieldName = schemaField["name"];

            bool nullableType = SchemaFieldSupportsType(schemaField, "null");

            if (value == null)
            {
                if (!nullableType)
                    throw new NotSupportedException("Value null not possible for the field " + fieldName);

                return null;
            }

            var type = value.GetType();

            if (type == typeof(decimal))
            {
                type = typeof(double);
                value = decimal.ToDouble((decimal)value);
            }

            if (type == typeof(bool))
            {
                if (!SchemaFieldSupportsType(schemaField, "boolean"))
                    throw new NotSupportedException("Value boolean not possible for the field " + fieldName);

                return nullableType ? (bool?)value : value;
            }
            else if (type == typeof(byte))
            {
                if (!SchemaFieldSupportsType(schemaField, "bytes"))
                    throw new NotSupportedException("Value bytes not possible for the field " + fieldName);

                return nullableType ? (byte?)value : value;
            }
            else if (type == typeof(int))
            {
                if (!SchemaFieldSupportsType(schemaField, "int"))
                    throw new NotSupportedException("Value int not possible for the field " + fieldName);

                return nullableType ? (int?)value : value;
            }
            else if (type == typeof(long))
            {
                if (!SchemaFieldSupportsType(schemaField, "long"))
                    throw new NotSupportedException("Value long not possible for the field " + fieldName);

                return nullableType ? (long?)value : value;
            }
            else if (type == typeof(float))
            {
                if (!SchemaFieldSupportsType(schemaField, "float"))
                    throw new NotSupportedException("Value float not possible for the field " + fieldName);

                return nullableType ? (float?)value : value;
            }
            else if (type == typeof(double))
            {
                if (!SchemaFieldSupportsType(schemaField, "double"))
                    throw new NotSupportedException("Value double not possible for the field " + fieldName);

                return nullableType ? (double?)value : value;
            }
            else if (type == typeof(string))
            {
                if (!SchemaFieldSupportsType(schemaField, "string"))
                    throw new NotSupportedException("Value string not possible for the field " + fieldName);

                return value; // TODO: in .NET Core 3.0 we need to adjust this with an cast to string?
            }
            else if (type == typeof(DateTime))
            {
                if (!SchemaFieldSupportsType(schemaField, "int"))
                    throw new NotSupportedException("Value int (DateTime) not possible for the field " + fieldName);

                // cast to UNIX Timestamp
                value = ((DateTime)value).GetUnixTimestamp();

                return nullableType ? (int?)value : value;
            }
            else
            {
                throw new NotSupportedException("Unsupported type from SQL:" + type.FullName);
            }
        }
    }
}
