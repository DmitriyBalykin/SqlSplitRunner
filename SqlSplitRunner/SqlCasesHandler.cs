using System;
using System.Collections.Generic;

namespace SqlSplitRunner
{
    public class SqlCasesHandler
    {
        private string currentTable;
        private string databaseName;

        public SqlCasesHandler(string databaseName)
        {
            this.databaseName = databaseName;
        }

        public void UpdateTable(string line)
        {
            var table = ParseTableName(line);
            if (table != null)
            {
                currentTable = table;
            }
        }

        public IList<string> ProcessTerminatingLine(string line, bool isFirstLine, bool isLastLine)
        {
            var sqlLines = new List<string>();

            if (isFirstLine)
            {
                sqlLines.Add($"USE [{databaseName}]");
                sqlLines.Add("GO");

                if (!line.StartsWith("USE ["))
                {
                    var table = ParseTableName(line);

                    sqlLines.Add($"SET IDENTITY_INSERT {table} ON");
                    sqlLines.Add("GO");

                    sqlLines.Add(line);
                }
            }

            if (isLastLine)
            {
                sqlLines.Add($"SET IDENTITY_INSERT {currentTable} OFF");
                sqlLines.Add("GO");
            }

            return sqlLines;
        }

        private string ParseTableName(string line)
        {
            // INSERT INTO dbo.[Orders]

            if (string.IsNullOrEmpty(line) || !line.StartsWith("INSERT", StringComparison.InvariantCultureIgnoreCase))
            {
                return null;
            }

            var parts = line.Split(" ");

            return parts[1];
        }
    }
}
