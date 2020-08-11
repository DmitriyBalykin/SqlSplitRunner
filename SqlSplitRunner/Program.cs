using System;
using System.IO;

namespace SqlSplitRunner
{
    class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine("Starting sql file splitting");

            var sqlFilePath = args[0];
            var databaseName = args[1];
            var linesPerFile = int.Parse(args[2]);
            var serverPath = args[3];

            var sqlFileName = Path.GetFileName(sqlFilePath);
            var splitFilesDirPath = sqlFilePath + ".split";
            Directory.CreateDirectory(splitFilesDirPath);

            var linesProcessor = new SqlCasesHandler(databaseName);

            var fileWriter = new FileSplitter(sqlFilePath, splitFilesDirPath, sqlFileName, linesPerFile, linesProcessor);

            var totalLinesWritten = fileWriter.Run();

            Console.WriteLine($"File splitting finished. Total lines written: {totalLinesWritten}");

            var sqlFileReader = new SqlFilesRunner(serverPath, splitFilesDirPath);

            sqlFileReader.Run();

            Console.WriteLine($"All tasks finished.");
        }
    }
}
