using System;
using System.Diagnostics;
using System.IO;
using System.Linq;

namespace SqlSplitRunner
{
    public class SqlFilesRunner
    {
        private string serverPath;
        private string splittedFilesDirPath;
        private int linesNumber = 0;
        DateTime startTime;
        int processedItems = 0;
        int totalItems;

        public SqlFilesRunner(string serverPath, string splittedFilesDirPath)
        {
            this.serverPath = serverPath;
            this.splittedFilesDirPath = splittedFilesDirPath;
        }

        internal void Run()
        {
            startTime = DateTime.Now;
            var filePathes = Directory.EnumerateFiles(splittedFilesDirPath);
            totalItems = filePathes.Count();

            foreach (var filePath in filePathes)
            {
                Process process = null;
                try
                {
                    linesNumber = 0;

                    process = new Process();
                    process.StartInfo = new ProcessStartInfo
                    {
                        CreateNoWindow = true,
                        FileName = "sqlcmd",
                        Arguments = $"-E -S {serverPath} -i {filePath}",
                        UseShellExecute = false,
                        RedirectStandardError = true,
                        RedirectStandardOutput = true
                    };
                    process.OutputDataReceived += OutputDataReceived;
                    process.ErrorDataReceived += ErrorOutputDataReceived;

                    if(processedItems > 0)
                    {
                        Console.SetCursorPosition(0, Console.CursorTop - 1);
                    }

                    Console.WriteLine($"{DateTime.Now}: executing file: {Path.GetFileName(filePath)}.");

                    process.Start();
                    process.BeginOutputReadLine();
                    process.BeginErrorReadLine();
                    process.WaitForExit();
                    processedItems++;
                }
                catch (Exception ex)
                {
                    Console.Error.WriteLine(ex.Message);
                }
                finally
                {
                    if (process != null)
                    {
                        process.OutputDataReceived -= OutputDataReceived;
                        process.ErrorDataReceived -= ErrorOutputDataReceived;
                    }
                }
            }
        }

        private TimeSpan GetEstimatedTimeLeft()
        {
            var timeSpent = DateTime.Now - startTime;
            var timePerItem = timeSpent.TotalSeconds / processedItems;
            var timeLeft = timePerItem * (totalItems - processedItems);
            return TimeSpan.FromSeconds(timeLeft);
        }

        private void ErrorOutputDataReceived(object sender, DataReceivedEventArgs e)
        {
            if (e.Data == null)
            {
                return;
            }

            Console.Error.WriteLine($"Error: {e.Data}");
        }

        private void OutputDataReceived(object sender, DataReceivedEventArgs e)
        {
            if (e.Data == null || e.Data.StartsWith("Changed database context"))
            {
                return;
            }

            if (e.Data.Equals("(1 rows affected)", StringComparison.InvariantCultureIgnoreCase))
            {
                LineWritten();

                return;
            }

            if (!string.IsNullOrEmpty(e.Data))
            {
                Console.WriteLine(e.Data); 
            }
        }

        private void LineWritten()
        {
            linesNumber++;

            if (linesNumber % 1000 == 0)
            {
                if (linesNumber > 1000)
                {
                    Console.SetCursorPosition(0, Console.CursorTop - 1);
                }
                Console.WriteLine($"Lines written: {linesNumber}.  Time left: {GetEstimatedTimeLeft()}                    ");
            }
        }
    }
}
