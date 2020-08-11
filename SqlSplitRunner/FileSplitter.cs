using System;
using System.IO;

namespace SqlSplitRunner
{
    public class FileSplitter
    {
        private string sqlFileName;
        private string splitFilesFolder;
        private string sourceFilePath;
        private SqlCasesHandler linesProcessor;
        private int linesPerFile;
        private int currentFileLinesCount = 0;
        private int currentFileNumber = 0;
        private long totalLines = 0;

        public FileSplitter(string sourceFilePath, string splitFilesFolder, string sqlFileName, int linesPerFile, SqlCasesHandler linesProcessor)
        {
            this.splitFilesFolder = splitFilesFolder;
            this.sqlFileName = sqlFileName;
            this.linesPerFile = linesPerFile - 10;
            this.sourceFilePath = sourceFilePath;

            this.linesProcessor = linesProcessor;
        }

        public long Run()
        {
            StreamWriter streamWriter = null;
            try
            {
                using (var streamReader = new StreamReader(sourceFilePath))
                {
                    var isFirstLine = true;
                    var isChunkLastLine = false;
                    string line = null;
                    while (true)
                    {
                        if (isChunkLastLine)
                        {
                            //write previous chink latest line to new file
                            isChunkLastLine = false;
                            isFirstLine = true;
                        }
                        else
                        {
                            //else read new line
                            line = streamReader.ReadLine();
                        }

                        if (streamWriter == null)
                        {
                            streamWriter = new StreamWriter(CurrentFilePath);
                            Console.WriteLine($"Writing to file {CurrentFilePath}");
                        }

                        isChunkLastLine = IsTimeToSplit(line);

                        if (isFirstLine || isChunkLastLine)
                        {
                            foreach (var processedLine in linesProcessor.ProcessTerminatingLine(line, isFirstLine, isChunkLastLine))
                            {
                                streamWriter.WriteLine(processedLine);
                            } 
                        }
                        else
                        {
                            linesProcessor.UpdateTable(line);
                            streamWriter.WriteLine(line);
                        }

                        if (isChunkLastLine)
                        {
                            streamWriter.Flush();
                            streamWriter.Close();
                            streamWriter = null;
                        }

                        if (line == null)
                        {
                            break;
                        }

                        isFirstLine = false;
                    }
                }
            }
            catch (Exception ex)
            {
                Console.Error.WriteLine(ex.Message);
            }
            finally
            {
                if (streamWriter != null)
                {
                    streamWriter.Flush();
                    streamWriter.Close();
                }
            }

            return totalLines;
        }

        private string CurrentFilePath => Path.Combine(splitFilesFolder, $"{currentFileNumber:d3}_{sqlFileName}");
        
        private bool IsTimeToSplit(string line)
        {
            if (line == null)
            {
                return true;
            }

            currentFileLinesCount++;

            if (!line.StartsWith("INSERT"))
            {
                return false;
            }

            if (currentFileLinesCount > linesPerFile)
            {
                currentFileNumber++;
                totalLines += currentFileLinesCount;
                currentFileLinesCount = 0;

                return true;
            }

            return false;
        }
    }
}
