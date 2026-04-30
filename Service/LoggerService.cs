using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Service
{
    public static class LoggerService
    {
        private static readonly object _lock = new object();
        private static string logPath = "logs.txt";

        public static void Info(string message)
        {
            Write("INFO", message);
        }

        public static void Warning(string message)
        {
            Write("WARNING", message);
        }

        public static void Error(string message)
        {
            Write("ERROR", message);
        }

        public static void Fault(string message)
        {
            Write("FAULT", message);
        }

        private static void Write(string level, string message)
        {
            lock (_lock)
            {
                File.AppendAllText(logPath, $"{DateTime.Now:yyyy-MM-dd HH:mm:ss} [{level}] {message}\n");
            }
        }
    }
}
