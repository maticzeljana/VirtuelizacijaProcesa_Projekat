using Common;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.ServiceModel;
using System.Text;
using System.Threading.Tasks;
using System.Configuration;

namespace Client
{
    public class Program 
    {
        private StreamReader reader;
        private StreamWriter rejectedWriter;

        static void Main(string[] args)
        {
            ChannelFactory<IPvService> factory = new ChannelFactory<IPvService>("Service");
            IPvService proxy = factory.CreateChannel();

            Program p = new Program();

            try
            {
                int maxRows = int.Parse(ConfigurationManager.AppSettings["RowLimitN"]);
                var samples = p.ParseCsv("FPV_Altamonte_FL_data.csv", maxRows);

                foreach (var s in samples)
                {
                    proxy.PushSample(s);
                }

                Console.WriteLine("Done: " + samples.Count);
            }
            finally
            {
                p.reader?.Close();
                p.rejectedWriter?.Close();

                ((ICommunicationObject)proxy)?.Close();
                factory?.Close();
            }
        }

        public List<PvSample> ParseCsv(string path, int maxRows)
        {
            List<PvSample> result = new List<PvSample>();

            reader = new StreamReader(path);
            rejectedWriter = new StreamWriter("rejected_client.csv");

            var headers = reader.ReadLine().Split(',');

            int dayIdx = Array.IndexOf(headers, "DAY");
            int hourIdx = Array.IndexOf(headers, "HOUR");

            int acPwrtIdx = Array.IndexOf(headers, "ACPWRT");
            int dcVoltIdx = Array.IndexOf(headers, "DCVOLT");
            int temperIdx = Array.IndexOf(headers, "TEMPER");

            int vl1Idx = Array.IndexOf(headers, "VL1TO2");
            int vl2Idx = Array.IndexOf(headers, "VL2TO3");
            int vl3Idx = Array.IndexOf(headers, "VL3TO1");

            int acc1Idx = Array.IndexOf(headers, "ACCUR1");
            int acv1Idx = Array.IndexOf(headers, "ACVLT1");

            int count = 0;

            while (!reader.EndOfStream && count < maxRows)
            {
                var parts = reader.ReadLine().Split(',');

                try
                {
                    var sample = new PvSample
                    {
                        Day = int.Parse(parts[dayIdx]),
                        Hour = TimeSpan.Parse(parts[hourIdx]).Hours,

                        AcPwrt = ParseNullable(parts[acPwrtIdx]),
                        DcVolt = ParseNullable(parts[dcVoltIdx]),
                        Temper = ParseNullable(parts[temperIdx]),

                        Vl1to2 = ParseNullable(parts[vl1Idx]),
                        Vl2to3 = ParseNullable(parts[vl2Idx]),
                        Vl3to1 = ParseNullable(parts[vl3Idx]),

                        AcCur1 = ParseNullable(parts[acc1Idx]),
                        AcVlt1 = ParseNullable(parts[acv1Idx]),

                        RowIndex = count
                    };

                    result.Add(sample);
                    count++;
                }
                catch (Exception ex)
                {
                    rejectedWriter.WriteLine(string.Join(",", parts) + ",ERROR:" + ex.Message);
                }
            }

            return result;
        }

        static double? ParseNullable(string value)
        {
            if (double.TryParse(value, NumberStyles.Any, CultureInfo.InvariantCulture, out double d))
            {
                if (d == 32767.0)
                    return null;

                return d;
            }

            return null;
        }
    }
}
