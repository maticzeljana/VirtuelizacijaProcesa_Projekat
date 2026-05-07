using Common;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.ServiceModel;
using System.Text;
using System.Threading.Tasks;
using System.Configuration;

namespace Service
{
    public class PvService : IPvService, IDisposable
    {
        private StreamWriter _sessionWriter;
        private StreamWriter _rejectWriter;
        private static string _sessionPath;
        private static string _rejectPath;
        private string _basePath;

        private readonly double _overTempThreshold =
            double.Parse(ConfigurationManager.AppSettings["OverTempThreshold"]);

        private readonly double _voltageImbalancePct =
            double.Parse(ConfigurationManager.AppSettings["VoltageImbalancePct"]);

        private readonly int _powerFlatlineWindow =
            int.Parse(ConfigurationManager.AppSettings["PowerFlatlineWindow"]);

        private readonly double _acCur1SpikeThreshold =
            double.Parse(ConfigurationManager.AppSettings["AcCur1SpikeThreshold"]);

        private int _lastRowIndex = -1;
        private double? _previousPower = null;
        private int _flatlineCounter = 0;
        private static int _totalRows;
        private static int _receivedRows;

        private bool disposed = false;
        private bool _manualDispose = false;
        public void StartSession(PvMeta meta)
        {
            string date = DateTime.Now.ToString("yyyy-MM-dd");

            _basePath = Path.Combine("Data", meta.PlantID, date);

            Directory.CreateDirectory(_basePath);

            _sessionPath = Path.Combine(_basePath, "session.csv");
            _rejectPath = Path.Combine(_basePath, "rejects.csv");

            EnsureWriters();

            LoggerService.Info("Session started");

            _totalRows = meta.RowLimitN;
            _receivedRows = 0;
            Console.WriteLine("Transfer...");
        }

        public SampleResult PushSample(PvSample sample)
        {
            EnsureWriters();

            var result = ValidateSample(sample);

            if (result.isValid)
            {
                _sessionWriter.WriteLine(ToCsv(sample));
                _sessionWriter.Flush();

                return new SampleResult
                {
                    IsValid = true,
                    Message = !string.IsNullOrWhiteSpace(result.reason)
                        ? result.reason
                        : $"Sample {sample.RowIndex} processed"
                };
            }
            else
            {
                _rejectWriter.WriteLine($"{sample.RawLine},{result.reason}");
                _rejectWriter.Flush();

                return new SampleResult
                {
                    IsValid = false,
                    Message = result.reason
                };
            }
        }
        public void EndSession()
        {
            LoggerService.Info("Session ended");
            _manualDispose = true;
            Dispose();

            Console.WriteLine("\nTransfer ended");
        }

        private string ToCsv(PvSample s)
        {
            return $"{s.RowIndex},{s.Day},{s.Hour},{s.AcPwrt},{s.DcVolt},{s.Temper},{s.Vl1to2},{s.Vl2to3},{s.Vl3to1},{s.AcCur1},{s.AcVlt1}";
        }

        public (bool isValid, string reason) ValidateSample(PvSample s)
        {
            
            if (s.AcPwrt == 32767.0) { LoggerService.Fault("AcPwrt sentinel detected"); s.AcPwrt = null; }
            if (s.DcVolt == 32767.0) { LoggerService.Warning("DcVolt sentinel detected"); s.DcVolt = null; }
            if (s.Temper == 32767.0) { LoggerService.Warning("Temper sentinel detected"); s.Temper = null; }

            if (s.Vl1to2 == 32767.0) { LoggerService.Warning("Vl1to2 sentinel detected"); s.Vl1to2 = null; }
            if (s.Vl2to3 == 32767.0) { LoggerService.Warning("Vl2to3 sentinel detected"); s.Vl2to3 = null; }
            if (s.Vl3to1 == 32767.0) { LoggerService.Warning("Vl3to1 sentinel detected"); s.Vl3to1 = null; }

            if (s.AcCur1 == 32767.0) { LoggerService.Warning("AcCur1 sentinel detected"); s.AcCur1 = null; }
            if (s.AcVlt1 == 32767.0) { LoggerService.Warning("AcVlt1 sentinel detected"); s.AcVlt1 = null; }

            bool isValid = true;
            string reason = string.Empty;

            
            if (s.AcPwrt.HasValue && s.AcPwrt < 0)
            {
                reason = "AcPwrt must be ≥ 0!";
                LoggerService.Error(reason);
                isValid = false;
            }

            if (s.DcVolt.HasValue && s.DcVolt <= 0)
            {
                reason = "DcVolt must be > 0!";
                LoggerService.Error(reason);
                isValid = false;
            }

            if (s.Vl1to2.HasValue && s.Vl1to2 <= 0)
            {
                reason = "Vl1to2 must be > 0!";
                LoggerService.Error(reason);
                isValid = false;
            }

            if (s.Vl2to3.HasValue && s.Vl2to3 <= 0)
            {
                reason = "Vl2to3 must be > 0!";
                LoggerService.Error(reason);
                isValid = false;
            }

            if (s.Vl3to1.HasValue && s.Vl3to1 <= 0)
            {
                reason = "Vl3to1 must be > 0!";
                LoggerService.Error(reason);
                isValid = false;
            }

            if (s.AcVlt1.HasValue && s.AcVlt1 <= 0)
            {
                reason = "AcVlt1 must be > 0!";
                LoggerService.Error(reason);
                isValid = false;
            }

            if (s.RowIndex <= _lastRowIndex)
            {
                reason = $"RowIndex not monotonic: {s.RowIndex}";
                LoggerService.Error(reason);
                isValid = false;
            }

            _lastRowIndex = s.RowIndex;

    

            if (s.Temper.HasValue && s.Temper > _overTempThreshold)
            {
                LoggerService.Warning("EVENT: Temperature exceeded threshold");
                reason += "EVENT: Temperature exceeded threshold; ";
                
            }

            if (s.AcCur1.HasValue && s.AcCur1 > _acCur1SpikeThreshold)
            {
                LoggerService.Warning("EVENT: AcCur1 spike detected");
                reason += "EVENT: AcCur1 spike detected; ";
                
            }

            if (s.Vl1to2.HasValue && s.Vl2to3.HasValue && s.Vl3to1.HasValue)
            {
                double avg = (s.Vl1to2.Value + s.Vl2to3.Value + s.Vl3to1.Value) / 3;

                double maxDiff =
                    Math.Max(
                        Math.Max(
                            Math.Abs(s.Vl1to2.Value - avg),
                            Math.Abs(s.Vl2to3.Value - avg)),
                        Math.Abs(s.Vl3to1.Value - avg));

                double imbalancePercent = (maxDiff / avg) * 100;

                if (imbalancePercent > _voltageImbalancePct)
                {
                    LoggerService.Warning("EVENT: Voltage imbalance detected");
                    reason += "EVENT: Voltage imbalance detected; ";
                   
                }
            }

            if (s.AcPwrt.HasValue)
            {
                if (_previousPower.HasValue && s.AcPwrt == _previousPower)
                {
                    _flatlineCounter++;
                }
                else
                {
                    _flatlineCounter = 0;
                }

                _previousPower = s.AcPwrt;

                if (_flatlineCounter >= _powerFlatlineWindow)
                {
                    LoggerService.Warning("EVENT: Power flatline detected");
                    reason += "EVENT: Power flatline detected; ";
                    
                }
            }

            return (isValid, reason);
        }

        private void EnsureWriters()
        {
            if (_sessionWriter == null || IsClosed(_sessionWriter))
            {
                _sessionWriter = new StreamWriter(_sessionPath, true);
            }

            if (_rejectWriter == null || IsClosed(_rejectWriter))
            {
                _rejectWriter = new StreamWriter(_rejectPath, true);
            }
        }

        private bool IsClosed(StreamWriter w)
        {
            try
            {
                return w.BaseStream == null || !w.BaseStream.CanWrite;
            }
            catch
            {
                return true;
            }
        }

        ~PvService()
        {
            Dispose(false);
        }
        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }
        protected virtual void Dispose(bool disposing)
        {
            if (!disposed)
            {
                if (disposing)
                {
                    _sessionWriter?.Flush();
                    _sessionWriter?.Close();

                    _rejectWriter?.Flush();
                    _rejectWriter?.Close();

                    if (_manualDispose)
                    {
                        LoggerService.Info("Resources disposed");
                    }
                }
                disposed = true;
            }
        }
    }
}
