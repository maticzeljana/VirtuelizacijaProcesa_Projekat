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
        #region Fields
        private StreamWriter _sessionWriter;
        private StreamWriter _rejectWriter;
        private static string _sessionPath;
        private static string _rejectPath;
        private string _basePath;

        private readonly double _overTempThreshold = double.Parse(ConfigurationManager.AppSettings["OverTempThreshold"]);
        private readonly double _voltageImbalancePct = double.Parse(ConfigurationManager.AppSettings["VoltageImbalancePct"]);
        private readonly int _powerFlatlineWindow = int.Parse(ConfigurationManager.AppSettings["PowerFlatlineWindow"]);
        private readonly double _acCur1SpikeThreshold = double.Parse(ConfigurationManager.AppSettings["AcCur1SpikeThreshold"]);
        private readonly double _dcVoltMin = double.Parse(ConfigurationManager.AppSettings["DcVoltMin"]);
        private readonly double _dcVoltMax = double.Parse(ConfigurationManager.AppSettings["DcVoltMax"]);

        private double? _previousPower = null;
        private int _flatlineCounter = 0;
        private double? _previousAcCur1 = null;
        private double _acCur1Mean = 0;
        private int _acCur1Count = 0;

        private int _lastRowIndex = -1;
        private static int _totalRows;
        private static int _receivedRows;

        private bool disposed = false;
        private bool _manualDispose = false;
        #endregion
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

            _receivedRows++;
            double percent = (double)_receivedRows / _totalRows * 100;
            Console.Write($"\rTransfer status(valid samples percent): {_receivedRows}/{_totalRows} ({percent:0}%)   ");

            if (result.isValid)
            {
                _sessionWriter.WriteLine(ToCsv(sample));
                _sessionWriter.Flush();

                return new SampleResult
                {
                    IsValid = true,
                    Message = !string.IsNullOrWhiteSpace(result.reason) ? result.reason : $"Sample {sample.RowIndex} processed"
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

        #region Helpers
        public (bool isValid, string reason) ValidateSample(PvSample s)
        {
            #region sentinel
            if (s.AcPwrt == 32767.0) { LoggerService.Fault("AcPwrt sentinel detected"); s.AcPwrt = null; }
            if (s.DcVolt == 32767.0) { LoggerService.Warning("DcVolt sentinel detected"); s.DcVolt = null; }
            if (s.Temper == 32767.0) { LoggerService.Warning("Temper sentinel detected"); s.Temper = null; }

            if (s.Vl1to2 == 32767.0) { LoggerService.Warning("Vl1to2 sentinel detected"); s.Vl1to2 = null; }
            if (s.Vl2to3 == 32767.0) { LoggerService.Warning("Vl2to3 sentinel detected"); s.Vl2to3 = null; }
            if (s.Vl3to1 == 32767.0) { LoggerService.Warning("Vl3to1 sentinel detected"); s.Vl3to1 = null; }

            if (s.AcCur1 == 32767.0) { LoggerService.Warning("AcCur1 sentinel detected"); s.AcCur1 = null; }
            if (s.AcVlt1 == 32767.0) { LoggerService.Warning("AcVlt1 sentinel detected"); s.AcVlt1 = null; }
            #endregion

            #region errors
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
            #endregion

            #region analytics
            #region Temperature Analytics
            if (s.Temper.HasValue && s.Temper > _overTempThreshold)
            {
                LoggerService.Warning("EVENT: OverTempWarning");
                reason += $"OverTempWarning (value={s.Temper:0.00}); ";
            }
            #endregion

            #region AcCur1 Analytics
            if (s.AcCur1.HasValue && _previousAcCur1.HasValue)
            {
                double delta = s.AcCur1.Value - _previousAcCur1.Value;

                if (Math.Abs(delta) > _acCur1SpikeThreshold)
                {
                    string direction = delta > 0 ? "UP" : "DOWN";
                    LoggerService.Warning("EVENT: AcCur1 spike detected");
                    reason += $"CurrentSpikeWarning ({direction}, delta={delta:0.00})";
                }
            }

            if (s.AcCur1.HasValue && _acCur1Count > 1)
            {
                double lower = 0.80 * _acCur1Mean;
                double upper = 1.20 * _acCur1Mean;

                if (s.AcCur1.Value < lower || s.AcCur1.Value > upper)
                {
                    LoggerService.Warning("EVENT: CurrentOutOfBandWarning");
                    reason += $"EVENT: CurrentOutOfBandWarning (value={s.AcCur1.Value:0.00}, mean={_acCur1Mean:0.00})";
                }
            }

            if (s.AcCur1.HasValue)
            {
                _acCur1Mean = ((_acCur1Mean * _acCur1Count) + s.AcCur1.Value) / (_acCur1Count + 1);
                _acCur1Count++;
            }

            if (s.AcCur1.HasValue)
            {
                _previousAcCur1 = s.AcCur1.Value;
            }
            #endregion

            #region DcVolt Analytics
            if (s.DcVolt.HasValue)
            {
                if (s.DcVolt.Value < _dcVoltMin || s.DcVolt.Value > _dcVoltMax)
                {
                    LoggerService.Warning("EVENT: DcVoltOutOfRangeWarning");
                    reason += $"DcVoltOutOfRangeWarning (value={s.DcVolt.Value:0.00});";
                }
            }
            #endregion

            #region Voltage Imbalance Analytics
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
                    reason += "Voltage imbalance detected; ";
                }
            }
            #endregion

            #region Power Flatline Analytics
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
                    reason += "Power flatline detected; ";
                }
            }
            #endregion
            #endregion
            return (isValid, reason);
        }

        private string ToCsv(PvSample s)
        {
            return $"{s.RowIndex},{s.Day},{s.Hour},{s.AcPwrt},{s.DcVolt},{s.Temper},{s.Vl1to2},{s.Vl2to3},{s.Vl3to1},{s.AcCur1},{s.AcVlt1}";
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
        #endregion

        #region Dispose Pattern
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
        #endregion
    }
}
