using Common;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Service
{
    public class PvService : IPvService
    {
        private int _lastRowIndex = -1;
        public void StartSession(PvMeta meta)
        {
        }

        public void PushSample(PvSample sample)
        {
        }
        public void EndSession()
        {
        }

        public void ValidateSample(PvSample s)
        {
            if (s.AcPwrt == 32767.0) { LoggerService.Fault("AcPwrt sentinel"); s.AcPwrt = null; }
            if (s.DcVolt == 32767.0) { LoggerService.Warning("DcVolt sentinel"); s.DcVolt = null; }
            if (s.Temper == 32767.0) { LoggerService.Warning("Temper sentinel"); s.Temper = null; }

            if (s.Vl1to2 == 32767.0) { LoggerService.Warning("Vl1to2 sentinel"); s.Vl1to2 = null; }
            if (s.Vl2to3 == 32767.0) { LoggerService.Warning("Vl2to3 sentinel"); s.Vl2to3 = null; }
            if (s.Vl3to1 == 32767.0) { LoggerService.Warning("Vl3to1 sentinel"); s.Vl3to1 = null; }

            if (s.AcCur1 == 32767.0) { LoggerService.Warning("AcCur1 sentinel"); s.AcCur1 = null; }
            if (s.AcVlt1 == 32767.0) { LoggerService.Warning("AcVlt1 sentinel"); s.AcVlt1 = null; }


            if (s.AcPwrt.HasValue && s.AcPwrt < 0)
                LoggerService.Error("AcPwrt must be ≥ 0");

            if (s.DcVolt.HasValue && s.DcVolt <= 0)
                LoggerService.Error("DcVolt must be > 0");

            if (s.Vl1to2.HasValue && s.Vl1to2 <= 0)
                LoggerService.Error("Vl1to2 must be > 0");

            if (s.Vl2to3.HasValue && s.Vl2to3 <= 0)
                LoggerService.Error("Vl2to3 must be > 0");

            if (s.Vl3to1.HasValue && s.Vl3to1 <= 0)
                LoggerService.Error("Vl3to1 must be > 0");

            if (s.AcVlt1.HasValue && s.AcVlt1 <= 0)
                LoggerService.Error("AcVlt1 must be > 0");

            if (s.RowIndex <= _lastRowIndex)
                LoggerService.Error($"RowIndex not monotonic: {s.RowIndex}");

            _lastRowIndex = s.RowIndex;
        }
    }
}
