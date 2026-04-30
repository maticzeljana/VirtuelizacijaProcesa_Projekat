using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.Serialization;
using System.Text;
using System.Threading.Tasks;

namespace Common
{
    [DataContract]
    public class PvSample
    {
        [DataMember]
        public int Day { get; set; }

        [DataMember]
        public int Hour { get; set; }

        [DataMember]
        public double? AcPwrt { get; set; }

        [DataMember]
        public double? DcVolt { get; set; }

        [DataMember]
        public double? Temper { get; set; }

        [DataMember]
        public double? Vl1to2 { get; set; }

        [DataMember]
        public double? Vl2to3 { get; set; }

        [DataMember]
        public double? Vl3to1 { get; set; }

        [DataMember]
        public double? AcCur1 { get; set; }

        [DataMember]
        public double? AcVlt1 { get; set; }

        [DataMember]
        public int RowIndex { get; set; }
    }
}
