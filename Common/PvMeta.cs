using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Runtime.Serialization;

namespace Common
{
    [DataContract]
    public class PvMeta
    {
        [DataMember]
        public string FileName { get; set; }

        [DataMember]
        public int TotalRows { get; set; }

        [DataMember]
        public string SchemaVersion { get; set; }

        [DataMember]
        public int RowLimitN { get; set; }
    }
}
