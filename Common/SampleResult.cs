using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Runtime.Serialization;

namespace Common
{
    [DataContract]
    public class SampleResult
    {
        [DataMember]
        public bool IsValid {  get; set; }

        [DataMember]
        public string Message {  get; set; }
    }
}
