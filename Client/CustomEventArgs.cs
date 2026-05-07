using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Client
{
    public class CustomEventArgs:EventArgs
    {
        public string Message {  get; set; }
        public CustomEventArgs(string message) {  this.Message = message; }
    }
}
