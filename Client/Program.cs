using Common;
using System;
using System.Collections.Generic;
using System.Linq;
using System.ServiceModel;
using System.Text;
using System.Threading.Tasks;

namespace Client
{
    public class Program
    {
        static void Main(string[] args)
        {
            ChannelFactory<IPvService> factory = new ChannelFactory<IPvService>("PvService");

            IPvService proxy = factory.CreateChannel();
        }
    }
}
