using Client;
using Common;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.IO;
using System.ServiceModel;

namespace Tests
{
    [TestClass]
    public class DisposeTests
    {
        [TestMethod]
        public void Dispose_ShouldReleaseResources()
        {
            var p = new Program();

            File.WriteAllText("test.csv", "DAY,HOUR,ACPWRT,DCVOLT,TEMPER,VL1TO2,VL2TO3,VL3TO1,ACCUR1,ACVLT1\n" + "2023163,12:00:00,100,200,30,10,10,10,5,220\n");

            p.ParseCsv("test.csv", 1);
            p.Dispose();
            
            try
            {
                using (var fs = new FileStream("rejected_client.csv", FileMode.OpenOrCreate, FileAccess.ReadWrite))
                {
                    Assert.IsTrue(fs.CanWrite, "File is still locked - resources not released");
                }
            }
            catch
            {
                Assert.Fail("File is locked → Dispose did NOT release resources");
            }
        }
    }
}
