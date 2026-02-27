using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace MondoCore.Common.UnitTests
{
    [TestClass]
    [TestCategory("Unit Tests")]
    public class ExceptionExtensionsTests
    {
        [TestMethod]
        public void ExceptionExtensions_WithData()
        {
            try
            { 
                throw new ArgumentException("Make should be a Pontiac").WithData( new { Make = "Chevy"} );
            }
            catch (Exception ex) 
            { 
                Assert.Contains("Make", ex.Data.Keys);
                Assert.AreEqual("Chevy", ex.Data["Make"]);
            }
        }

        [TestMethod]
        public void ExceptionExtensions_WithData_existing()
        {
            try
            { 
                var ex = new ArgumentException("Car should be a Pontiac Firebird").WithData( new { Make = "Chevy", Model = "Corvette"} );

                throw ex.WithData( new { Make = "Chevy"} );
            }
            catch (Exception ex) 
            { 
                Assert.Contains("Make", ex.Data.Keys);
                Assert.AreEqual("Chevy", ex.Data["Make"]);

                Assert.Contains("Model", ex.Data.Keys);
                Assert.AreEqual("Corvette", ex.Data["Model"]);
            }
        }
    }
}
