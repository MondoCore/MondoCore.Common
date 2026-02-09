using Microsoft.VisualStudio.TestTools.UnitTesting;
using MondoCore.Common;
using Moq;
using System.IO;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;

namespace MondoCore.Common.UnitTests
{
    [TestClass]
    [TestCategory("Unit Tests")]
    public class StreamExtensions
    {
        private const string data = "This once recognized, our attention naturally turns to the question of how small the payment can be. We know that by means of a wheel and axle we can raise 1,000 kilogrammes through 1 metre by allowing 100 kilogrammes to fall 10 metres.";

        [TestMethod]
        public async Task StreamExtensions_WriteAsync_string()
        {
            using var input = new MemoryStream();

            await input.WriteAsync(data);

            var result = await input.ReadStringAsync();

            Assert.AreEqual(data, result);
        }

        [TestMethod]
        public async Task StreamExtensions_ReadString()
        {
            using var input = new MemoryStream();

            await input.WriteAsync(data);

            var result = input.ReadString();

            Assert.AreEqual(data, result);
        }

        [TestMethod]
        public async Task StreamExtensions_WriteAsync_stream()
        {
            using var input = new MemoryStream();
            using var output = new MemoryStream();

            // Initialize input with some data
            await input.WriteAsync(data);
            input.Seek(0, SeekOrigin.Begin);

            // Write input stream to output stream
            await input.CopyToAsync(output);

            var result = await output.ReadStringAsync();

            Assert.AreEqual(data, result);
        }

        [TestMethod]
        public async Task StreamExtensions_WriteAsync_stream_2()
        {
            using var input = new MemoryStream();
            using var output = new MemoryStream();

            // Initialize input with some data (> buffer size which is 64k)
            for(var i = 0; i < 600; ++i)
                await input.WriteAsync(data);

            input.Seek(0, SeekOrigin.Begin);

            // Write input stream to output stream
            await input.CopyToAsync(output);

            var result = await output.ReadStringAsync();

            Assert.AreEqual(data.Length * 600, result.Length);
            Assert.AreEqual(data, result.Substring(0, data.Length));
        }

        [TestMethod]
        public async Task StreamExtensions_ReadObject()
        {
            await using var memStream = new MemoryStream(Encoding.UTF8.GetBytes( JsonSerializer.Serialize(new Car { Make = "Chevy", Model = "Camaro", Year = 1969, Color = "Blue"})));

            var car = await memStream.ReadObject<Car>();

            Assert.AreEqual("Chevy",  car.Make);
            Assert.AreEqual("Camaro", car.Model);
            Assert.AreEqual(1969,     car.Year);
            Assert.AreEqual("Blue",   car.Color);
        }

        [TestMethod]
        public async Task StreamExtensions_WriteObject()
        {
            await using var memStream = new MemoryStream();

            await memStream.WriteObject(new Car { Make = "Chevy", Model = "Camaro", Year = 1969, Color = "Blue"});

            var car = await memStream.ReadObject<Car>();

            Assert.AreEqual("Chevy",  car.Make);
            Assert.AreEqual("Camaro", car.Model);
            Assert.AreEqual(1969,     car.Year);
            Assert.AreEqual("Blue",   car.Color);
        }
    }
}
