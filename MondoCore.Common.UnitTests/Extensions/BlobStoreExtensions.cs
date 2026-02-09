using Microsoft.VisualStudio.TestTools.UnitTesting;

using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;

using MondoCore.Common;

using Moq;
using System.IO;

namespace MondoCore.Common.UnitTests
{
    [TestClass]
    [TestCategory("Unit Tests")]
    public class BlobStoreExtensionsTests
    {
        [TestMethod]
        public async Task BlobStoreExtensions_GetObject()
        {
            var reader = new Mock<IBlobStoreReader<Car>>();

            await using var memStream = new MemoryStream(Encoding.UTF8.GetBytes( JsonSerializer.Serialize(new Car { Make = "Chevy", Model = "Camaro", Year = 1969, Color = "Blue"})));

            reader.Setup( r=> r.Get("bob", It.IsAny<Stream>(), CancellationToken.None)).Callback(async (string id, Stream stream, CancellationToken token)=> 
            {
                await memStream.CopyToAsync(stream);
            });

            var car = await reader.Object.GetObject<Car>("bob");

            Assert.AreEqual("Chevy",  car.Make);
            Assert.AreEqual("Camaro", car.Model);
            Assert.AreEqual(1969,     car.Year);
            Assert.AreEqual("Blue",   car.Color);
        }

        [TestMethod]
        public async Task BlobStoreExtensions_PutObject()
        {
            var writer = new Mock<IBlobStoreWriter<Car>>();

            await using var memStream = new MemoryStream();

            writer.Setup( r=> r.Put("bob", It.IsAny<Stream>(), CancellationToken.None)).Callback(async (string id, Stream stream, CancellationToken token)=> 
            {
                stream.Seek(0, SeekOrigin.Begin);
                await stream.CopyToAsync(memStream, token);
            });

            await writer.Object.PutObject<Car>("bob", new Car { Make = "Chevy", Model = "Camaro", Year = 1969, Color = "Blue"});

            memStream.Seek(0, SeekOrigin.Begin);

            var car = await JsonSerializer.DeserializeAsync<Car>(memStream);

            Assert.AreEqual("Chevy",  car.Make);
            Assert.AreEqual("Camaro", car.Model);
            Assert.AreEqual(1969,     car.Year);
            Assert.AreEqual("Blue",   car.Color);
        }
    }
}
