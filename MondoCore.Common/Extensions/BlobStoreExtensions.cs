using System;
using System.IO;
using System.Text;
using System.Threading.Tasks;

namespace MondoCore.Common
{
    public static class BlobStoreExtensions
    {
        /// <summary>
        /// Gets a blob with the given id/path
        /// </summary>
        /// <param name="id">An identifier for the blob. This could be a path in file storage for instance</param>
        /// <param name="encoding">A text encoding to use to encode the text</param>
        /// <returns>A string that is the blob</returns>
        public static async Task<string> Get(this IBlobStore store, string id, Encoding? encoding = null)
        {
            encoding = encoding ?? UTF8Encoding.UTF8;

            return encoding.GetString(await store.GetBytes(id));
        }

        /// <summary>
        /// Gets a blob with the given id/path
        /// </summary>
        /// <param name="id">An identifier for the blob. This could be a path in file storage for instance</param>
        /// <returns>The blob as an array of bytes</returns>
        public static async Task<byte[]> GetBytes(this IBlobStore store, string id)
        {
            using(var memStream = new MemoryStream())
            {
                await store.Get(id, memStream);

                return memStream.ToArray();
            }
        }

        /// <summary>
        /// Puts the string into the blob storage
        /// </summary>
        /// <param name="id">An identifier for the blob. This could be a path in file storage for instance</param>
        /// <param name="content">The string to store</param>
        public static Task Put(this IBlobStore store, string id, string content, Encoding? encoding = null)
        {
            encoding = encoding ?? UTF8Encoding.UTF8;

            using var stream = new MemoryStream(encoding.GetBytes(content));

            return store.Put(id, stream);
        }


    }
}
