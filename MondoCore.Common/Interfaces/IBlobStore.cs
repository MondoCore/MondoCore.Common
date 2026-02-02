/*************************************************************************** 
 *                                                                           
 *    The MondoCore Libraries  	                                             
 *                                                                           
 *      Namespace: MondoCore.Common	                                         
 *           File: IBlobStore.cs                                             
 *      Class(es): IBlobStore                                                
 *        Purpose: Generic interface for storing blobs                       
 *                                                                           
 *  Original Author: Jim Lightfoot                                           
 *    Creation Date: 29 Nov 2015                                             
 *                                                                           
 *   Copyright (c) 2015-2026 - Jim Lightfoot, All rights reserved            
 *                                                                           
 *  Licensed under the MIT license:                                          
 *    http://www.opensource.org/licenses/mit-license.php                     
 *                                                                           
 ****************************************************************************/

using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Text;
using System.Threading.Tasks;

namespace MondoCore.Common
{
    /****************************************************************************/
    /****************************************************************************/
    /// <summary>
    /// Interface for blob storage
    /// </summary>
    public interface IBlobStore : IDisposable
    {
        /// <summary>
        /// Writes a blob with the given id/path to the given stream
        /// </summary>
        /// <param name="id">An identifier for the blob. This could be a path.</param>
        /// <param name="destination">Destination stream to write blob to</param>
        Task Get(string id, Stream destination);

        /// <summary>
        /// Opens a readonly stream to a blob with the given id/path 
        /// </summary>
        /// <param name="id">An identifier for the blob. This could be a path.</param>
        /// <returns>A readonly stream to read the blob from</returns>
        Task<Stream> OpenRead(string id);

        /// <summary>
        /// Opens a writable stream to a blob with the given id/path 
        /// </summary>
        /// <param name="id">An identifier for the blob. This could be a path.</param>
        /// <returns>A writable stream to write to the blob</returns>
        Task<Stream> OpenWrite(string id);

        /// <summary>
        /// Puts the stream into the blob storage
        /// </summary>
        /// <param name="id">An identifier for the blob. This could be a path in file storage for instance</param>
        /// <param name="content">The content to store</param>
        Task Put(string id, Stream content);

        /// <summary>
        /// Deletes the blob from storage
        /// </summary>
        /// <param name="id">An identifier for the blob. This could be a path in file storage for instance</param>
        Task Delete(string id);

        /// <summary>
        /// Finds all blobs that meet the filter 
        /// </summary>
        /// <param name="filter">A file path type filter, e.g. "Policies*.*"</param>
        /// <returns>A collection of the blob ids/paths</returns>
        Task<IEnumerable<string>> Find(string filter);

        /// <summary>
        /// Enumerates on each blob and calls the given function for each
        /// </summary>
        /// <param name="filter">A file path type filter, e.g. "Policies*.*"</param>
        /// <param name="fnEach">A function to call with each blob</param>
        /// <returns></returns>
        Task Enumerate(string filter, Func<IBlob, Task> fnEach, bool asynchronous = true);

        /// <summary>
        /// Locks a blob and prevents updating or deletion
        /// </summary>
        /// <param name="id">An identifier for the blob. This could be a path.</param>
        Task<IDisposable> Lock(string id);

        /// <summary>
        /// Determinse if blob exists
        /// </summary>
        /// <param name="id">An identifier for the blob. This could be a path.</param>
        Task<bool> Exists(string id);

        IAsyncEnumerable<IBlob> AsAsyncEnumerable();

        #region Default methods

        /// <summary>
        /// Gets a blob with the given id/path
        /// </summary>
        /// <param name="id">An identifier for the blob. This could be a path in file storage for instance</param>
        /// <param name="encoding">A text encoding to use to encode the text</param>
        /// <ret urns>A string that is the blob</returns>
        public async Task<string> Get(string id, Encoding? encoding = null)
        {
            encoding = encoding ?? UTF8Encoding.UTF8;

            var bytes = await this.GetBytes(id);
            var stripped = bytes.StripNulls();

            return encoding.GetString(stripped.Bytes, 0, stripped.Length);
        }

        /// <summary>
        /// Gets a blob with the given id/path
        /// </summary>
        /// <param name="id">An identifier for the blob. This could be a path in file storage for instance</param>
        /// <returns>The blob as an array of bytes</returns>
        public async Task<byte[]> GetBytes(string id)
        {
            using(var memStream = new MemoryStream())
            {
                await this.Get(id, memStream);

                return memStream.ToArray();
            }
        }

        /// <summary>
        /// Puts the string into the blob storage
        /// </summary>
        /// <param name="id">An identifier for the blob. This could be a path in file storage for instance</param>
        /// <param name="content">The string to store</param>
        public async Task Put(string id, string content, Encoding? encoding = null)
        {
            encoding = encoding ?? UTF8Encoding.UTF8;

            var bytes = encoding.GetBytes(content);
            var stripped = bytes.StripNulls();

            using(var stream = new MemoryStream(stripped.Bytes, 0, stripped.Length))
            { 
                await this.Put(id, stream);
            }
        }

        #endregion
    }

    public interface IBlob
    {
        string                       Name            { get; }
        bool                         Deleted         { get; }
        bool                         Enabled         { get; }
        string?                      Version         { get; }
        string                       ContentType     { get; }
        DateTimeOffset?              Expires         { get; }
        IDictionary<string, string>? Metadata        { get; }
        IDictionary<string, string>? Tags            { get; }
    }
}
