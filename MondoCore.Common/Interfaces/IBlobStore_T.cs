/*************************************************************************** 
 *                                                                           
 *    The MondoCore Libraries  	                                             
 *                                                                           
 *      Namespace: MondoCore.Common	                                         
 *           File: IBlobStore_T.cs                                             
 *      Class(es): IBlobStore<out T>                                                
 *        Purpose: Generic interface for storing blobs                       
 *                                                                           
 *  Original Author: Jim Lightfoot                                           
 *    Creation Date: 3 Fed 2026                                             
 *                                                                           
 *   Copyright (c) 2026 - Jim Lightfoot, All rights reserved            
 *                                                                           
 *  Licensed under the MIT license:                                          
 *    http://www.opensource.org/licenses/mit-license.php                     
 *                                                                           
 ****************************************************************************/

using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace MondoCore.Common
{
    /****************************************************************************/
    /****************************************************************************/
    /// <summary>
    /// Interface for blob storage
    /// </summary>
    public interface IBlobStore<out T> : IAsyncDisposable
    {
        /// <summary>
        /// Returns an interface to perform read only operations on the blob store
        /// </summary>
        IBlobStoreReader<T> Reader { get; }

        /// <summary>
        /// Returns an interface to perform write only operations on the blob store
        /// </summary>
        IBlobStoreWriter<T> Writer { get; }
    }

    /****************************************************************************/
    /****************************************************************************/
    /// <summary>
    /// Perform read only operations on the blob store
    /// </summary>
    public interface IBlobStoreReader<out T> 
    {
        /// <summary>
        /// Gets a blob with the given id/path
        /// </summary>
        /// <param name="id">An identifier for the blob. This could be a path.</param>
        /// <param name="destination">Destination stream to write blob to</param>
        /// <param name="cancellationToken">A cancellation token</param>
        Task Get(string id, Stream destination, CancellationToken cancellationToken = default);

        /// <summary>
        /// Opens a readonly stream to a blob with the given id/path 
        /// </summary>
        /// <param name="id">An identifier for the blob. This could be a path.</param>
        /// <param name="cancellationToken">A cancellation token</param>
        /// <returns>A readonly stream to read the blob from</returns>
        Task<Stream> OpenRead(string id, CancellationToken cancellationToken = default);

        /// <summary>
        /// Finds all blobs that meet the filter 
        /// </summary>
        /// <param name="filter">A file path type filter, e.g. "Policies*.*"</param>
        /// <param name="cancellationToken">A cancellation token</param>
        /// <returns>A collection of the blob ids/paths</returns>
        Task<IEnumerable<string>> Find(string filter, CancellationToken cancellationToken = default);

        /// <summary>
        /// Enumerates on each blob and calls the given function for each
        /// </summary>
        /// <param name="filter">A file path type filter, e.g. "Policies*.*"</param>
        /// <param name="fnEach">A function to call with each blob</param>
        /// <param name="asynchronous">Specify if the enumeration is asynchronous</param>
        /// <param name="cancellationToken">A cancellation token</param>
        Task Enumerate(string filter, Func<IBlob, Task> fnEach, bool asynchronous = true, CancellationToken cancellationToken = default);

        /// <summary>
        /// Determinse if blob exists
        /// </summary>
        /// <param name="id">An identifier for the blob. This could be a path.</param>
        /// <param name="cancellationToken">A cancellation token</param>
        Task<bool> Exists(string id, CancellationToken cancellationToken = default);

        /// <summary>
        /// Returns an IASyncEnumerable that enumerates on blobs in the blob store
        /// </summary>
        /// <param name="prefix">Enumerates only blobs whose ids start with this prefix (if specified)</param>
        /// <param name="cancellationToken">A cancellation token</param>
        IAsyncEnumerable<IBlob> AsAsyncEnumerable(string? prefix = null, CancellationToken cancellationToken = default);

        #region Default methods

        /// <summary>
        /// Gets a blob with the given id/path
        /// </summary>
        /// <param name="id">An identifier for the blob. This could be a path in file storage for instance</param>
        /// <param name="encoding">A text encoding to use to encode the text</param>
        /// <param name="cancellationToken">A cancellation token</param>
        /// <ret urns>A string that is the blob</returns>
        public async Task<string> Get(string id, Encoding? encoding = null, CancellationToken cancellationToken = default)
        {
            encoding = encoding ?? UTF8Encoding.UTF8;

            var bytes = await this.GetBytes(id, cancellationToken);
            var stripped = bytes.StripNulls();

            return encoding.GetString(stripped.Bytes, 0, stripped.Length);
        }

        /// <summary>
        /// Gets a blob with the given id/path
        /// </summary>
        /// <param name="id">An identifier for the blob. This could be a path in file storage for instance</param>
        /// <param name="cancellationToken">A cancellation token</param>
        /// <returns>The blob as an array of bytes</returns>
        public async Task<byte[]> GetBytes(string id, CancellationToken cancellationToken = default)
        {
            await using var memStream = new MemoryStream();
           
            await this.Get(id, memStream, cancellationToken);

            return memStream.ToArray();
        }

        #endregion
    }

    /****************************************************************************/
    /****************************************************************************/
    /// <summary>
    /// Perform write only operations on the blob store
    /// </summary>
    public interface IBlobStoreWriter<out T> 
    {
        /// <summary>
        /// Puts the stream into the blob storage
        /// </summary>
        /// <param name="id">An identifier for the blob. This could be a path in file storage for instance</param>
        /// <param name="lease">A lease (lock) on a specific blob. Must be the result of AcquireLease</param>
        /// <param name="cancellationToken">A cancellation token</param>
        /// <param name="content">The content to store</param>
        Task Put(string id, Stream content, CancellationToken cancellationToken = default);

        /// <summary>
        /// Deletes the blob from storage
        /// </summary>
        /// <param name="id">An identifier for the blob. This could be a path in file storage for instance</param>
        /// <param name="cancellationToken">A cancellation token</param>
        Task Delete(string id, CancellationToken cancellationToken = default);

        /// <summary>
        /// Opens a writable stream to a blob with the given id/path 
        /// </summary>
        /// <param name="id">An identifier for the blob. This could be a path.</param>
        /// <param name="cancellationToken">A cancellation token</param>
        /// <returns>A writable stream to write to the blob</returns>
        Task<Stream> OpenWrite(string id, CancellationToken cancellationToken = default);

        /// <summary>
        /// Creates a lease (lock) on a blob to be used for subsequent write operations
        /// </summary>
        /// <param name="cancellationToken">A cancellation token</param>
        Task<IBlobLease> AcquireLease(string id, bool createIfNotExists = true, CancellationToken cancellationToken = default);

        #region Default methods

        /// <summary>
        /// Puts the string into the blob storage
        /// </summary>
        /// <param name="id">An identifier for the blob. This could be a path in file storage for instance</param>
        /// <param name="content">The string to store</param>
        /// <param name="cancellationToken">A cancellation token</param>
        public async Task Put(string id, string content, Encoding? encoding = null, CancellationToken cancellationToken = default)
        {
            encoding = encoding ?? UTF8Encoding.UTF8;

            var bytes = encoding.GetBytes(content);
            var stripped = bytes.StripNulls();

            var stream = new MemoryStream(stripped.Bytes, 0, stripped.Length);
            
             await this.Put(id, stream, cancellationToken);
        }

        #endregion
    }

    public interface IBlobLease : IAsyncDisposable
    {
        public string BlobId   { get; }
        public string LeaseId  { get; }

        /// <summary>
        /// Puts the stream into the blob storage
        /// </summary>
        /// <param name="cancellationToken">A cancellation token</param>
        /// <param name="content">The content to store</param>
        Task Put(Stream content, CancellationToken cancellationToken = default);

        /// <summary>
        /// Deletes the blob from storage
        /// </summary>
        /// <param name="cancellationToken">A cancellation token</param>
        Task Delete(CancellationToken cancellationToken = default);

        /// <summary>
        /// Opens a writable stream to a blob with the given id/path 
        /// </summary>
        /// <param name="cancellationToken">A cancellation token</param>
        /// <returns>A writable stream to write to the blob</returns>
        Task<Stream> OpenWrite(CancellationToken cancellationToken = default);

        #region Default methods

        /// <summary>
        /// Puts the string into the blob storage
        /// </summary>
        /// <param name="content">The string to store</param>
        /// <param name="cancellationToken">A cancellation token</param>
        public async Task Put(string content, Encoding? encoding = null, CancellationToken cancellationToken = default)
        {
            encoding = encoding ?? UTF8Encoding.UTF8;

            var bytes = encoding.GetBytes(content);
            var stripped = bytes.StripNulls();

            var stream = new MemoryStream(stripped.Bytes, 0, stripped.Length);
            
             await this.Put(stream, cancellationToken);
        }

        #endregion
    }

    public class LeaseException(Exception innerException) : Exception("There is an existing lease on this blob", innerException)
    {
            
    }
}
