/***************************************************************************
 *                                                                          
 *    The MondoCore Libraries  							                    
 *                                                                          
 *        Namespace: MondoCore.Common							            
 *             File: BlobStoreExtensions.cs					    		        
 *        Class(es): BlobStoreExtensions				         		            
 *          Purpose: Extensions for IBlobStore<T>                 
 *                                                                          
 *  Original Author: Jim Lightfoot                                          
 *    Creation Date: 8 Feb 2026                                             
 *                                                                          
 *   Copyright (c) 2026 - Jim Lightfoot, All rights reserved           
 *                                                                          
 *  Licensed under the MIT license:                                         
 *    http://www.opensource.org/licenses/mit-license.php                    
 *                                                                          
 ****************************************************************************/

using System.Text.Json;
using System.Threading.Tasks;
using System.Threading;
using System.IO;
using System.Text;

namespace MondoCore.Common
{
    /****************************************************************************/
    /****************************************************************************/
    public static class BlobStoreExtensions
    {
        /// <summary>
        /// Loads an object from a blob with the given id/path
        /// </summary>
        /// <param name="id">An identifier for the blob. This could be a path in file storage for instance</param>
        /// <param name="encoding">A text encoding to use to encode the text</param>
        /// <param name="cancellationToken">A cancellation token</param>
        /// <returns>An object loaded from the blob</returns>
        public static async Task<T> GetObject<T>(this IBlobStoreReader<T> reader, string id, CancellationToken cancellationToken = default) where T : class
        {
            await using var memStream = new MemoryStream();

            await reader.Get(id, memStream, cancellationToken);

            return await memStream.ReadObject<T>(cancellationToken)!;
        } 

        /// <summary>
        /// Uploads an object to a blob store with the given id/path
        /// </summary>
        /// <param name="id">An identifier for the blob. This could be a path in file storage for instance</param>
        /// <param name="encoding">A text encoding to use to encode the text</param>
        /// <param name="cancellationToken">A cancellation token</param>
        /// <returns>An object loaded from the blob</returns>
        public static async Task PutObject<T>(this IBlobStoreWriter<T> writer, string id, T obj, CancellationToken cancellationToken = default) where T : class
        {
            await using var memStream = new MemoryStream();

            await memStream.WriteObject(obj, cancellationToken);

            await writer.Put(id, memStream, cancellationToken);
        } 
    }
}
