/*************************************************************************** 
 *                                                                           
 *    The MondoCore Libraries  							                     
 *                                                                           
 *        Namespace: MondoCore.Common							             
 *             File: StreamExtensions.cs					    		         
 *        Class(es): StreamExtensions				         		             
 *          Purpose: Extensions for Streams                  
 *                                                                           
 *  Original Author: Jim Lightfoot                                           
 *    Creation Date: 1 Jan 2020                                              
 *                                                                           
 *   Copyright (c) 2005-2025 - Jim Lightfoot, All rights reserved            
 *                                                                           
 *  Licensed under the MIT license:                                          
 *    http://www.opensource.org/licenses/mit-license.php                     
 *                                                                           
 ****************************************************************************/

using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;

namespace MondoCore.Common
{
    /****************************************************************************/
    /****************************************************************************/
    public static class StreamExtensions
    {
        /****************************************************************************/
        /// <summary>
        /// Reads in from string and converts into a string
        /// </summary>
        /// <param name="stream">Stream to read from</param>
        /// <param name="encoder">Text encoder. Will default to UTFEncoding</param>
        /// <returns>The resulting string</returns>
        public static async Task<string> ReadStringAsync(this Stream stream, Encoding? encoder = null)
        {
            encoder = encoder ?? UTF8Encoding.UTF8;

            if(stream is MemoryStream memStream)
            { 
                var array  = memStream.ToArray();
                var arrLen = array.Length;
                var str    = encoder.GetString(array);
                var atrLen = str.Length;

                return str.TrimNulls();
            }

            if(stream.CanSeek)
                stream.Seek(0, SeekOrigin.Begin);

            try
            { 
                using(var mem = new MemoryStream())
                { 
                    await stream.CopyToAsync(mem).ConfigureAwait(false);

                    return encoder.GetString(mem.ToArray()).TrimNulls();
                }
            }
            finally
            { 
                if(stream.CanSeek)
                    stream.Seek(0, SeekOrigin.Begin);
            }
        }

        /****************************************************************************/
        /// <summary>
        /// Reads in from string and converts into a string
        /// </summary>
        /// <param name="stream">Stream to read from</param>
        /// <param name="encoder">Text encoder. Will default to UTFEncoding</param>
        /// <returns>The resulting string</returns>
        public static string ReadString(this Stream stream, Encoding? encoder = null)
        {
            encoder = encoder ?? UTF8Encoding.UTF8;

            if(stream.CanSeek)
                stream.Seek(0, SeekOrigin.Begin);

            try
            { 
                using(var mem = new MemoryStream())
                { 
                    stream.CopyTo(mem);

                    return encoder.GetString(mem.ToArray());
                }
            }
            finally
            { 
                if(stream.CanSeek)
                    stream.Seek(0, SeekOrigin.Begin);
            }
        }

        /****************************************************************************/
        /// <summary>
        /// Write a string to the stream
        /// </summary>
        /// <param name="stream">The stream to write to</param>
        /// <param name="data"></param>
        /// <param name="encoder">Text encoder. Will default to UTFEncoding</param>
        /// <returns></returns>
        public static Task WriteAsync(this Stream stream, string data, Encoding? encoder = null)
        {
            encoder = encoder ?? UTF8Encoding.UTF8;

            byte[] bytes = encoder.GetBytes(data);
       
            return stream.WriteAsync(bytes, 0, bytes.Length);
        }

        /****************************************************************************/
        /// <summary>
        /// Converts stream to byte array
        /// </summary>
        /// <param name="stream">Stream to read from</param>
        /// <param name="encoder">Text encoder. Will default to UTFEncoding</param>
        /// <returns>The resulting string</returns>
        public static byte[] ToArray(this Stream stream, Encoding? encoder = null)
        {
            encoder = encoder ?? UTF8Encoding.UTF8;

            if(stream is MemoryStream memStream)
                return memStream.ToArray();

            try
            { 
                using(var mem = new MemoryStream())
                { 
                    stream.CopyTo(mem);

                    return mem.ToArray();
                }
            }
            finally
            { 
            }
        }

        /// <summary>
        /// Loads an object from the stream
        /// </summary>
        /// <param name="stream">The stream to read the object from</param>
        /// <returns>An object loaded from the stream</returns>
        public static async Task<T> ReadObject<T>(this Stream stream, CancellationToken cancellationToken = default) where T : class
        {
            if(stream.CanSeek)
                stream.Seek(0, SeekOrigin.Begin);

            var result = await JsonSerializer.DeserializeAsync<T>(stream, cancellationToken: cancellationToken);

            return result!;
        } 

        /// <summary>
        /// Uploads an object to a stream
        /// </summary>
        /// <param name="stream">The stream to load the object to</param>
        /// <param name="cancellationToken">A cancellation token</param>
        public static async Task WriteObject<T>(this Stream stream, T obj, CancellationToken cancellationToken = default) where T : class
        {
            await JsonSerializer.SerializeAsync(stream, obj, cancellationToken: cancellationToken);
        } 
    }
}
