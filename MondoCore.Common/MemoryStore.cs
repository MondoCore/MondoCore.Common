/**************************************************************************
 *                                                                         
 *    The MondoCore Libraries  	                                           
 *                                                                         
 *      Namespace: MondoCore.Common	                                       
 *           File: MemoryStore.cs                                          
 *      Class(es): MemoryStore                                             
 *        Purpose: In memory implementation of IBlobStore                  
 *                                                                         
 *  Original Author: Jim Lightfoot                                         
 *    Creation Date: 29 Jan 2020                                           
 *                                                                         
 *   Copyright (c) 2020-2025 - Jim Lightfoot, All rights reserved               
 *                                                                         
 *  Licensed under the MIT license:                                        
 *    http://www.opensource.org/licenses/mit-license.php                   
 *                                                                         
 ****************************************************************************/

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Drawing;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

namespace MondoCore.Common
{
    /****************************************************************************/
    /****************************************************************************/
    /// <summary>
    /// In memory implementation of IBlobStore
    /// </summary>
    public class MemoryStore : IBlobStore
    {
        private readonly ConcurrentDictionary<string, MemoryStream> _store = new ConcurrentDictionary<string, MemoryStream>();

        /****************************************************************************/
        public void Clear()
        {
            _store.Clear();
        }

        public byte[] this[string key] => _store[key].ToArray();
        public byte[] this[int key]    => _store.ToList()[key].Value.ToArray();
        public int    Count            => _store.Count;

        #region IBlobStore

        /****************************************************************************/
        public async Task Get(string id, Stream destination)
        {
            if(!_store.ContainsKey(id))
                throw new FileNotFoundException();
                
            var result = _store[id] as Stream;

            result.Seek(0L, SeekOrigin.Begin);

            await result.CopyToAsync(destination);

            return;
        }
          
        /****************************************************************************/
        /// <inheritdoc/>
        public Task<Stream> OpenRead(string id)
        {
            if(!_store.ContainsKey(id))
                throw new FileNotFoundException();
                
            return Task.FromResult<Stream>(new NonDisposablStream(_store[id]));
        }        

        /****************************************************************************/
        public async Task Put(string id, Stream content)
        {
            var newStream = new MemoryStream();

            await content.CopyToAsync(newStream);

            _store[id] = newStream;

            return;
        }

        /****************************************************************************/
        public Task Delete(string id)
        {
            _store.TryRemove(id, out _);

            return Task.CompletedTask;
        }

        /****************************************************************************/
        public Task<IEnumerable<string>> Find(string filter)
        {
            var keys = _store.Keys as IEnumerable<string>;

            return Task.FromResult(keys.Where( k=> k.MatchesWildcard(filter) ));
        }

        /****************************************************************************/
        /// <inheritdoc/>
        public async Task Enumerate(string filter, Func<IBlob, Task> fnEach, bool asynchronous = true)
        {
            var list = await this.Find(filter);
            List<Task>? tasks = asynchronous ? new List<Task>() : null;

            foreach(var file in list)
            {
                var task = fnEach(new FileStore.FileBlob(file));

                if(asynchronous)
                    tasks!.Add(task);
                else
                    await task;
            }

            if(asynchronous)
                await Task.WhenAll(tasks);
        }

        /****************************************************************************/
        /// <inheritdoc/>
        public async Task<Stream> OpenWrite(string id)
        {
            if(!_store.ContainsKey(id))
                await (this as IBlobStore).Put(id, "");

            return new NonDisposablStream(_store[id]);
        }

        /****************************************************************************/
        internal class NonDisposablStream(Stream parent) : Stream
        {
            public override bool CanRead  => parent.CanRead;
            public override bool CanSeek  => parent.CanSeek;
            public override bool CanWrite => parent.CanWrite;
            public override long Length   => parent.Length;

            public override long Position { get => parent.Position; set => parent.Position = value; }

            public override void Flush()
            {
                parent.Flush();
            }

            public override int Read(byte[] buffer, int offset, int count)
            {
                return parent.Read(buffer, offset, count);
            }

            public override long Seek(long offset, SeekOrigin origin)
            {
                return parent.Seek(offset, origin);
            }

            public override void SetLength(long value)
            {
                parent.SetLength(value);
            }

            public override void Write(byte[] buffer, int offset, int count)
            {
                parent.Write(buffer, offset, count);
            }

            protected override void Dispose(bool disposing)
            {
                base.Dispose(disposing);
            }
        }

        /****************************************************************************/
        /// <inheritdoc/>
        public async IAsyncEnumerable<IBlob> AsAsyncEnumerable()
        {
            var keys = _store.Keys as IEnumerable<string>;

            foreach(var key in keys)
            {
                await Task.Delay(0);

                yield return new FileStore.FileBlob(key);
            }
        }

        /****************************************************************************/
        /// <inheritdoc/>
        public Task<IDisposable> Lock(string id)
        {
            return Task.FromResult<IDisposable>(new FakeDisposable());
        }

        /****************************************************************************/
        /// <inheritdoc/>
        public Task<bool> Exists(string id)
        {
            return Task.FromResult<bool>(_store.ContainsKey(id));
        }

        /****************************************************************************/
        /// <inheritdoc/>
        public void Dispose()
        {
            foreach(var blob in _store.Values)
                blob.Dispose();

            _store.Clear();
        }

        #endregion
    }

    internal class FakeDisposable : IDisposable
    {
        public void Dispose()
        {
            // Do nothing
        }
    }
}
