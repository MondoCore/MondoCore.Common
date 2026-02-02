/***************************************************************************
 *                                                                          
 *    The MondoCore Libraries  							                    
 *                                                                          
 *        Namespace: MondoCore.Common							            
 *             File: FileStore.cs					    		            
 *        Class(es): FileStore				         		                
 *          Purpose: IBlobStore wrapper for file system                     
 *                                                                          
 *  Original Author: Jim Lightfoot                                          
 *    Creation Date: 20 Jan 2020                                            
 *                                                                          
 *   Copyright (c) 2020-2026 - Jim Lightfoot, All rights reserved                
 *                                                                          
 *  Licensed under the MIT license:                                         
 *    http://www.opensource.org/licenses/mit-license.php                    
 *                                                                          
 ****************************************************************************/

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

namespace MondoCore.Common
{
    /****************************************************************************/
    /****************************************************************************/
    public class FileStore : IBlobStore
    {
        private readonly string _pathRoot;

        /****************************************************************************/
        public FileStore(string pathRoot)
        {
            _pathRoot = pathRoot;
        }

        #region IBlobStore 

        /****************************************************************************/
        /// <inheritdoc/>
        public virtual Task<IEnumerable<string>> Find(string filter)
        {
            var directory = new DirectoryInfo(_pathRoot);

            return Task.FromResult(directory.EnumerateFiles(filter, SearchOption.AllDirectories).Select( (fi)=> 
            {
                return fi.FullName.Substring(_pathRoot.Length).EnsureNotStartsWith(Path.DirectorySeparatorChar.ToString());
            }));
        }

        /****************************************************************************/
        /// <inheritdoc/>
        public async Task Get(string id, Stream destination)
        {
            using(var memStream = await OpenRead(id))
            {
                await memStream.CopyToAsync(destination);
            }
        }
          
        /****************************************************************************/
        /// <inheritdoc/>
        public Task<Stream> OpenRead(string id)
        {
            return Task.FromResult((Stream)new FileStream(CombinePath(id),  
                                                          FileMode.Open, 
                                                          FileAccess.Read, 
                                                          FileShare.Read,  
                                                          bufferSize: 4096,
                                                          useAsync: true));
        }
          
        /****************************************************************************/
        /// <inheritdoc/>
        public async Task Delete(string id)
        {   
            try
            {
                var fileName = CombinePath(id);
                int nTrys = 0;

                while(File.Exists(fileName) && ++nTrys <= 20)
                {
                    try
                    {
                        File.Delete(fileName);
                        return;
                    }
                    catch
                    {
                    }

                    await Task.Delay(50);
                }
            }
            catch
            {
            }        
        }

        /****************************************************************************/
        /// <inheritdoc/>
        public async Task Put(string id, Stream content)
        {
            var path = CombinePath(id);

            try
            {
                using (var fileStream = new FileStream(path,  
                                                       FileMode.Append, 
                                                       FileAccess.Write, 
                                                       FileShare.None,  
                                                       bufferSize: 4096, 
                                                       useAsync: true))  
                {        
                    await content.CopyToAsync(fileStream).ConfigureAwait(false);  
                }; 
            }
            catch(DirectoryNotFoundException)
            {
                if(EnsurePathExists(path))
                    await Put(id, content);
                else
                    throw;
            }        
        }

        /****************************************************************************/
        /// <inheritdoc/>
        public async Task Enumerate(string filter, Func<IBlob, Task> fnEach, bool asynchronous = true)
        {
            var enume = await this.Find(filter);
            var list = new List<string>(enume);
            List<Task>? tasks = asynchronous ? new List<Task>() : null;

            foreach(var file in list)
            {
                var task = fnEach(new FileBlob(file));

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
        public async IAsyncEnumerable<IBlob> AsAsyncEnumerable()
        {
            var directory = new DirectoryInfo(_pathRoot);
            var files     = directory.EnumerateFiles("*.*", SearchOption.AllDirectories);
            var rootPath = _pathRoot.EnsureEndsWith(Path.DirectorySeparatorChar);

            foreach(var file in files)
            { 
                await Task.Delay(0);

                yield return new FileStore.FileBlob(file.FullName.Substring(rootPath.Length));
            }
        }

        /****************************************************************************/
        /// <inheritdoc/>
        public Task<IDisposable> Lock(string id)
        {
            throw new NotImplementedException();
        }
        

        /****************************************************************************/
        /// <inheritdoc/>
        public Task<bool> Exists(string id)
        {
            return Task.FromResult<bool>(File.Exists(id));
        }
        
        /****************************************************************************/
        /// <inheritdoc/>
        public void Dispose()
        {
            // Nothing to do
        }

        #endregion

        /****************************************************************************/
        /// <summary>
        /// Ensures the full path of the given filename exists by creating it if does not exist
        /// </summary>
        /// <param name="fileName">A full path name of a file</param>
        /// <returns>Returns true if the path was created or false if it was already there</returns>
        public static bool EnsurePathExists(string fileName)
        {
            var destFile = new FileInfo(fileName);

            if(!destFile.Directory.Exists)
            {
                destFile.Directory.Create();

                return true;
            }

            return false;
        }
        
        #region Private 

        /****************************************************************************/
        private string CombinePath(string id)
        {
            return Path.Combine(_pathRoot, id.Replace("/", "\\").Replace("~", "").EnsureNotStartsWith("\\")).Replace("\\\\", "\\");
        }

        /****************************************************************************/
        public Task<Stream> OpenWrite(string id)
        {
            throw new NotImplementedException();
        }

        /****************************************************************************/
        public class FileBlob : IBlob
        { 
            internal FileBlob(string blob)
            {
                this.Name = blob;
            }

            public string                       Name        { get; }
            public bool                         Deleted     => false;
            public bool                         Enabled     => true;
            public string?                      Version     => null;
            public string                       ContentType => "";
            public DateTimeOffset?              Expires     => null;
            public IDictionary<string, string>? Metadata    => null;
            public IDictionary<string, string>? Tags        => null;
        }

        #endregion
    }
}
