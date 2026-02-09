/*************************************************************************** 
 *                                                                           
 *    The MondoCore Libraries  	                                             
 *                                                                           
 *      Namespace: MondoCore.Common	                                         
 *           File: ICache.cs                                                 
 *      Class(es): ICache, ICacheDependency, FileDependency                  
 *        Purpose: Generic cache interface                                   
 *                                                                           
 *  Original Author: Jim Lightfoot                                           
 *    Creation Date: 29 Nov 2015                                             
 *                                                                           
 *   Copyright (c) 2015-2025 - Jim Lightfoot, All rights reserved            
 *                                                                           
 *  Licensed under the MIT license:                                          
 *    http://www.opensource.org/licenses/mit-license.php                     
 *                                                                           
 ****************************************************************************/

using System;
using System.Threading.Tasks;
using System.Collections.Generic;

namespace MondoCore.Common
{
    /****************************************************************************/
    /****************************************************************************/
    public interface ICache
    {
        Task<object?> Get(string key);

        Task          Add(string key, object objToAdd);
        Task          Add(string key, object objToAdd, DateTime dtExpires, ICacheDependency? dependency = null);
        Task          Add(string key, object objToAdd, TimeSpan tsExpires, ICacheDependency? dependency = null);
                      
        Task          Remove(string key);

        #region Default Methodd

        /****************************************************************************/
        public async Task<string?> GetString(string key)
        {
            return (await this.Get(key))?.ToString();
        }

        /****************************************************************************/
        /// <summary>
        /// Retrieve an item from the cache or create the item if it does not exist
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="cache">Cache to query</param>
        /// <param name="key">Key of item to retrieve</param>
        /// <param name="fnCreate">Callback for creating the item if it does not exist in the cache</param>
        /// <param name="onError">A callback to call if there was an error adding the newly created item into the cache</param>
        /// <param name="dtExpires">Explicit datetime to expire the item in the cache</param>
        /// <param name="tsExpires">Time relative to now to expire the cache. If both dtExpires is valid then this value is ignored</param>
        /// <param name="dependency">Optional dependency that will remove the item from the cache id triggered</param>
        /// <returns></returns>
        public async Task<T> Get<T>(string key, Func<Task<T>> fnCreate, Func<Exception, Task>? onError = null, DateTime? dtExpires = null, TimeSpan? tsExpires = null, ICacheDependency? dependency = null)
        {
            object? obj = null;
            
            try
            {
                obj = await this.Get(key);
            }
            catch
            {
                // Not in cache or other retrieval error
            }

            T? tobj = (T?)obj;

            if(obj == null)
            { 
                tobj = await fnCreate();

                try
                { 
                    if(dtExpires != null)
                        await this.Add(key, tobj!, dtExpires.Value, dependency);
                    else if(tsExpires != null)
                        await this.Add(key, tobj!, tsExpires.Value, dependency);
                    else
                        await this.Add(key, tobj!);
                }
                catch(Exception ex)
                {
                    if(onError != null)
                        await onError(ex);
                }
            }

            return tobj!;
        }
        #endregion
    }

    /****************************************************************************/
    /****************************************************************************/
    public interface ICacheDependency
    {
        string Type { get; }
    }

    /****************************************************************************/
    /****************************************************************************/
    public class FileDependency : ICacheDependency
    {
        /****************************************************************************/
        public FileDependency(string fileName)
        {
            this.FileName = fileName;
        }

        /****************************************************************************/
        public string Type      => "file";
        public string FileName  { get; }
    }
}
