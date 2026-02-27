/***************************************************************************
 *                                                                          
 *    The MondoCore Libraries  							                    
 *                                                                          
 *        Namespace: MondoCore.Common							            
 *             File: ExceptionExtensions.cs					    		        
 *        Class(es): ExceptionExtensions				         		            
 *          Purpose: Extensions for Exception               
 *                                                                          
 *  Original Author: Jim Lightfoot                                          
 *    Creation Date: 27 Feb 2026                                             
 *                                                                          
 *   Copyright (c) 2026 - Jim Lightfoot, All rights reserved           
 *                                                                          
 *  Licensed under the MIT license:                                         
 *    http://www.opensource.org/licenses/mit-license.php                    
 *                                                                          
 ****************************************************************************/

using MondoCore.Collections;
using System;

namespace MondoCore.Common
{
    /****************************************************************************/
    /****************************************************************************/
    public static class ExceptionExtensions
    {
        public static Exception WithData(this Exception ex, object data)
        {
            var properties = data.ToDictionary()!;

            foreach(var keyValuePair in properties)
                ex.Data[keyValuePair.Key] = keyValuePair.Value;

            return ex;
        }
    }
}
