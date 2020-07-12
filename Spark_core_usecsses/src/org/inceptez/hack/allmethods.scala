package org.inceptez.hack

import scala.util.matching.Regex

   class allmethods  extends java.io.Serializable
  {
   def remspecialchar(i:String):String=
    {
     
    val pattern="[0-9]|\\-|\\?|\\,|\\/|\\_|\\(|\\)|\\[|\\]".r
    val p="[0-9]|\\-|\\?|\\,|\\/|\\_|\\(|\\)|\\[|\\]"
    
    /*pattern replaceAllIn(i,"")
     * 
     */
    return i.trim().replaceAll(p, " ").
    replaceAll("\\\\/"," ").
    replaceAll("\\\\_"," ")
    
    
    }
  }
