package org.spark.base

/**
 * Created by pc on 2016/6/29.
 */
class NodeImpl(val id : String, val host:String, val port:Int, val cores:Int, val memory:Int) {
  var lastHeartbeat : Long = _

  init()

  def init() = {
    lastHeartbeat = System.currentTimeMillis()
  }

  override def toString: String = {
    s"""{${id}:<cores:$cores,memory:$memory>}"""
  }
}
