package zk

import org.apache.zookeeper.AsyncCallback.StatCallback
import org.apache.zookeeper.KeeperException.Code
import org.apache.zookeeper.Watcher.Event
import org.apache.zookeeper.Watcher.Event.KeeperState
import org.apache.zookeeper.data.Stat
import org.apache.zookeeper.{WatchedEvent, Watcher, ZooKeeper}
import util.ConsoleOps._

class AppMonitor(val zk: ZooKeeper,
                 val zkNode: String,
                 val listener: AppMonitorListener) extends Watcher with StatCallback {

  zk.exists(zkNode, true, this, null)
  var alive: Boolean = true
  var previousData: Array[Byte] = Array.emptyByteArray

  override def process(event: WatchedEvent): Unit =
    event.getType match {
      case Event.EventType.None =>
        event.getState match {
          case KeeperState.Expired =>
            alive = false
            listener.closing(Code.SESSIONEXPIRED.intValue)
          case other => println(s"Event state: $other".green)
        }
      case other if event.getPath == zkNode =>
        println(s"Received event of type: $other".green)
        zk.exists(zkNode, true, this, null)
    }

  override def processResult(rc: Int, path: String, ctx: Any, stat: Stat): Unit = {
    println("Processing...".blue)
    Code.get(rc) match {
      case Code.OK => processNewData(zk.getData(zkNode, false, null))(listener.exists)
      case Code.NONODE => processNewData(Array.emptyByteArray)(listener.exists)
      case Code.SESSIONEXPIRED | Code.NOAUTH => alive = false; listener.closing(rc)
      case _ =>
        println(s"$rc $path")
        zk.exists(zkNode, true, this, null)
    }
  }

  private def processNewData[T](data: Array[Byte])(f: Array[Byte] => T): Unit =
    if (data == null || !data.sameElements(previousData)) {
      f(data)
      previousData = data
    }

}
