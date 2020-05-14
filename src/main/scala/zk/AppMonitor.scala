package zk

import org.apache.zookeeper.AsyncCallback.{ChildrenCallback, StatCallback}
import org.apache.zookeeper.KeeperException.Code
import org.apache.zookeeper.Watcher.Event
import org.apache.zookeeper.Watcher.Event.KeeperState
import org.apache.zookeeper.data.Stat
import org.apache.zookeeper.{WatchedEvent, Watcher, ZooKeeper}
import util.ConsoleOps._
import java.util

class AppMonitor(override val zk: ZooKeeper,
                 override val zkNode: String,
                 val listener: AppMonitorListener
                ) extends Watcher with StatCallback with ChildrenCallback with TreePrinter {

  listen()
  var alive: Boolean = true
  var previousData: Array[Byte] = Array.emptyByteArray

  override def process(event: WatchedEvent): Unit =
    event.getType match {
      case Event.EventType.None if event.getState == KeeperState.Expired =>
        alive = false
        listener.closing(Code.SESSIONEXPIRED.intValue)
      case _ =>
        println(s"Received watched event: $event".ok)
        listen()
    }

  override def processResult(rc: Int, path: String, ctx: Any, stat: Stat): Unit = {
    println(s"Received server response. ($rc)".info)
    Code.get(rc) match {
      case Code.OK =>
        println(s"$zkNode is alive".info)
        listener.exists(stat, isAlive = true)
      case Code.NONODE =>
        println(s"$zkNode is not alive.".info)
        listener.exists(stat, isAlive = false)
      case Code.SESSIONEXPIRED | Code.NOAUTH => alive = false; listener.closing(rc)
      case _ =>
        println(s"Unexpected response ($rc) from $path".warn)
    }
  }

  override def processResult(rc: Int, path: String, ctx: Any, children: util.List[String]): Unit = printTree()

  private def listen(): Unit = {
    zk.exists(zkNode, true, this, null)
    zk.getChildren(zkNode, true, this, null)
  }
}
