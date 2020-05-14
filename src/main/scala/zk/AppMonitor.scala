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

  var alive: Boolean = true
  listen()

  override def process(event: WatchedEvent): Unit = {
    println(s"Received watched event: $event".ok)
    event.getType match {
      case Event.EventType.None if event.getState == KeeperState.Expired =>
        alive = false
        listener.closing(Code.SESSIONEXPIRED.intValue)
      case Event.EventType.NodeCreated | Event.EventType.NodeDeleted if event.getPath == zkNode =>
        zk.exists(zkNode, true, this, null)
        zk.getChildren(zkNode, true, this, null)
      case Event.EventType.NodeChildrenChanged =>
        zk.getChildren(event.getPath, true, this, null)
      case _ =>
        println("No action.".warn)
    }
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
      case Code.SESSIONEXPIRED | Code.NOAUTH =>
        alive = false
        listener.closing(rc)
      case _ =>
        println(s"Unexpected response ($rc) from $path".warn)
    }
  }

  override def processResult(rc: Int, path: String, ctx: Any, children: util.List[String]): Unit =
    println(s"(from $path) total tree size: $getTreeSize".info)

  private def listen(): Unit = {
    zk.exists(zkNode, true, this, null)
    traverseTree(zkNode)(zk.getChildren(_, true, this, null))
  }
}
