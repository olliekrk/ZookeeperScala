package zk

import org.apache.zookeeper.data.Stat

trait AppMonitorListener {

  def exists(stat: Stat, isAlive: Boolean): Unit

  def closing(rc: Int): Unit

}
