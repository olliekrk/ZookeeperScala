package zk

trait AppMonitorListener {

  def exists(data: Array[Byte]): Unit

  def closing(reason: Int): Unit

}
