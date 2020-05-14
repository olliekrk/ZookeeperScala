package zk

import org.apache.zookeeper.ZooKeeper
import util.ConsoleOps._

import scala.annotation.tailrec
import scala.jdk.CollectionConverters._

trait TreePrinter {

  def zk: ZooKeeper

  def zkNode: String

  def printTree(): Unit = println(s"Total tree size is ${traverseTree(zkNode)(node => println(node.ok))}".info)

  def printTreeSize(): Unit = println(s"Total tree size is ${traverseTree(zkNode)()}".info)

  @tailrec
  final def traverseTree(nodePath: String, queued: List[String] = Nil, traversedCount: Int = 0)
                          (f: String => Unit = _ => ()): Int = {
    val (children, updatedCount) = {
      if (zk.exists(nodePath, false) != null) {
        f(nodePath)
        (zk.getChildren(nodePath, false).asScala.map(s"$nodePath/" + _).toList, traversedCount + 1)
      } else {
        (Nil, traversedCount)
      }
    }

    (children, queued) match {
      case (Nil, Nil) => updatedCount
      case (Nil, first :: rest) => traverseTree(first, rest, updatedCount)(f)
      case (child :: rest, _) => traverseTree(child, rest ::: queued, updatedCount)(f)
    }
  }

}