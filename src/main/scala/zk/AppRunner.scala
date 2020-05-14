package zk

import org.apache.zookeeper.{WatchedEvent, Watcher, ZooKeeper}
import util.ConsoleOps._

import scala.annotation.tailrec
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.io.{Source, StdIn}
import scala.jdk.CollectionConverters._
import scala.sys.process._
import scala.util.Try

class AppRunner(zkAddress: String,
                zkNode: String,
                val executable: String,
                val executableArgs: List[String]
               ) extends Watcher with Runnable with AppMonitorListener {

  private val zk: ZooKeeper = new ZooKeeper(zkAddress, 3000, this)
  private val appMonitor = new AppMonitor(zk, zkNode, this)
  private var app: Option[Process] = None

  override def process(event: WatchedEvent): Unit = appMonitor.process(event)

  override def exists(data: Array[Byte]): Unit =
    if (data.isEmpty && app.nonEmpty) {
      println("Killing the process...".green)
      triggerAppShutdown()
      app = None
    } else {
      if (app.nonEmpty) {
        println("Received new data. Stopping the app.")
        triggerAppShutdown()
        Source
          .fromBytes(data)
          .getLines()
          .map(_.blue)
          .foreach(println)
      }

      println("Starting the process...".green)
      app = Option {
        val process = Process(executableArgs)
        val logger = ProcessLogger(out => println(out.cyan))
        process.run(logger)
      }
    }

  override def closing(reason: Int): Unit =
    synchronized {
      notifyAll()
    }

  override def run(): Unit =
    try synchronized {
      while (appMonitor.alive) wait()
    } catch {
      case e: InterruptedException => println(s"Shutdown after exception:\n$e".red)
    }

  def printTree(): Unit = println(s"Total tree size is ${traverseTree(zkNode)}")

  @tailrec
  private def traverseTree(nodePath: String, queued: List[String] = Nil, traversedCount: Int = 0): Int = {
    val (children, updatedCount) = {
      if (zk.exists(nodePath, false) != null) {
        println(nodePath.yellow)
        (zk.getChildren(nodePath, false).asScala.toList, traversedCount + 1)
      } else {
        (Nil, traversedCount)
      }
    }

    (children, queued) match {
      case (Nil, Nil) => updatedCount
      case (Nil, first :: rest) => traverseTree(first, rest, updatedCount)
      case (child :: rest, _) => traverseTree(s"$nodePath/$child", rest ::: queued, updatedCount)
    }
  }

  private def triggerAppShutdown(): Unit =
    Try {
      app.tapEach(_.destroy()).foreach(_.exitValue())
    } recover {
      case _: InterruptedException => println("Interrupted while waiting for app shutdown!".yellow)
    }
}


object AppRunner extends App {
  if (args.length < 4) {
    println("Missing required arguments: <zk-address> <zk-node> <executable> {args}".yellow)
    sys.exit(1)
  }

  val zkAddress :: zkNode :: executable :: executableArgs = args.toList
  val appRunner = new AppRunner(zkAddress, zkNode, executable, executableArgs)

  private def parseCommands: Future[Unit] = Future {
    while (true) {
      println("Waiting for input:".magenta)
      StdIn.readLine() match {
        case "tree" =>
          println(s"Tree for ZK node '$zkNode':")
          appRunner.printTree()
        case _ =>
          println("Unknown command.".red)
      }
    }
  }

  parseCommands
  appRunner.run()
}
