package zk

import org.apache.zookeeper.data.Stat
import org.apache.zookeeper.{WatchedEvent, Watcher, ZooKeeper}
import util.ConsoleOps._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.io.StdIn
import scala.sys.process._
import scala.util.Try

class AppRunner(zkAddress: String,
                override val zkNode: String,
                val executableArgs: List[String]
               ) extends Watcher with Runnable with AppMonitorListener with TreePrinter {

  override val zk: ZooKeeper = new ZooKeeper(zkAddress, 30_000, this)
  private val appMonitor = new AppMonitor(zk, zkNode, this)
  private var app: Option[Process] = None

  override def process(event: WatchedEvent): Unit = appMonitor.process(event)

  override def exists(stat: Stat, isAlive: Boolean): Unit =
    if (isAlive && app.isEmpty) {
      println("Starting the process...".warn)
      app = Option {
        val process = Process(executableArgs)
        val logger = ProcessLogger(out => println(out.white))
        process.run(logger)
      }
    } else if (!isAlive && app.nonEmpty) {
      println("Killing the process...".warn)
      triggerAppShutdown()
      app = None
    }

  override def closing(rc: Int): Unit =
    synchronized {
      notifyAll()
    }

  override def run(): Unit =
    try synchronized {
      while (appMonitor.alive) wait()
    } catch {
      case e: InterruptedException => println(s"Shutdown after exception:\n$e".red)
    }

  private def triggerAppShutdown(): Unit =
    Try {
      app.tapEach(_.destroy()).foreach(_.exitValue())
    } recover {
      case _: InterruptedException => println("Interrupted while waiting for app shutdown".red)
    }
}


object AppRunner extends App {
  if (args.length < 4) {
    println("Missing required arguments: <zk-address> <zk-node> <executable> {args}".warn)
    sys.exit(1)
  }

  val zkAddress :: zkNode :: executableArgs = args.toList
  val appRunner = new AppRunner(zkAddress, zkNode, executableArgs)

  private def parseCommands: Future[Unit] = Future {
    while (true) {
      println("Waiting for input:".info)
      StdIn.readLine() match {
        case "tree" =>
          println(s"Tree for ZK node '$zkNode':")
          val treeSize = appRunner.traverseTree(zkNode)(node => println(node.ok))
          println(s"Total tree size is $treeSize".info)
        case _ =>
          println("Unknown command.".warn)
      }
    }
  }

  parseCommands
  appRunner.run()
}
