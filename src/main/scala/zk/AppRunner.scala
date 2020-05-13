package zk

import org.apache.zookeeper.{WatchedEvent, Watcher, ZooKeeper}
import util.ConsoleOps._

import scala.io.Source
import scala.util.Try

class AppRunner(zkAddress: String,
                zkNode: String,
                val executable: String,
                val executableArgs: List[String]
               ) extends Watcher with Runnable with AppMonitorListener {

  private val zk = new ZooKeeper(zkAddress, 3000, this)
  private val monitor = new AppMonitor(zk, zkNode, this)
  private var app: Option[Process] = None

  override def process(event: WatchedEvent): Unit = monitor.process(event)

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
      app = Option(Runtime.getRuntime.exec(executableArgs.toArray))
    }

  override def closing(reason: Int): Unit =
    synchronized {
      notifyAll()
    }

  override def run(): Unit =
    try synchronized {
      while (monitor.alive) wait()
    } catch {
      case e: InterruptedException => println(s"Shutdown after exception:\n$e".red)
    }

  private def triggerAppShutdown(): Unit =
    Try {
      app
        .tapEach(_.destroy())
        .foreach(_.waitFor())
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
  new AppRunner(zkAddress, zkNode, executable, executableArgs).run()
}
