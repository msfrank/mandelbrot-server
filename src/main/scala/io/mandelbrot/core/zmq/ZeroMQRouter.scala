package io.mandelbrot.core.zmq

//import org.zeromq.{ZMsg, ZMQ, ZContext}
//import akka.actor.ActorRef
//import org.slf4j.LoggerFactory
//
//class ZeroMQRouter(zcontext: ZContext, address: String, handler: ActorRef) extends Thread {
//
//  val log = LoggerFactory.getLogger(classOf[ZeroMQRouter])
//
//  override def run(): Unit = {
//    val socket = zcontext.createSocket(ZMQ.ROUTER)
//    try {
//      socket.bind(address)
//      log.debug("binding socket to " + address)
//
//      var done = false
//      while (!done && !Thread.currentThread().isInterrupted) {
//        ZMsg.recvMsg(socket) match {
//          case null =>
//            done = true
//          case zmsg: ZMsg =>
//            val address = zmsg.pop()
//            val data = zmsg.pop()
//            handler ! ZeroMQMessage(address.getData, data.getData)
//            zmsg.destroy()
//            data.destroy()
//            address.destroy()
//        }
//      }
//    } catch {
//      case ex: InterruptedException =>
//        log.debug("thread was interrupted")
//      case ex: Throwable =>
//        log.debug("caught unhandled exception: " + ex.getMessage)
//    } finally {
//      socket.close()
//    }
//  }
//
//  def shutdown(): Unit = interrupt()
//
//}
//
//case class ZeroMQMessage(address: Array[Byte], data: Array[Byte])
