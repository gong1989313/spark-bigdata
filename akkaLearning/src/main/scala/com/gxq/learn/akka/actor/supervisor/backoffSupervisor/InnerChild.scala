package com.gxq.learn.akka.actor.supervisor.backoffSupervisor

import akka.actor._
import akka.pattern._
import scala.util.Random

import scala.concurrent.duration._

object InnerChild {
  case class TestMessage(msg: String)
  class ChildException(val errmsg: TestMessage) extends Exception
  object CException { //for pattern match of class with parameter
    def apply(msg: TestMessage) = new ChildException(msg)
    def unapply(cex: ChildException) = Some(cex.errmsg)
  }
  def props = Props[InnerChild]
}
class InnerChild extends Actor with ActorLogging {
  import InnerChild._
  context.parent ! BackoffSupervisor.Reset //reset backoff counts
  override def receive: Receive = {
    case TestMessage(msg) => //模拟子级功能
      if (Random.nextBoolean()) //任意产生异常
        throw new ChildException(TestMessage(msg))
      else
        log.info(s"Child received message: ${msg}")
  }
}
object Supervisor {
  def props: Props = { //在这里定义了监管策略和child Actor构建
    def decider: PartialFunction[Throwable, SupervisorStrategy.Directive] = {
      case InnerChild.CException(tmsg) =>
        println(s"Message causing exception: ${tmsg.msg}") //we can extract message here
        BackoffSupervisorDemo.sendToParent(tmsg) //resend message
        SupervisorStrategy.Restart
    }

    val options = Backoff.onFailure(InnerChild.props, "innerChild", 1 second, 5 seconds, 0.0)
      .withManualReset
      .withSupervisorStrategy(
        OneForOneStrategy(maxNrOfRetries = 5, withinTimeRange = 5 seconds)(
          decider.orElse(SupervisorStrategy.defaultDecider)))
    BackoffSupervisor.props(options)
  }
}
//注意：下面是Supervisor的父级，不是InnerChild的父级
object ParentalActor {
  case class SendToSupervisor(msg: InnerChild.TestMessage)
  case class SendToInnerChild(msg: InnerChild.TestMessage)
  case class SendToChildSelection(msg: InnerChild.TestMessage)
  def props = Props[ParentalActor]
}
class ParentalActor extends Actor with ActorLogging {
  import ParentalActor._
  //在这里构建子级Actor supervisor
  val supervisor = context.actorOf(Supervisor.props, "supervisor")
  override def receive: Receive = {
    case msg @ _ => supervisor ! msg
  }

}
object DeadLetterMonitor {
  def props(parentRef: ActorRef) = Props(new DeadLetterMonitor(parentRef))
}
class DeadLetterMonitor(receiver: ActorRef) extends Actor with ActorLogging {
  import InnerChild._
  import context.dispatcher
  override def receive: Receive = {
    case DeadLetter(msg, sender, _) =>
      //wait till InnerChild finishes restart then resend
      context.system.scheduler.scheduleOnce(1 second, receiver, msg.asInstanceOf[TestMessage])
  }
}
object BackoffSupervisorDemo extends App {
  import ParentalActor._
  import InnerChild._

  def sendToParent(msg: TestMessage) = parent ! msg

  val testSystem = ActorSystem("testSystem")
  val parent = testSystem.actorOf(ParentalActor.props, "parent")

  val deadLetterMonitor = testSystem.actorOf(DeadLetterMonitor.props(parent), "dlmonitor")
  testSystem.eventStream.subscribe(deadLetterMonitor, classOf[DeadLetter]) //listen to DeadLetter

  parent ! TestMessage("Hello message 1 to supervisor")
  parent ! TestMessage("Hello message 2 to supervisor")
  parent ! TestMessage("Hello message 3 to supervisor")
  parent ! TestMessage("Hello message 4 to supervisor")
  parent ! TestMessage("Hello message 5 to supervisor")
  parent ! TestMessage("Hello message 6 to supervisor")

  scala.io.StdIn.readLine()

  testSystem.terminate()

}