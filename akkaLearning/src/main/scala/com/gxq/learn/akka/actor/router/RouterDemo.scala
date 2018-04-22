package com.gxq.learn.akka.actor.router

import akka.actor._
import akka.routing._
import scala.annotation.tailrec
import scala.concurrent.duration._
import scala.util.Random

object FibonacciRoutee {
  case class FibonacciNumber(nbr: Int, msDelay: Int) //增加延迟参数
  case class GetAnswer(nbr: Int)

  class RouteeException extends Exception

  def props = Props[FibonacciRoutee]
}

class FibonacciRoutee extends Actor with ActorLogging {
  import FibonacciRoutee._
  import context.dispatcher

  override def receive: Receive = {
    case FibonacciNumber(nbr, ms) =>
      context.system.scheduler.scheduleOnce(ms second, self, GetAnswer(nbr))
    case GetAnswer(nbr) =>
      if (Random.nextBoolean())
        throw new RouteeException
      else {
        val answer = fibonacci(nbr)
        log.info(s"${self.path.name}'s answer: Fibonacci($nbr)=$answer")
      }
  }
  private def fibonacci(n: Int): Int = {
    @tailrec
    def fib(n: Int, b: Int, a: Int): Int = n match {
      case 0 => a
      case _ =>
        fib(n - 1, a + b, b)
    }
    fib(n, 1, 0)
  }

  override def preRestart(reason: Throwable, message: Option[Any]): Unit = {
    log.info(s"Restarting ${self.path.name} on ${reason.getMessage}")
    message foreach { m => self ! m }
    super.preRestart(reason, message)
  }

  override def postRestart(reason: Throwable): Unit = {
    log.info(s"Restarted ${self.path.name} on ${reason.getMessage}")
    super.postRestart(reason)
  }

  override def postStop(): Unit = {
    log.info(s"Stopped ${self.path.name}!")
    super.postStop()
  }

}
object RouterDemo extends App {
  import FibonacciRoutee._
  import scala.concurrent.ExecutionContext.Implicits.global
  val routingSystem = ActorSystem("routingSystem")
  /* cannot set SupervisorStrategy in config file
  val router = routingSystem.actorOf(
    FromConfig.props(FibonacciRoutee.props)
    ,"balance-pool-router")
    */
  val routingDecider: PartialFunction[Throwable, SupervisorStrategy.Directive] = {
    case _: RouteeException => SupervisorStrategy.Restart
  }
  val routerSupervisorStrategy = OneForOneStrategy(maxNrOfRetries = 5, withinTimeRange = 5 seconds)(
    routingDecider.orElse(SupervisorStrategy.defaultDecider))
  /* does not support resizing routees
  val router = routingSystem.actorOf(
    BalancingPool(nrOfInstances = 3
      ,supervisorStrategy=routerSupervisorStrategy    //set SupervisorStrategy here
      ).withDispatcher("akka.pool-dispatcher")
      .props(FibonacciRoutee.props)
    ,"balance-pool-router"
  ) */

  val resizer = DefaultResizer(
    lowerBound = 2, upperBound = 5, pressureThreshold = 1, rampupRate = 1, backoffRate = 0.25, backoffThreshold = 0.25, messagesPerResize = 1)
  val router = routingSystem.actorOf(
    RoundRobinPool(nrOfInstances = 2, resizer = Some(resizer), supervisorStrategy = routerSupervisorStrategy)
      .props(FibonacciRoutee.props), "roundrobin-pool-router")

  router ! FibonacciNumber(10, 5)
  router ! FibonacciNumber(13, 2)
  router ! FibonacciNumber(15, 3)
  router ! FibonacciNumber(17, 1)
  router ! FibonacciNumber(27, 1)
  router ! FibonacciNumber(37, 1)
  router ! FibonacciNumber(47, 1)

  scala.io.StdIn.readLine()
  routingSystem.terminate()
}