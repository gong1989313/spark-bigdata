package com.gxq.learn.akka.actor.router

import akka.actor._
import akka.routing.ConsistentHashingRouter.{ ConsistentHashMapping, ConsistentHashable, ConsistentHashableEnvelope }
import akka.routing._

object MoneyCounter {
  sealed class Counting(cur: String) extends ConsistentHashable {
    override def consistentHashKey: Any = cur
  }
  case class OneHand(cur: String, amt: Double) extends Counting(cur)
  case class ReportTotal(cur: String) extends Counting(cur)
  def props = Props(new MoneyCounter)
}
class MoneyCounter extends Actor with ActorLogging {
  import MoneyCounter._
  var currency: String = "RMB"
  var amount: Double = 0

  override def receive: Receive = {
    case OneHand(cur, amt) =>
      currency = cur
      amount += amt
      log.info(s"${self.path.name} received one hand of $amt$cur")
    case ReportTotal(_) =>
      log.info(s"${self.path.name} has a total of $amount$currency")
  }
}
object HashingRouter extends App {
  import MoneyCounter._
  import scala.util.Random

  val currencies = List("RMB", "USD", "EUR", "JPY", "GBP", "DEM", "HKD", "FRF", "CHF")

  val routerSystem = ActorSystem("routerSystem")

  val router = routerSystem.actorOf(ConsistentHashingPool(
    nrOfInstances = currencies.size + 1, virtualNodesFactor = 2).props(
    MoneyCounter.props), name = "moneyCounter")

  (1 to 20).toList foreach (_ => router ! OneHand(
    currencies(Random.nextInt(currencies.size - 1)), Random.nextInt(100) * 1.00))

  currencies foreach (c => router ! ReportTotal(c))
  scala.io.StdIn.readLine()
  routerSystem.terminate()
}