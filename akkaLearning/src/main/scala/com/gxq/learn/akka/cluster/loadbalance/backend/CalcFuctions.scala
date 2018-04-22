/*package com.gxq.learn.akka.cluster.loadbalance.backend

import akka.actor.Props
import akka.actor.AbstractActor.Receive
import akka.actor.Actor
import com.gxq.learn.akka.cluster.loadbalance.messages.Messages._

object CalcFuctions {
  def propsFuncs = Props(new CalcFuctions)
  def propsSuper(role: String) = Props(new CalculatorSupervisor(role))
}

class CalcFuctions extends Actor {
  override def receive: Receive = {
    case Add(x,y) =>
      println(s"$x + $y carried out by ${self} with result=${x+y}")
    case Sub(x,y) =>
      println(s"$x - $y carried out by ${self} with result=${x - y}")
    case Mul(x,y) =>
      println(s"$x * $y carried out by ${self} with result=${x * y}")
    case Div(x,y) =>
        println(s"$x / $y carried out by ${self} with result=${x / y}")
  }

  override def preRestart(reason: Throwable, message: Option[Any]): Unit = {
    println(s"Restarting calculator: ${reason.getMessage}")
    super.preRestart(reason, message)
  }
}*/