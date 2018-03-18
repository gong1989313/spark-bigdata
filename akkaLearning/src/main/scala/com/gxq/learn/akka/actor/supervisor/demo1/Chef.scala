package com.gxq.learn.akka.actor.supervisor.demo1

import akka.actor.Props
import akka.actor.ActorLogging
import akka.actor.Actor

object Chef {
  sealed trait Cooking
  case object CookSpecial extends Cooking
  class ChefBusy(msg: String) extends Exception(msg)
  def props = Props(new Chef)
}
class Chef extends Actor with ActorLogging {
  import Chef._
  log.info(s"Chef actor created at ${System.currentTimeMillis()}")
  override def receive: Receive = {
    case _ => throw new ChefBusy("Chef is busy cooking!")
  }

  override def preRestart(reason: Throwable, message: Option[Any]): Unit = {
    super.preRestart(reason, message)
    log.info(s"Restarting Chef for $message")
  }

  override def postRestart(reason: Throwable): Unit = {
    super.postRestart(reason)
    log.info(s"Chef restarted for ${reason.getMessage}")
  }

  override def postStop(): Unit = {
    log.info("Chef stopped!")
  }
}