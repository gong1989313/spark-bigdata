package com.gxq.learn.akka.actor.supervisor.demo1

import akka.actor.OneForOneStrategy
import akka.actor.SupervisorStrategy
import akka.actor.ActorLogging
import akka.pattern.BackoffSupervisor
import akka.actor.Actor
import akka.pattern.Backoff
import scala.concurrent.duration._
import akka.actor.actorRef2Scala

object Kitchen {
  def kitchenProps = {
    import Chef._
    val options = Backoff.onFailure(Chef.props, "chef", 200 millis, 10 seconds, 0.0)
      .withSupervisorStrategy(OneForOneStrategy(maxNrOfRetries = 4, withinTimeRange = 30 seconds) {
        case _: ChefBusy => SupervisorStrategy.Restart
      })
    BackoffSupervisor.props(options)
  }
}
class Kitchen extends Actor with ActorLogging {
  override def receive: Receive = {
    case x => context.children foreach {child => child ! x}
  }
}