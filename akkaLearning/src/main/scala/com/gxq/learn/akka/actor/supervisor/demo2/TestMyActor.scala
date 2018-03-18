package com.gxq.learn.akka.actor.supervisor.demo2

import akka.actor.ActorSystem
import akka.actor.Props

object TestMyActor extends App {
  val system = ActorSystem("testSystem")
  val parentActor = system.actorOf(Props[Parent],"parentActor")

  parentActor ! "Hello 1"
  parentActor ! "Hello 2"
  parentActor ! "Hello 3"
  parentActor ! "Hello 4"
  parentActor ! "Hello 5"

  Thread.sleep(5000)
  system.terminate()
}