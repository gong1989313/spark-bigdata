package com.gxq.learn.akka.actor.demo

import akka.actor.ActorSystem
import com.typesafe.config.ConfigFactory
import akka.actor.Props

object Actor101 extends App {
  // val system = ActorSystem("actor101-demo", ConfigFactory.load)
  //val wallet = system.actorOf(Wallet.props.withDispatcher("prio-dispatcher"), "mean-wallet")
  val system = ActorSystem("actor101-demo")
  val wallet = system.actorOf(Props[Wallet], name = "helloActor")

  wallet ! Wallet.UnZip
  wallet ! Wallet.PutIn(10.50)
  wallet ! Wallet.PutIn(20.30)
  wallet ! Wallet.DrawOut(10.00)
  wallet ! Wallet.ZipUp
  wallet ! Wallet.PutIn(100.00)
  wallet ! Wallet.CheckBalance

  Thread.sleep(1000)
  system.terminate()

}