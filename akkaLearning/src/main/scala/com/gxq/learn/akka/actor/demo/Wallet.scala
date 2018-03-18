package com.gxq.learn.akka.actor.demo

import akka.actor.Props
import akka.actor.AbstractActor.Receive
import akka.actor.Actor
import com.typesafe.config.Config
import akka.actor.ActorSystem
import akka.dispatch.UnboundedPriorityMailbox
import akka.actor.PoisonPill
import akka.dispatch.PriorityGenerator

class PriorityMailbox(settings: ActorSystem.Settings, config: Config)
  extends UnboundedPriorityMailbox(
    PriorityGenerator {
      case Wallet.ZipUp        => 0
      case Wallet.UnZip        => 0
      case Wallet.PutIn(_)     => 0
      case Wallet.DrawOut(_)   => 2
      case Wallet.CheckBalance => 4
      case PoisonPill          => 4
      case otherwise           => 4
    })

class Wallet extends Actor {
  import Wallet._
  var balance: Double = 0
  var zipped: Boolean = true

  override def receive: Receive = {
    case ZipUp =>
      zipped = true
      println("Zipping up wallet.")
    case UnZip =>
      zipped = false
      println("Unzipping wallet.")
    case PutIn(amt) =>
      if (zipped) {
        self ! UnZip //无论如何都要把钱存入
        self ! PutIn(amt)
      } else {
        balance += amt
        println(s"$amt put-in wallet.")
      }

    case DrawOut(amt) =>
      if (zipped) //如果钱包没有打开就算了
        println("Wallet zipped, Cannot draw out!")
      else if ((balance - amt) < 0)
        println(s"$amt is too much, not enough in wallet!")
      else {
        balance -= amt
        println(s"$amt drawn out of wallet.")
      }

    case CheckBalance => println(s"You have $balance in your wallet.")
  }
}

object Wallet {
  sealed trait WalletMsg
  case object ZipUp extends WalletMsg //锁钱包
  case object UnZip extends WalletMsg //开钱包
  case class PutIn(amt: Double) extends WalletMsg //存入
  case class DrawOut(amt: Double) extends WalletMsg //取出
  case object CheckBalance extends WalletMsg //查看余额

  def props = Props(new Wallet)
}