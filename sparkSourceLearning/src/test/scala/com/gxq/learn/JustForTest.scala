package com.gxq.learn

object JustForTest extends App {
  trait Logger {
    println("Logger")
  }

  trait FileLogger extends Logger {
    println("FileLgger")
  }

  trait Closeable {
    println("Closeable")
  }

  class Person {
    println("Constructing Person...")
  }

  class Student extends Person with FileLogger with Closeable {
    println("Constructing Student...")
  }
  
  new Student
}