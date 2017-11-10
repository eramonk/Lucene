package com.ra


object Print {
  implicit class Printable[T](obj: T) {
    def print = {
      println(obj.toString())
      obj
    }
  }
}
