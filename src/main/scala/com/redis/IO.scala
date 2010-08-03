package com.redis

import java.io._
import java.net.Socket

trait IO {
  val host: String
  val port: Int

  var socket: Socket = null
  var out: OutputStream = null
  var in: BufferedReader = null
  var db: Int = 0

  def connected = { 
    socket != null 
  }
  def reconnect = { 
    disconnect && connect 
  }

  // Connects the socket, and sets the input and output streams.
  def connect: Boolean = {
    try {
      socket = new Socket(host, port)
      socket.setSoTimeout(0)
      socket.setKeepAlive(true)
      out = socket.getOutputStream
      in = new BufferedReader(
             new InputStreamReader(socket.getInputStream))
      true
    } catch {
      case x => 
        clearFd
        throw new RuntimeException(x)
    }
  }
  
  // Disconnects the socket.
  def disconnect: Boolean = {
    try {
      socket.close
      out.close
      in.close
      clearFd
      true
    } catch {
      case x => 
        false
    }
  }
  
  def clearFd = {
    socket = null
    out = null
    in = null
  }

   // Wrapper for the socket write operation.
  def write_to_socket(data: String)(op: OutputStream => Unit) = op(out)
  
  // Writes data to a socket using the specified block.
  def write(data: String) = {
    if(!connected) connect;
    write_to_socket(data){ os =>
      try {
        os.write(data.getBytes)
        os.flush
      } catch {
        case x => 
          reconnect;
      }
    }
  }

  def readLine: String = {
    try {
      if(!connected) connect;
      in.readLine
    } catch {
      case x => {
        throw new RuntimeException(x)
      }
    }
  }

  def readCounted(count: Int): String = {
    try {
      if(!connected) connect;
      val car = new Array[Char](count)
      in.read(car, 0, count)
      car.mkString
    } catch {
      case x => throw new RuntimeException(x)
    }
  }
}
