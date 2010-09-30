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

  def getOutputStream = out
  def getInputStream = in
  def getSocket = socket

  def connected = { getSocket != null }
  def reconnect = { disconnect && connect }

  // Connects the socket, and sets the input and output streams.
  def connect: Boolean = {
    try {
      socket = new Socket(host, port)
      socket.setKeepAlive(true)
      out = getSocket.getOutputStream
      in = new BufferedReader(
             new InputStreamReader(getSocket.getInputStream))
      true
    } catch {
      case _ => clearFd; false;
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
      case _ => false
    }
  }
  
  def clearFd = {
    socket = null
    out = null
    in = null
  }

   // Wraper for the socket write operation.
  def write_to_socket(data: String)(op: OutputStream => Unit) = op(getOutputStream)
  
  // Writes data to a socket using the specified block.
  def write(data: String) = {
    if(!connected) connect;
    write_to_socket(data){
      getSocket =>
        try {
          getSocket.write(data.getBytes)
        } catch {
          case _ => reconnect;
        }
    }
  }

  def readLine: String = {
    try {
      if(!connected) connect;
      getInputStream.readLine
    } catch {
      case x => throw new RuntimeException(x)
    }
  }
}
