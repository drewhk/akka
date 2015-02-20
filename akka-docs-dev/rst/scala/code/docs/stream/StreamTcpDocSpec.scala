/**
 * Copyright (C) 2014 Typesafe Inc. <http://www.typesafe.com>
 */
package docs.stream

import java.net.InetSocketAddress

import akka.actor.ActorSystem
import akka.stream.scaladsl._
import akka.stream.testkit.AkkaSpec
import akka.util.ByteString
import cookbook.RecipeParseLines

import scala.concurrent.Future

class StreamTcpDocSpec extends AkkaSpec {

  implicit val ec = system.dispatcher

  //#setup
  import akka.stream.FlowMaterializer
  import akka.stream.scaladsl.StreamTcp
  import akka.stream.scaladsl.StreamTcp._

  implicit val sys = ActorSystem("stream-tcp-system")
  implicit val mat = FlowMaterializer()
  //#setup

  val localhost = new InetSocketAddress("127.0.0.1", 8888)

  "simple server connection" ignore {
    //#echo-server-simple-bind
    val localhost = new InetSocketAddress("127.0.0.1", 8888)
    //#echo-server-simple-handle
    val connections: Source[IncomingConnection, Future[ServerBinding]] = StreamTcp().bind(localhost)
    //#echo-server-simple-bind

    connections runForeach { connection =>
      println(s"New connection from: ${connection.remoteAddress}")

      val echo = Flow[ByteString]
        .transform(() => RecipeParseLines.parseLines("\n", maximumLineBytes = 256))
        .map(_ ++ "!!!\n")
        .map(ByteString(_))

      connection.handleWith(echo)
    }
    //#echo-server-simple-handle
  }

  "simple repl client" ignore {
    val sys: ActorSystem = ???

    //#repl-client
    val connection: Flow[ByteString, ByteString, Future[OutgoingConnection]] = StreamTcp().outgoingConnection(localhost)

    val repl = Flow[ByteString]
      .transform(() => RecipeParseLines.parseLines("\n", maximumLineBytes = 256))
      .map(text => println("Server: " + text))
      .map(_ => readLine("> "))
      .map {
        case "q" =>
          sys.shutdown(); ByteString("BYE")
        case text => ByteString(s"$text")
      }

    connection.join(repl)
    //#repl-client
  }

  "initial server banner echo server" ignore {
    val connections = StreamTcp().bind(localhost)

    //#welcome-banner-chat-server
    connections runForeach { connection =>

      val serverLogic = Flow() { implicit b =>
        import FlowGraph.Implicits._

        val welcomeMsg =
          s"""|Welcome to: ${connection.localAddress}!
              |You are: ${connection.remoteAddress}!""".stripMargin

        val welcome = Source.single(ByteString(welcomeMsg))
        val echo = Flow[ByteString]
          .transform(() => RecipeParseLines.parseLines("\n", maximumLineBytes = 256))
          .map(_ ++ "!!!")
          .map(ByteString(_))

        val concat = Concat[ByteString]()
        // first we emit the welcome message,
        welcome ~> concat.first
        // then we continue using the echo-logic Flow
        echo.outlet ~> concat.second

        (echo.inlet, concat.out)
      }

      connection.handleWith(serverLogic)
    }
    //#welcome-banner-chat-server

  }
}
