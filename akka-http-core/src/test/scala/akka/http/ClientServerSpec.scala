/**
 * Copyright (C) 2009-2014 Typesafe Inc. <http://www.typesafe.com>
 */

package akka.http

import java.io.{ BufferedReader, BufferedWriter, InputStreamReader, OutputStreamWriter }
import java.net.Socket
import akka.stream.impl.{ PublisherSink, SubscriberSource }
import com.typesafe.config.{ Config, ConfigFactory }
import scala.annotation.tailrec
import scala.concurrent.Await
import scala.concurrent.duration._
import org.scalatest.{ BeforeAndAfterAll, Matchers, WordSpec }
import akka.actor.ActorSystem
import akka.stream.scaladsl._
import akka.stream.BindFailedException
import akka.stream.ActorFlowMaterializer
import akka.stream.testkit.StreamTestKit
import akka.stream.testkit.StreamTestKit.{ PublisherProbe, SubscriberProbe }
import akka.http.engine.client.ClientConnectionSettings
import akka.http.engine.server.ServerSettings
import akka.http.model._
import akka.http.util._
import headers._
import HttpEntity._
import HttpMethods._
import TestUtils._

class ClientServerSpec extends WordSpec with Matchers with BeforeAndAfterAll {
  val testConf: Config = ConfigFactory.parseString("""
    akka.event-handlers = ["akka.testkit.TestEventListener"]
    akka.loglevel = WARNING""")
  implicit val system = ActorSystem(getClass.getSimpleName, testConf)
  import system.dispatcher

  implicit val materializer = ActorFlowMaterializer()

  "The low-level HTTP infrastructure" should {

    "properly bind a server" in {
      val (hostname, port) = temporaryServerHostnameAndPort()
      val probe = StreamTestKit.SubscriberProbe[Http.IncomingConnection]()
      val binding = Http().bind(hostname, port).runWith(Sink(probe))
      val sub = probe.expectSubscription() // if we get it we are bound
      val address = Await.result(binding.localAddress(mm), 1.second)
      sub.cancel()
    }

    "report failure if bind fails" in {
      val (hostname, port) = temporaryServerHostnameAndPort()
      val binding = Http().bind(hostname, port)
      val probe1 = StreamTestKit.SubscriberProbe[Http.IncomingConnection]()
      val b1 = Await.result(binding.toMat(Sink(probe1))(Keep.left).run(), 3.seconds)
      probe1.expectSubscription()

      // Bind succeeded, we have a local address
      Await.result(binding), 1.second)

      val probe2 = StreamTestKit.SubscriberProbe[Http.IncomingConnection]()
      val mm2 = binding.connections.to(Sink(probe2)).run()
      probe2.expectErrorOrSubscriptionFollowedByError()

      val probe3 = StreamTestKit.SubscriberProbe[Http.IncomingConnection]()
      val mm3 = binding.connections.to(Sink(probe3)).run()
      probe3.expectErrorOrSubscriptionFollowedByError()

      an[BindFailedException] shouldBe thrownBy { Await.result(binding.localAddress(mm2), 1.second) }
      an[BindFailedException] shouldBe thrownBy { Await.result(binding.localAddress(mm3), 1.second) }

      // The unbind should NOT fail even though the bind failed.
      Await.result(binding.unbind(mm2), 1.second)
      Await.result(binding.unbind(mm3), 1.second)

      // Now unbind the first
      Await.result(binding.unbind(mm1), 1.second)
      probe1.expectComplete()

      if (!akka.util.Helpers.isWindows) {
        val probe4 = StreamTestKit.SubscriberProbe[Http.IncomingConnection]()
        val mm4 = binding.connections.to(Sink(probe4)).run()
        probe4.expectSubscription()

        // Bind succeeded, we have a local address
        Await.result(binding.localAddress(mm4), 1.second)
        // clean up
        Await.result(binding.unbind(mm4), 1.second)
      }
    }

    "properly complete a simple request/response cycle" in new TestSetup {
      val (clientOut, clientIn) = openNewClientConnection()
      val (serverIn, serverOut) = acceptConnection()

      val clientOutSub = clientOut.expectSubscription()
      clientOutSub.expectRequest()
      clientOutSub.sendNext(HttpRequest(uri = "/abc"))

      val serverInSub = serverIn.expectSubscription()
      serverInSub.request(1)
      serverIn.expectNext().uri shouldEqual Uri(s"http://$hostname:$port/abc")

      val serverOutSub = serverOut.expectSubscription()
      serverOutSub.expectRequest()
      serverOutSub.sendNext(HttpResponse(entity = "yeah"))

      val clientInSub = clientIn.expectSubscription()
      clientInSub.request(1)
      val response = clientIn.expectNext()
      toStrict(response.entity) shouldEqual HttpEntity("yeah")

      clientOutSub.sendComplete()
      serverInSub.request(1) // work-around for #16552
      serverIn.expectComplete()
      serverOutSub.expectCancellation()
      clientInSub.request(1) // work-around for #16552
      clientIn.expectComplete()
    }

    "properly complete a chunked request/response cycle" in new TestSetup {
      val (clientOut, clientIn) = openNewClientConnection()
      val (serverIn, serverOut) = acceptConnection()

      val chunks = List(Chunk("abc"), Chunk("defg"), Chunk("hijkl"), LastChunk)
      val chunkedContentType: ContentType = MediaTypes.`application/base64`
      val chunkedEntity = HttpEntity.Chunked(chunkedContentType, Source(chunks))

      val clientOutSub = clientOut.expectSubscription()
      clientOutSub.sendNext(HttpRequest(POST, "/chunked", List(Accept(MediaRanges.`*/*`)), chunkedEntity))

      val serverInSub = serverIn.expectSubscription()
      serverInSub.request(1)
      private val HttpRequest(POST, uri, List(Accept(Seq(MediaRanges.`*/*`)), Host(_, _), `User-Agent`(_)),
        Chunked(`chunkedContentType`, chunkStream), HttpProtocols.`HTTP/1.1`) = serverIn.expectNext()
      uri shouldEqual Uri(s"http://$hostname:$port/chunked")
      Await.result(chunkStream.grouped(4).runWith(Sink.head()), 100.millis) shouldEqual chunks

      val serverOutSub = serverOut.expectSubscription()
      serverOutSub.expectRequest()
      serverOutSub.sendNext(HttpResponse(206, List(Age(42)), chunkedEntity))

      val clientInSub = clientIn.expectSubscription()
      clientInSub.request(1)
      val HttpResponse(StatusCodes.PartialContent, List(Age(42), Server(_), Date(_)),
        Chunked(`chunkedContentType`, chunkStream2), HttpProtocols.`HTTP/1.1`) = clientIn.expectNext()
      Await.result(chunkStream2.grouped(1000).runWith(Sink.head()), 100.millis) shouldEqual chunks

      clientOutSub.sendComplete()
      serverInSub.request(1) // work-around for #16552
      serverIn.expectComplete()
      serverOutSub.expectCancellation()
      clientInSub.request(1) // work-around for #16552
      clientIn.expectComplete()
    }

    "be able to deal with eager closing of the request stream on the client side" in new TestSetup {
      val (clientOut, clientIn) = openNewClientConnection()
      val (serverIn, serverOut) = acceptConnection()

      val clientOutSub = clientOut.expectSubscription()
      clientOutSub.sendNext(HttpRequest(uri = "/abc"))
      clientOutSub.sendComplete() // complete early

      val serverInSub = serverIn.expectSubscription()
      serverInSub.request(1)
      serverIn.expectNext().uri shouldEqual Uri(s"http://$hostname:$port/abc")

      val serverOutSub = serverOut.expectSubscription()
      serverOutSub.expectRequest()
      serverOutSub.sendNext(HttpResponse(entity = "yeah"))

      val clientInSub = clientIn.expectSubscription()
      clientInSub.request(1)
      val response = clientIn.expectNext()
      toStrict(response.entity) shouldEqual HttpEntity("yeah")

      serverInSub.request(1) // work-around for #16552
      serverIn.expectComplete()
      serverOutSub.expectCancellation()
      clientInSub.request(1) // work-around for #16552
      clientIn.expectComplete()
    }
  }

  override def afterAll() = system.shutdown()

  class TestSetup {
    val (hostname, port) = temporaryServerHostnameAndPort()
    def configOverrides = ""

    // automatically bind a server
    val connSource = {
      val settings = configOverrides.toOption.map(ServerSettings.apply)
      val binding = Http().bind(hostname, port, settings = settings)
      val probe = StreamTestKit.SubscriberProbe[Http.IncomingConnection]
      binding.runWith(Sink(probe))
      probe
    }
    val connSourceSub = connSource.expectSubscription()

    def openNewClientConnection(settings: Option[ClientConnectionSettings] = None): (PublisherProbe[HttpRequest], SubscriberProbe[HttpResponse]) = {
      val requestPublisherProbe = StreamTestKit.PublisherProbe[HttpRequest]()
      val responseSubscriberProbe = StreamTestKit.SubscriberProbe[HttpResponse]()

      val connectionFuture = Source(requestPublisherProbe)
        .viaMat(Http().outgoingConnection(hostname, port, settings = settings))(Keep.right)
        .to(Sink(responseSubscriberProbe)).run()

      val connection = Await.result(connectionFuture, 3.seconds)

      connection.remoteAddress.getHostName shouldEqual hostname
      connection.remoteAddress.getPort shouldEqual port
      requestPublisherProbe -> responseSubscriberProbe
    }

    def acceptConnection(): (SubscriberProbe[HttpRequest], PublisherProbe[HttpResponse]) = {
      connSourceSub.request(1)
      val incomingConnection = connSource.expectNext()
      val sink = Sink.publisher[HttpRequest]
      val source = Source.subscriber[HttpResponse]

      val handler = Flow(sink, source)(Keep.both) { implicit b ⇒
        (snk, src) ⇒
          (snk.inlet, src.outlet)
      }

      val (pub, sub) = incomingConnection.handleWith(handler)
      val requestSubscriberProbe = StreamTestKit.SubscriberProbe[HttpRequest]()
      val responsePublisherProbe = StreamTestKit.PublisherProbe[HttpResponse]()

      pub.subscribe(requestSubscriberProbe)
      responsePublisherProbe.subscribe(sub)
      requestSubscriberProbe -> responsePublisherProbe
    }

    def openClientSocket() = new Socket(hostname, port)

    def write(socket: Socket, data: String) = {
      val writer = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream))
      writer.write(data)
      writer.flush()
      writer
    }

    def readAll(socket: Socket)(reader: BufferedReader = new BufferedReader(new InputStreamReader(socket.getInputStream))): (String, BufferedReader) = {
      val sb = new java.lang.StringBuilder
      val cbuf = new Array[Char](256)
      @tailrec def drain(): (String, BufferedReader) = reader.read(cbuf) match {
        case -1 ⇒ sb.toString -> reader
        case n  ⇒ sb.append(cbuf, 0, n); drain()
      }
      drain()
    }
  }

  def toStrict(entity: HttpEntity): HttpEntity.Strict = Await.result(entity.toStrict(500.millis), 1.second)
}
