/**
 * Copyright (C) 2014 Typesafe Inc. <http://www.typesafe.com>
 */
package docs.stream

import akka.stream.FlowMaterializer
import akka.stream.scaladsl._
import akka.stream.scaladsl.Graphs.{ OutPort, InPort, Ports, FlowPorts }
import akka.stream.testkit.AkkaSpec

import scala.collection.immutable
import scala.concurrent.Await
import scala.concurrent.Future
import scala.concurrent.duration._

class StreamPartialFlowGraphDocSpec extends AkkaSpec {

  implicit val ec = system.dispatcher

  implicit val mat = FlowMaterializer()

  "build with open ports" in {
    // FIXME #16902 This feels cumbersome. Am I missing something? / ban
    // format: OFF
    //#simple-partial-flow-graph
    class MaxOfThreePorts(ip1: InPort[Int], ip2: InPort[Int], ip3: InPort[Int], op: OutPort[Int]) extends Ports {
      val in1 = ip1
      val in2 = ip2
      val in3 = ip3
      val out = op
      override val inlets = immutable.Seq(ip1, ip2, ip3)
      override val outlets = immutable.Seq(op)
      override def deepCopy() =
        new MaxOfThreePorts(new InPort(in1.toString), new InPort(in2.toString), new InPort(in3.toString),
          new OutPort(out.toString))
    }
    val pickMaxOfThree = FlowGraph.partial { implicit b =>
      import FlowGraph.Implicits._

      val zip1 = ZipWith[Int, Int, Int](math.max _)
      val zip2 = ZipWith[Int, Int, Int](math.max _)
      zip1.out ~> zip2.in1
      new MaxOfThreePorts(zip1.in1, zip1.in2, zip2.in2, zip2.out)
    }
    //#simple-partial-flow-graph
    // format: ON
    //#simple-partial-flow-graph

    val resultSink = Sink.head[Int]

    val g = FlowGraph(pickMaxOfThree, resultSink)((mp, ms) => ms) { implicit b =>
      (pm3, sink) =>
        import FlowGraph.Implicits._

        Source.single(1) ~> pm3.in1
        Source.single(2) ~> pm3.in2
        Source.single(3) ~> pm3.in3
        pm3.out ~> sink.inlet
    }

    val max: Future[Int] = g.run()
    Await.result(max, 300.millis) should equal(3)
    //#simple-partial-flow-graph

    // FIXME #16902 simple-partial-flow-graph-import-shorthand no longer applies 
  }

  "build source from partial flow graph" in {
    //#source-from-partial-flow-graph
    val pairs: Source[(Int, Int), Unit] = Source() { implicit b =>
      import FlowGraph.Implicits._

      // prepare graph elements
      val zip = Zip[Int, Int]()
      def ints = Source(() => Iterator.from(1))

      // connect the graph
      ints ~> Flow[Int].filter(_ % 2 != 0) ~> zip.left
      ints ~> Flow[Int].filter(_ % 2 == 0) ~> zip.right

      // expose port
      zip.out
    }

    val firstPair: Future[(Int, Int)] = pairs.runWith(Sink.head)
    //#source-from-partial-flow-graph
    Await.result(firstPair, 300.millis) should equal(1 -> 2)
  }

  "build flow from partial flow graph" in {
    //#flow-from-partial-flow-graph
    val pairUpWithToString = Flow() { implicit b =>
      import FlowGraph.Implicits._

      // prepare graph elements
      val broadcast = Broadcast[Int](2)
      val zip = Zip[Int, String]()

      // connect the graph
      broadcast.out(0) ~> Flow[Int].map(identity) ~> zip.left
      broadcast.out(1) ~> Flow[Int].map(_.toString) ~> zip.right

      // expose ports
      (broadcast.in, zip.out)
    }

    //#flow-from-partial-flow-graph

    // format: OFF
    val (_, matSink: Future[(Int, String)]) =
      //#flow-from-partial-flow-graph
    pairUpWithToString.runWith(Source(List(1)), Sink.head)
    //#flow-from-partial-flow-graph
    // format: ON

    Await.result(matSink, 300.millis) should equal(1 -> "1")
  }
}
