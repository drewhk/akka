package akka.stream.scaladsl

import akka.stream.{ FlowMaterializer, MaterializerSettings, FlowShape }
import akka.stream.testkit.AkkaSpec

import scala.concurrent.{ Await, Future }
import scala.concurrent.duration._

class GraphPartialSpec extends AkkaSpec {
  import Graph.Implicits._

  val settings = MaterializerSettings(system)
    .withInputBuffer(initialSize = 2, maxSize = 16)

  implicit val materializer = FlowMaterializer(settings)

  "FlowGraph.partial" must {
    import Graph.Implicits._

    "be able to build and reuse simple partial graphs" in {
      val doubler = Graph.partial() { implicit b ⇒
        val bcast = b.add(Broadcast[Int](2))
        val zip = b.add(ZipWith((a: Int, b: Int) ⇒ a + b))

        bcast.out(0) ~> zip.in0
        bcast.out(1) ~> zip.in1
        FlowShape(bcast.in, zip.out)
      }

      val (_, _, result) = Graph.closed(doubler, doubler, Sink.head[Seq[Int]])(Tuple3.apply) { implicit b ⇒
        (d1, d2, sink) ⇒
          Source(List(1, 2, 3)) ~> d1.inlet
          d1.outlet ~> d2.inlet
          d2.outlet.grouped(100) ~> sink.inlet
      }.run()

      Await.result(result, 3.seconds) should be(List(4, 8, 12))
    }

    "be able to build and reuse simple materializing partial graphs" in {
      val doubler = Graph.partial(Sink.head[Seq[Int]]) { implicit b ⇒
        sink ⇒
          val bcast = b.add(Broadcast[Int](3))
          val zip = b.add(ZipWith((a: Int, b: Int) ⇒ a + b))

          bcast.out(0) ~> zip.in0
          bcast.out(1) ~> zip.in1
          bcast.out(2).grouped(100) ~> sink.inlet
          FlowShape(bcast.in, zip.out)
      }

      val (sub1, sub2, result) = Graph.closed(doubler, doubler, Sink.head[Seq[Int]])(Tuple3.apply) { implicit b ⇒
        (d1, d2, sink) ⇒
          Source(List(1, 2, 3)) ~> d1.inlet
          d1.outlet ~> d2.inlet
          d2.outlet.grouped(100) ~> sink.inlet
      }.run()

      Await.result(result, 3.seconds) should be(List(4, 8, 12))
      Await.result(sub1, 3.seconds) should be(List(1, 2, 3))
      Await.result(sub2, 3.seconds) should be(List(2, 4, 6))
    }

    "be able to build and reuse complex materializing partial graphs" in {
      val summer = Sink.fold[Int, Int](0)(_ + _)

      val doubler = Graph.partial(summer, summer)(Tuple2.apply) { implicit b ⇒
        (s1, s2) ⇒
          val bcast = b.add(Broadcast[Int](3))
          val bcast2 = b.add(Broadcast[Int](2))
          val zip = b.add(ZipWith((a: Int, b: Int) ⇒ a + b))

          bcast.out(0) ~> zip.in0
          bcast.out(1) ~> zip.in1
          bcast.out(2) ~> s1.inlet

          zip.out ~> bcast2.in
          bcast2.out(0) ~> s2.inlet

          FlowShape(bcast.in, bcast2.out(1))
      }

      val (sub1, sub2, result) = Graph.closed(doubler, doubler, Sink.head[Seq[Int]])(Tuple3.apply) { implicit b ⇒
        (d1, d2, sink) ⇒
          Source(List(1, 2, 3)) ~> d1.inlet
          d1.outlet ~> d2.inlet
          d2.outlet.grouped(100) ~> sink.inlet
      }.run()

      Await.result(result, 3.seconds) should be(List(4, 8, 12))
      Await.result(sub1._1, 3.seconds) should be(6)
      Await.result(sub1._2, 3.seconds) should be(12)
      Await.result(sub2._1, 3.seconds) should be(12)
      Await.result(sub2._2, 3.seconds) should be(24)
    }

    "be able to expose the ports of imported graphs" in {
      val p = Graph.partial(Flow[Int].map(_ + 1)) { implicit b ⇒
        flow ⇒
          FlowShape(flow.inlet, flow.outlet)
      }

      val fut = Graph.closed(Sink.head[Int], p)(Keep.left) { implicit b ⇒
        (sink, flow) ⇒
          import Graph.Implicits._
          Source.single(0) ~> flow.inlet
          flow.outlet ~> sink.inlet
      }.run()

      Await.result(fut, 3.seconds) should be(0)

    }
  }

}
