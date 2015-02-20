package docs.stream

import akka.stream.{ OverflowStrategy, FlowMaterializer }
import akka.stream.scaladsl._
import akka.stream.testkit.AkkaSpec

class GraphCyclesSpec extends AkkaSpec {

  implicit val mat = FlowMaterializer()

  // FIXME #16902 Should we remove this for now?
  "Cycle demonstration" must {
    val source = Source(() => Iterator.from(0))

    "include a deadlocked cycle" in {

      // format: OFF
      //#deadlocked
      // WARNING! The graph below deadlocks!
      FlowGraph() { implicit b =>
        import FlowGraph.Implicits._
        //b.allowCycles() FIXME #16902

        val merge = Merge[Int](2)
        val bcast = Broadcast[Int](2)

        source ~> merge.in(0)
                  merge.out ~> Flow[Int].map { (s) => println(s); s } ~> bcast.in
        bcast.out(0) ~> merge.in(1)
        bcast.out(1) ~> Sink.ignore
      }
      //#deadlocked
      // format: ON
    }

    "include an unfair cycle" in {
      // format: OFF
      //#unfair
      // WARNING! The graph below stops consuming from "source" after a few steps
      FlowGraph() { implicit b =>
        import FlowGraph.Implicits._
        // b.allowCycles() FIXME #16902

        val merge = MergePreferred[Int](1)
        val bcast = Broadcast[Int](2)

        source ~> merge.in(0)
                  merge.out ~> Flow[Int].map { (s) => println(s); s } ~> bcast.in
        bcast.out(0) ~> merge.preferred
        bcast.out(1) ~> Sink.ignore
      }
      //#unfair
      // format: ON
    }

    "include a dropping cycle" in {
      // format: OFF
      //#dropping
      FlowGraph() { implicit b =>
        import FlowGraph.Implicits._
        // b.allowCycles() FIXME #16902

        val merge = Merge[Int](2)
        val bcast = Broadcast[Int](2)

        source ~> merge.in(0)
                  merge.out ~> Flow[Int].map { (s) => println(s); s } ~> bcast.in
        bcast.out(0) ~> Flow[Int].buffer(10, OverflowStrategy.dropHead) ~> merge.in(1)
        bcast.out(1) ~> Sink.ignore
      }
      //#dropping
      // format: ON
    }

    "include a dead zipping cycle" in {
      //#zipping-dead
      // WARNING! The graph below never processes any elements
      FlowGraph() { implicit b =>
        import FlowGraph.Implicits._
        // b.allowCycles() FIXME #16902

        val zip = ZipWith[Int, Int, Int]((left, right) => right)
        val bcast = Broadcast[Int](2)

        source ~> zip.in1
        zip.out ~> Flow[Int].map { (s) => println(s); s } ~> bcast.in
        bcast.out(0) ~> zip.in2
        bcast.out(1) ~> Sink.ignore
      }
      //#zipping-dead

    }

    "include a live zipping cycle" in {
      //#zipping-live
      FlowGraph() { implicit b =>
        import FlowGraph.Implicits._
        // b.allowCycles() FIXME #16902

        val zip = ZipWith[Int, Int, Int]((left, right) => left)
        val bcast = Broadcast[Int](2)
        val concat = Concat[Int]()

        source ~> zip.in1
        zip.out ~> Flow[Int].map { (s) => println(s); s } ~> bcast.in
        bcast.out(0) ~> concat.second
        bcast.out(1) ~> zip.in2
        Source.single(0) ~> concat.first
      }
      //#zipping-live

    }

  }

}
