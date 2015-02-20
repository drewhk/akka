package docs.stream.cookbook

import akka.stream.OverflowStrategy
import akka.stream.scaladsl._
import akka.stream.testkit.StreamTestKit.SubscriberProbe

import scala.collection.immutable
import scala.concurrent.Await
import scala.concurrent.duration._

class RecipeDroppyBroadcast extends RecipeSpec {

  "Recipe for a droppy broadcast" must {
    "work" in {
      val myElements = Source(immutable.Iterable.tabulate(100)(_ + 1))

      val sub1 = SubscriberProbe[Int]()
      val sub2 = SubscriberProbe[Int]()
      val mySink1 = Sink(sub1)
      val mySink2 = Sink(sub2)
      val futureSink = Sink.head[Seq[Int]]
      val mySink3 = Flow[Int].grouped(200).to(futureSink)

      //#droppy-bcast
      // Makes a sink drop elements if too slow
      def droppySink[T, Mat](sink: Sink[T, Mat], bufferSize: Int): Sink[T, Mat] = {
        Flow[T].buffer(bufferSize, OverflowStrategy.dropHead).to(sink, (mf, ms: Mat) => ms)
      }

      val graph = FlowGraph() { implicit builder =>
        import FlowGraph.Implicits._

        val bcast = Broadcast[Int](3)

        myElements ~> bcast.in

        bcast.out(0) ~> droppySink(mySink1, 10)
        bcast.out(1) ~> droppySink(mySink2, 10)
        bcast.out(2) ~> droppySink(mySink3, 10)
      }
      //#droppy-bcast

      // FIXME #16902 Can't seem to wire things up properly in the graph
      //      Await.result(graph.run().get(futureSink), 3.seconds).sum should be(5050)
      //
      //      sub1.expectSubscription().request(10)
      //      sub2.expectSubscription().request(10)
      //
      //      for (i <- 91 to 100) {
      //        sub1.expectNext(i)
      //        sub2.expectNext(i)
      //      }
      //
      //      sub1.expectComplete()
      //      sub2.expectComplete()
      //
    }
  }

}
