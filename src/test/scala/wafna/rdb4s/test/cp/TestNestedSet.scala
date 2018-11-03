package wafna.rdb4s.test.cp
import org.scalatest.FlatSpec
import wafna.rdb4s.test.cp.NestedSetDB.Node
class TestNestedSet extends FlatSpec {
  "nested set" should "work and stuff" in {
    NestedSetDB { db =>
      val names = List("a", "b", "c", "d", "e")
      val nodeIds: List[Int] = names map db.createNode
      val nodes = db.fetchNodes(nodeIds)
      assertResult(names.length)(nodes.length)
      assert((nodes.map(_.id).toSet diff nodeIds.toSet).isEmpty)
      assert((nodes.map(_.name).toSet diff names.toSet).isEmpty)
      def assertAncestors(node: Node, expected: List[Node]): Unit = {
        val ancestors = db.getAncestors(node.id)
        assertResult(expected.length)(ancestors.length)
        assert(expected.zip(ancestors.reverse).forall(x => x._1.id == x._2.id))
      }
      nodes match {
        case a :: b :: c :: d :: e :: _ =>
          db.setParent(b.id, a.id)
          assertAncestors(a, Nil)
          assertAncestors(b, List(a))
          db.setParent(c.id, a.id)
          assertAncestors(a, Nil)
          assertAncestors(b, List(a))
          assertAncestors(c, List(a))
          db.setParent(d.id, b.id)
          assertAncestors(a, Nil)
          assertAncestors(b, List(a))
          assertAncestors(c, List(a))
          assertAncestors(d, List(a, b))
          println("----- DOWN")
          // move down
          db.moveNode(d.id, c.id)
          assertAncestors(a, Nil)
          assertAncestors(b, List(a))
          assertAncestors(c, List(a))
          assertAncestors(d, List(a, c))
          println("----- UP")
          // move up
          db.moveNode(d.id, b.id)
          assertAncestors(a, Nil)
          assertAncestors(b, List(a))
          assertAncestors(c, List(a))
          assertAncestors(d, List(a, b))
          println("----- WAY UP")
          // move way up
          db.moveNode(d.id, a.id)
          assertAncestors(a, Nil)
          assertAncestors(b, List(a))
          assertAncestors(c, List(a))
          assertAncestors(d, List(a))
//          db.setParent(e.id, c.id)
//          assertAncestors(e, List(a, c))
//          db.moveNode(e.id, d.id)
//          db showNS "---"
        //          println("- delete")
        //          deleteNode(n_1_1.id)
        //          flattenSet() foreach println
        case _ =>
          sys error "wat"
      }
    }
  }
}
