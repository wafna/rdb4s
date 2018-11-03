package wafna.rdb4s.test.nestedset

import org.scalatest.FlatSpec
import wafna.rdb4s.test.nestedset.NestedSetDB.Node
class TestNestedSet extends FlatSpec {
  def makeNodes(names: Seq[String])(implicit db: NestedSetDB): List[Node] = {
    val ids = names map db.createNode
    val nodes = db.fetchNodes(ids)
    assertResult(names.length)(nodes.length)
    assert((nodes.map(_.id).toSet diff ids.toSet).isEmpty)
    assert((nodes.map(_.name).toSet diff names.toSet).isEmpty)
    nodes
  }
  def assertAncestors(node: Node)(expected: Node*)(implicit db: NestedSetDB): Unit = {
    val ancestors = db.fetchAncestors(node.id)
    assertResult(expected.length)(ancestors.length)
    assert(expected.zip(ancestors.reverse).forall(x => x._1.id == x._2.id))
  }
  def setParent(child: Node, parent: Node)(implicit db: NestedSetDB): Unit =
    db.setParent(child.id, parent.id)
  "nested set" should "work and stuff" in {
    NestedSetDB { implicit db =>
      makeNodes(List("a", "b", "c", "d", "e")) match {
        case a :: b :: c :: d :: e :: _ =>
          // As we build the tree we record all the previous tests of ancestry
          // to ensure they remain true.
          var allAncestors: List[(Node, Seq[Node])] = Nil
          def assertNewAncestors(node: Node)(ancestors: Node*): Unit = {
            allAncestors ::= (node, ancestors.toSeq)
            allAncestors foreach { case (n, as) =>
              assertAncestors(n)(as: _*)
            }
          }
          setParent(b, a)
          assertNewAncestors(a)()
          assertNewAncestors(b)(a)
          setParent(c, a)
          assertNewAncestors(c)(a)
          setParent(d, b)
          assertNewAncestors(d)(a, b)
          // move d -> c
          db.moveNode(d.id, c.id)
          assertAncestors(d)(a, c)
          db showNS "----- MOVE d -> c"
          // move d -> b
          db.moveNode(d.id, b.id)
          assertAncestors(a)()
          assertAncestors(b)(a)
          assertAncestors(c)(a)
          assertAncestors(d)(a, b)
          db showNS "----- MOVE d -> b"
          // move d -> a
          db.moveNode(d.id, a.id)
          assertAncestors(a)()
          assertAncestors(b)(a)
          assertAncestors(c)(a)
          assertAncestors(d)(a)
          db showNS "----- MOVE d -> a"
          setParent(e, b)
          db.moveNode(b.id, c.id)
          db showNS "----- MOVE b -> c"
        case _ =>
          sys error "wat"
      }
    }
  }
  "nested set" should "perform many mutations" in {
    NestedSetDB { implicit db =>
      makeNodes(List("a", "b", "c", "d", "e", "f", "g", "h", "i")) match {
        case List(a, b, c, d, e, f, g, h, i) =>
          // As we build the tree we record all the previous tests of ancestry
          // to ensure they remain true.
          var allAncestors: List[(Node, Seq[Node])] = Nil
          def assertNewAncestors(node: Node)(ancestors: Node*): Unit = {
            allAncestors ::= (node, ancestors.toSeq)
            allAncestors foreach { case (n, as) =>
              assertAncestors(n)(as: _*)
            }
          }
          setParent(b, a)
          assertNewAncestors(a)()
          assertNewAncestors(b)(a)
          setParent(c, b)
          assertNewAncestors(c)(a, b)
          setParent(d, c)
          assertNewAncestors(d)(a, b, c)
          setParent(e, a)
          assertNewAncestors(e)(a)
          setParent(f, e)
          assertNewAncestors(f)(a, e)
          setParent(g, e)
          assertNewAncestors(g)(a, e)
          setParent(h, g)
          assertNewAncestors(h)(a, e, g)
          setParent(i, a)
          assertNewAncestors(i)(a)
          db showNS "---"
          println(db.fetchDescendants(c.id))
          println(db.fetchDescendants(b.id))
          println(db.fetchDescendants(e.id))
          println(db.fetchDescendants(a.id))
      }
    }
  }
}
