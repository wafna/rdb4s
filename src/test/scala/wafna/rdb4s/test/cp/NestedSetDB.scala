package wafna.rdb4s.test.cp
import wafna.rdb4s.db.ConnectionPool
import wafna.rdb4s.db.RDB.Connection
import wafna.rdb4s.dsl._
import wafna.rdb4s.test.HSQL
import scala.concurrent.duration._
object NestedSetDB {
  class TNode(alias: String) extends Table("node", alias) {
    val id: TField = "id"
    val name: TField = "name"
  }
  class TNodeTreeSet(alias: String) extends Table("node_tree", alias) {
    val nodeId: TField = "node_id"
    val left: TField = "lft"
    val right: TField = "rgt"
  }
  case class Node(id: Int, name: String)
  object n extends TNode("n")
  object nt extends TNodeTreeSet("nt")
  val timeout: FiniteDuration = 1.second
  def apply(borrow: NestedSetDB => Unit): Unit = {
    val dbConfig = new ConnectionPool.Config().name("nested-set")
        .connectionTestTimeout(0).connectionTestCycleLength(1.hour)
        .idleTimeout(1.hour).maxQueueSize(1000).maxPoolSize(4).minPoolSize(1)
    HSQL("nested-set", dbConfig) { db =>
      db blockCommit { tx =>
        Array(
          """create table node (
            |  id integer identity primary key,
            |  name varchar(32) not null
          )""".stripMargin,
          """create table node_tree (
            |  node_id integer not null foreign key references node (id),
            |  lft integer not null,
            |  rgt integer not null,
            |  constraint unique_rode unique (node_id),
            |  constraint unique_lft unique (lft),
            |  constraint unique_rgt unique (rgt)
            |)""".stripMargin
        ).foreach(tx.mutate(_, Nil))
      } reflect timeout
      borrow(new NestedSetDB(db))
    }
  }
}
class NestedSetDB private(db: HSQL.DB) {
  import NestedSetDB._
  def createNode(name: String): Int = db blockCommit { tx =>
    tx.mutate(insert(n)(n.name -> name))
    tx.lastInsertId()
  } reflect timeout
  def fetchNodes(ids: Seq[Int]): List[Node] = db autoCommit { cx =>
    cx.query(
      select(n.id, n.name).from(n).where(n.id in ids))(
      r => Node(r.int.get, r.string.get))
  } reflect timeout
  def setParent(childNode: Int, parentNode: Int): Unit = db blockCommit { tx =>
    // First we have to either find the parent in the tree or establish it as the root if the tree is empty.
    // The result is the right value of the parent.
    val right: Int = tx.query(
      select(nt.right).from(nt).where(nt.nodeId === parentNode))(_.int.get).headOption getOrElse {
      tx.query(select(nt.nodeId).from(nt).where(nt.left === 1))(_.int.get).headOption match {
        case None =>
          tx.mutate(insert(nt)((nt.nodeId, parentNode) ? (nt.left, 1) ? (nt.right, 2)))
          2
        case Some(root) =>
          sys error s"Parent $parentNode is not in the tree and a root has already been established at $root"
      }
    }
    tx.mutate(update(nt)(nt.left -> (nt.left.! + 2)).where(nt.left > right))
    tx.mutate(update(nt)(nt.right -> (nt.right.! + 2)).where(nt.right >= right))
    tx.mutate(insert(nt)((nt.nodeId, childNode) ? (nt.left, right) ? (nt.right, right + 1)))
  } reflect timeout
  def bounds(nodeId: Int)(implicit cx: HSQL.Connection): (Int, Int) = {
    cx.query(select(nt.left, nt.right).from(nt).where(nt.nodeId === nodeId))(r => (r.int.get, r.int.get)).headOption getOrElse sys.error(s"Node $nodeId is an orphan.")
  }
  def rightMost()(implicit cx: HSQL.Connection): Int =
    cx.query(select(nt.right).from(nt).where(nt.left === 1))(_.int.get).head
  /**
    * This is the question we want to answer with maximum efficiency.
    */
  def getAncestors(childNode: Int): List[Node] = db autoCommit { implicit cx =>
    bounds(childNode) match {
      case (left, right) =>
        cx.query(
          select(n.id, n.name).from(n)
              .innerJoin(nt).on(nt.nodeId === n.id)
              .where((nt.left < left) && (nt.right > right))
              .orderBy(nt.left.desc))(
          r => Node(r.int.get, r.string.get))
    }
  } reflect timeout
  def deleteNode(nodeId: Int): Unit = db blockCommit { implicit tx =>
    bounds(nodeId) match {
      case (left, right) =>
        tx.mutate(delete(nt)((nt.left >= left) && (nt.right <= right)))
        val dx = 1 + right - left
        tx.mutate(update(nt)(nt.left -> (nt.left.! - dx)).where(nt.left > right))
        tx.mutate(update(nt)(nt.right -> (nt.right.! - dx)).where(nt.right > right))
    }
  } reflect timeout
  def __showNS(msg: String)(implicit tx: Connection): Unit = {
    case class NodePos(node: Node, left: Int, right: Int)
    import wafna.rdb4s.test.TestUtils._
    println(msg)
    var indent = 0
    var nodeStack = List[NodePos]()
    tx.query(
      select(n.id, n.name, nt.left, nt.right).from(n).innerJoin(nt).on(n.id === nt.nodeId).orderBy(nt.left.asc)) { r =>
      val np = NodePos(Node(r.int.get, r.string.get), r.int.get, r.int.get)
      def setIndent(): Unit = {
        nodeStack.headOption foreach { case NodePos(_, _, right) =>
          if (np.right < right) indent += 1
          else {
            indent -= 1
            nodeStack = nodeStack.tail
            setIndent()
          }
        }
      }
      setIndent()
      nodeStack ::= np
      println(s"${(np.left, np.right) padRight 8} [${np.node.id padLeft 2}] ${"." * (1 + indent)} ${np.node.name}")
    }
  }
  def moveNode(targetNode: Int, destNode: Int): Unit = db blockCommit { implicit tx =>
    val targetBounds = bounds(targetNode)
    val destBounds = bounds(destNode)
    println(s"target bounds $targetBounds")
    println(s"  dest bounds $destBounds")
    if ((targetBounds._1 < destBounds._1) && (targetBounds._2 > destBounds._2))
      sys error s"Recursive move of node prohibited: target = $targetBounds, dest = $destBounds."
    // First, we move the target subtree out of the way
    val outOfBounds = rightMost()
    __showNS(s"--- move")
    tx mutate
        update(nt)(nt.left -> (nt.left.! + outOfBounds), nt.right -> (nt.right.! + outOfBounds)).where((nt.left >= targetBounds._1) && (nt.right <= targetBounds._2))
    __showNS("removed target")
    if (targetBounds._1 < destBounds._1) {
      println("moving DOWN")
      val dx = targetBounds._2 - targetBounds._1 + 1
      tx mutate
          update(nt)(nt.left -> (nt.left.! - dx))
              .where((nt.left > targetBounds._2) && (nt.left < destBounds._2))
      tx mutate
          update(nt)(nt.right -> (nt.right.! - dx))
              .where((nt.right > targetBounds._2) && (nt.right < destBounds._1))
      __showNS("rearrange")
      val inBounds = outOfBounds - dx
      println(s"inBounds $inBounds")
      tx mutate
          update(nt)(nt.left -> (nt.left.! - inBounds), nt.right -> (nt.right.! - inBounds))
              .where(nt.left > outOfBounds)
      __showNS("replace target")
    } else {
      println("moving UP")
      val dx = targetBounds._2 - targetBounds._1 + 1
      tx mutate
          update(nt)(nt.left -> (nt.left.! + dx))
              .where((nt.left > targetBounds._1) && (nt.right < targetBounds._2))
      tx mutate
          update(nt)(nt.right -> (nt.right.! + dx))
              .where((nt.right >= targetBounds._2) && (nt.right < destBounds._2))
      __showNS("rearrange")
      val right = destBounds._2 + (if (targetBounds._2 > destBounds._2) dx else 0)
      println(s">>> right  $right")
      tx mutate
          update(nt)(nt.left -> (right - dx), nt.right -> (right - 1))
              .where(nt.left > outOfBounds)
      __showNS("replace target")
    }
  } reflect timeout
  def showNS(msg: String): Unit = db autoCommit { __showNS(msg)(_) } reflect timeout
}

