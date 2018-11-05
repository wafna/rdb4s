package wafna.rdb4s.test.nestedset
import wafna.rdb4s.db.ConnectionPool
import wafna.rdb4s.db.RDB.Connection
import wafna.rdb4s.dsl._
import wafna.rdb4s.test.HSQL
import scala.concurrent.duration._
object NestedSetDB {
  // The nesting position.
  case class NPos(left: Int, right: Int) {
    def contains(other: NPos): Boolean = left < other.left && right > other.right
  }
  private case class NodePos(node: Node, pos: NPos)
  // The stuff we're organizing hierarchically.
  case class Node(id: Int, name: String)
  case class NSTree(node: Node, descendants: List[NSTree])
  /*
  Some tables.
   */
  class TNode(alias: String) extends Table("node", alias) {
    val id: TField = "id"
    val name: TField = "name"
  }
  class TNodeTreeSet(alias: String) extends Table("node_tree", alias) {
    val nodeId: TField = "node_id"
    val left: TField = "lft"
    val right: TField = "rgt"
  }
  object n extends TNode("n")
  object nt extends TNodeTreeSet("nt")
  val timeout: FiniteDuration = 1.second
  /**
    * Makes a database and creates the schema.
    */
  def apply(borrow: NestedSetDB => Unit): Unit = {
    val dbConfig = new ConnectionPool.Config().name("nested-set")
        .connectionTestTimeout(0).connectionTestCycleLength(1.hour)
        .idleTimeout(1.hour).maxQueueSize(1000).maxPoolSize(4).minPoolSize(1)
    // The random bit is to prevent unit tests from colliding.
    HSQL(s"nested-set-${java.util.UUID.randomUUID()}", dbConfig) { db =>
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
            |  constraint unique_node unique (node_id),
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
      r => Node(r.int.!, r.string.!))
  } reflect timeout
  def setParent(childNode: Int, parentNode: Int): Unit =
    db.blockCommit(__setParent(childNode, parentNode)(_)) reflect timeout
  def setRoot(nodeId: Int): Unit = db blockCommit { tx =>
    tx.query(select(nt.nodeId).from(nt).where(nt.left === 1))(rs => sys error s"Root node already exists at ${rs.int.!}")
    tx.mutate(insert(nt)(nt.nodeId -> nodeId, nt.left -> 1, nt.right -> 2))
  }
  private def __setParent(childNode: Int, parentNode: Int)(implicit tx: HSQL.Connection): Int = {
    val right: Int = bounds(parentNode).right
    tx.mutate(update(nt)(nt.left -> (nt.left.! + 2)).where(nt.left > right))
    tx.mutate(update(nt)(nt.right -> (nt.right.! + 2)).where(nt.right >= right))
    tx.mutate(insert(nt)(nt.nodeId -> childNode, nt.left -> right, nt.right -> (right + 1)))
  }
  def bounds(nodeId: Int)(implicit cx: HSQL.Connection): NPos =
    cx.query(
      select(nt.left, nt.right).from(nt).where(nt.nodeId === nodeId))(
      r => NPos(r.int.!, r.int.!))
        .headOption getOrElse sys.error(s"Node $nodeId is an orphan.")
  def rightMost()(implicit cx: HSQL.Connection): Int =
    cx.query(select(nt.right).from(nt).where(nt.left === 1))(_.int.!).head
  /**
    * This is the question we want to answer with maximum efficiency.
    */
  def fetchAncestors(childNode: Int): List[Node] = db autoCommit { implicit cx =>
    bounds(childNode) match {
      case NPos(left, right) =>
        cx.query(
          select(n.id, n.name).from(n)
              .innerJoin(nt).on(nt.nodeId === n.id)
              .where((nt.left < left) && (nt.right > right))
              // order desc so that the head of the list is nearest ancestor.
              .orderBy(nt.left.desc))(
          r => Node(r.int.!, r.string.!))
    }
  } reflect timeout
  // Computes the transitive closure from a node.
  private def __fetchDescendants(rootId: Int)(implicit cx: HSQL.Connection): NSTree =
    bounds(rootId) match {
      case NPos(left, right) =>
        var childrenStack = List[(NPos, NSTree)]()
        cx.query(
          select(n.id, n.name, nt.left, nt.right).from(n)
              .innerJoin(nt).on(nt.nodeId === n.id)
              .where((nt.left >= left) && (nt.right <= right))
              // The sort order is very important!
              // We're essentially folding from the right, building the tree from the bottom.
              .orderBy(nt.left.desc)) { r =>
          val np = NodePos(Node(r.int.!, r.string.!), NPos(r.int.!, r.int.!))
          childrenStack.span(p => np.pos.contains(p._1)) match {
            case (children, others) =>
              childrenStack = (np.pos, NSTree(np.node, children.map(_._2))) :: others
          }
        }
        childrenStack match {
          case h :: Nil => h._2
          case _ => sys error "Malformed tree."
        }
    }
  /**
    * This is a transitive closure from some node.
    */
  def fetchDescendants(rootId: Int): NSTree = db.autoCommit(__fetchDescendants(rootId)(_)) reflect timeout
  private def __deleteNode(nodeId: Int)(implicit tx: HSQL.Connection): Unit =
    bounds(nodeId) match {
      case NPos(left, right) =>
        tx.mutate(delete(nt)((nt.left >= left) && (nt.right <= right)))
        val dx = 1 + right - left
        tx.mutate(update(nt)(nt.left -> (nt.left.! - dx)).where(nt.left > right))
        tx.mutate(update(nt)(nt.right -> (nt.right.! - dx)).where(nt.right > right))
    }
  def deleteNode(nodeId: Int): Unit = db.blockCommit(__deleteNode(nodeId)(_)) reflect timeout
  def moveNode(targetNode: Int, destNode: Int): Unit = db blockCommit { implicit tx =>
    // We do this by snipping off the target tree, deleting it, then inserting it.
    // This is fairly expensive and could probably be done in constant queries rather than n queries
    // but the system is already really expensive to mutate.
    val targetTree = __fetchDescendants(targetNode)
    __deleteNode(targetNode)
    def insertNode(parent: Int, t: NSTree): Unit = {
      __setParent(t.node.id, parent)
      t.descendants foreach { n =>
        insertNode(t.node.id, n)
      }
    }
    insertNode(destNode, targetTree)
  } reflect timeout
  // shows a nested set with indenting right off the record set.
  private def __showNS(msg: String)(implicit tx: Connection): Unit = {
    import wafna.rdb4s.test.TestUtils._
    println(msg)
    var indent = 0
    var nodeStack = List[NodePos]()
    tx.query(
      select(n.id, n.name, nt.left, nt.right).from(n)
          .innerJoin(nt).on(n.id === nt.nodeId)
          // this ordering gives us recursive descent.
          .orderBy(nt.left.asc)) { r =>
      val np = NodePos(Node(r.int.!, r.string.!), NPos(r.int.!, r.int.!))
      def setIndent(): Unit = {
        nodeStack.headOption foreach { case NodePos(_, NPos(_, right)) =>
          if (np.pos.right < right) indent += 1
          else {
            indent -= 1
            nodeStack = nodeStack.tail
            setIndent()
          }
        }
      }
      setIndent()
      nodeStack ::= np
      println(s"${np.pos.left padLeft 3} ${np.pos.right padLeft 3}  [${np.node.id padLeft 2}] ${"." * (1 + indent)} ${np.node.name}")
    }
  }
  def showNS(msg: String): Unit = db autoCommit {
    __showNS(msg)(_)
  } reflect timeout
}

