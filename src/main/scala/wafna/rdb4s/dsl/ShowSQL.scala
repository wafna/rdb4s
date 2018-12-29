package wafna.rdb4s.dsl
import java.io.PrintWriter
import wafna.rdb4s.db.JSONB
import scala.collection.mutable.ArrayBuffer
/**
  * For emitting SQL.
  */
object ShowSQL {
  def intercalate[T](items: List[T], sep: T): List[T] = items.foldRight(List[T]()) { (b, a) =>
    b :: (if (a.isEmpty) a else sep :: a)
  }
  def stringWriter(use: java.io.PrintWriter => Unit): String = {
    import java.io._
    val buffer = new ByteArrayOutputStream(1024)
    val writer = new PrintWriter(buffer)
    try use(writer) finally writer.close()
    buffer toString java.nio.charset.StandardCharsets.UTF_8.name()
  }
  def apply(f: ShowSQL => Unit): (String, List[Any]) = {
    val params = new ArrayBuffer[Any]()
    stringWriter(w => f(new ShowSQL(w, params))) -> params.toList
  }
}
class ShowSQL private(sql: PrintWriter, params: ArrayBuffer[Any]) {
  import ShowSQL._
  def showUpdate(u: Update): Unit = {
    sql print s"UPDATE ${u.table.tableName} SET "
    intercalate(u.fields.map(Some(_)), None) foreach {
      case None => sql print ", "
      case Some(v) =>
        sql print s" ${v._1.name} = "
        showValue(v._2)(FieldNamePlain)
    }
    sql print " WHERE "
    showBool(u.where)(FieldNamePlain)
  }
  def showInsert(i: Insert): Unit = {
    sql print s"INSERT INTO ${i.table.tableName} (${i.fields.map(_._1.name) mkString ", "})" +
        s" VALUES (${i.fields.map(p => showLiteralParam(p._2.v)) mkString ", "})"
    params ++= i.fields.map(_._2.v)
  }
  def showDelete(d: Delete): Unit = {
    sql print s"DELETE FROM ${d.table.tableName} WHERE "
    showBool(d.where)(FieldNamePlain)
  }
  def showSelection(s: Selection)(implicit p: FieldName): Unit = {
    showValue(s.f)
    s.name.foreach(n => sql print s" as $n")
  }
  def showSelect(s: Select): Unit = {
    sql print s"SELECT "
    if (s.distinct)
      sql print s"DISTINCT "
    intercalate(s.selections.toList.map(Some(_)), None) foreach {
      case None => sql print ", "
      case Some(v) => showSelection(v)(FieldNameFQ)
    }
    sql print s" FROM "
    intercalate(s.tables.toList.map(Some(_)), None) foreach {
      case None => sql print ", "
      case Some(v) => showRelation(v)
    }
    // reverse preserves the syntactic order.
    s.joins.reverse foreach { j =>
      showJoin(j)
    }
    if (s.whereClause.nonEmpty) {
      sql print " WHERE "
      intercalate(s.whereClause.toList.map(Some(_)), None) foreach {
        case None => " AND "
        case Some(e) =>
          showBool(e)(FieldNameFQ)
      }
    }
    if (s.order.nonEmpty) {
      sql print " ORDER BY "
      intercalate(s.order.map(Some(_)), None) foreach {
        case None => sql print ", "
        case Some(e) => showSortKey(e)
      }
    }
    if (s.group.nonEmpty) {
      sql print s" GROUP BY ${s.group.map(_.qname) mkString ", "}"
    }
  }
  def showRelation(t: Relation): Unit = t match {
    case ts: SubQuery =>
      sql print "("
      showSelect(ts.select)
      sql print s") ${ts.alias}"
    case t: Table =>
      sql print s"${t.tableName} ${t.alias}"
  }
  /**
    * Because we always use aliased tables in updates and queries we need to indicate
    * whether to use the base name or qualified name when we emit the SQL.
    * Otherwise, we'd need to have different logic for printing essentially the same stuff.
    */
  sealed trait FieldName {
    def apply(field: Field): String
  }
  final object FieldNameFQ extends FieldName {
    def apply(field: Field): String = field.qname
  }
  final object FieldNamePlain extends FieldName {
    def apply(field: Field): String = field.name
  }
  def showBinOp(p: Value, q: Value, op: String)(implicit fn: FieldName): Unit = {
    sql print "("
    showValue(p)
    sql print s" $op "
    showValue(q)
    sql print ")"
  }
  /**
    * Special handling for query params.
    */
  def showLiteralParam(x: Any): String = x match {
    case JSONB(_) =>
      "? :: JSONB"
    case _ => "?"
  }
  def showValue(v: Value)(implicit fn: FieldName): Unit = v match {
    case f: Field => sql print fn(f)
    case Value.QField(f) => sql print fn(f)
    case Value.Null => sql print "NULL"
    case Value.True => sql print "TRUE"
    case Value.False => sql print "FALSE"
    case Value.Literal(x) =>
      sql print showLiteralParam(x)
      params += x
    case Value.InList(list) =>
      sql print s"(${list.map(showLiteralParam) mkString ", "})"
      params ++= list
    case Value.ADD(p, q) => showBinOp(p, q, "+")
    case Value.SUB(p, q) => showBinOp(p, q, "-")
    case Value.MUL(p, q) => showBinOp(p, q, "*")
    case Value.DIV(p, q) => showBinOp(p, q, "/")
    case f: Function =>
      def showF(n: String, v: Value): Unit = {
        sql print n
        sql print "("
        showValue(v)
        sql print ")"
      }
      f match {
        case TableFunction(field) =>
          sql print field.qname
        case a: AggregateFunction => a match {
          case Max(q) => showF("MAX", q)
          case Min(q) => showF("MIN", q)
          case Avg(q) => showF("AVG", q)
        }
        case CustomFunction(n, a) =>
          sql print s"$n("
          showValue(a)(FieldNameFQ)
          sql print ")"
      }
  }
  def showBool(cond: Bool)(implicit fn: FieldName): Unit =
    cond match {
      case c: Pred => c match {
        case Pred.EQ(p, q) => showBinOp(p, q, "=")
        case Pred.NEQ(p, q) => showBinOp(p, q, "<>")
        case Pred.LT(p, q) => showBinOp(p, q, "<")
        case Pred.LTE(p, q) => showBinOp(p, q, "<=")
        case Pred.GT(p, q) => showBinOp(p, q, ">")
        case Pred.GTE(p, q) => showBinOp(p, q, ">=")
        case Pred.Like(p, q) => showBinOp(p, q, "LIKE")
        case Pred.In(p, q) => showBinOp(p, q, "IN")
      }
      case Bool.AND(p, q) =>
        sql print "("
        showBool(p)
        sql print " AND "
        showBool(q)
        sql print ")"
      case Bool.OR(p, q) =>
        sql print "("
        showBool(p)
        sql print " OR "
        showBool(q)
        sql print ")"
      case Bool.NOT(p) =>
        sql print "NOT"
        showBool(p)
      case Bool.BOOL(b) =>
        sql print (if (b) "TRUE" else "FALSE")
    }
  def showJoin(j: Join): Unit = {
    sql print (j.kind match {
      case Join.Inner => " INNER"
      case Join.Outer => " OUTER"
      case Join.Left => " LEFT"
      case Join.Right => " RIGHT"
    })
    sql print " JOIN "
    showRelation(j.table)
    sql print " ON "
    showBool(j.cond)(FieldNameFQ)
  }
  def showSortKey(sk: SortKey): Unit = {
    sql print sk.field.qname
    sql print " "
    sql print (sk.direction match {
      case SortDirection.ASC => "ASC"
      case SortDirection.DESC => "DESC"
    })
  }
}
