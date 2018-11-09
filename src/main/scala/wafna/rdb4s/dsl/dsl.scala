package wafna.rdb4s
import scala.collection.mutable.ArrayBuffer
import scala.language.implicitConversions
package object dsl {
  /**
    * The name of the field in the schema.
    */
  abstract class Field(val name: String) {
    /**
      * The qualified name when the field is in an aliased table.
      */
    def qname: String
  }
  /**
    * A model for the names of columns in a table.
    *
    * @param tableName The name in the schema.
    * @param alias     The name in the query.
    */
  abstract class Table(val tableName: String, alias: String) {
    implicit def `string to field`(fieldName: String): TField = field(fieldName)
    private var fields = Set[String]()
    case class TField protected(fieldName: String) extends Field(fieldName) {
      override def qname: String = s"$alias.$name"
    }
    /**
      * Defines a table field.
      */
    protected[this] def field(name: String): TField = {
      val f = TField(name)
      if (fields contains name)
        sys error s"Field $name already exists."
      else {
        fields += name
        f
      }
    }
    def qname: String = s"$tableName $alias"
  }
  /**
    * Intermediate syntactic element.
    */
  class SelectFields(val fields: Seq[Field]) {
    def from(table: Table): Select = new Select(fields, table, Nil, None, Nil)
  }
  /**
    * A SELECT statement.
    */
  class Select(val fields: Seq[Field], val table: Table, val joins: List[Join], where: Option[Bool], order: List[SortKey]) extends ShowSQL {
    def join(joinTable: Table, kind: JoinKind = Join.Inner): JoinCondition = new JoinCondition(this, joinTable, kind)
    def innerJoin(joinTable: Table): JoinCondition = join(joinTable, Join.Inner)
    def outerJoin(joinTable: Table): JoinCondition = join(joinTable, Join.Outer)
    def leftJoin(joinTable: Table): JoinCondition = join(joinTable, Join.Left)
    def rightJoin(joinTable: Table): JoinCondition = join(joinTable, Join.Right)
    // Repeated calls are ANDed together.
    def where(cond: Bool): Select =
      new Select(fields, table, joins, Some(where.map(w => Bool.AND(w, cond)).getOrElse(cond)), order)
    def orderBy(sortKeys: SortKey*): Select =
      if (order.nonEmpty) sys error "Ordering already defined."
      else new Select(fields, table, joins, where, sortKeys.toList)
    def sql: (String, List[Any]) = {
      val sql = stringWriter { w =>
        w println s"SELECT ${fields.map(_.qname).mkString(", ")}"
        w println s"FROM ${table.qname}"
        // reverse preserves the syntactic order.
        joins.reverse foreach { j =>
          w println Show.join(j)
        }
        where foreach { c => w println s"WHERE ${Show.bool(c)(Show.FieldNameFQ)}" }
        if (order.nonEmpty) {
          w println s"ORDER BY ${order map Show.sortKey mkString ", "}"
        }
      }
      (sql, where.map(Show.collectCondParams) getOrElse Nil)
    }
  }
  sealed abstract class JoinKind
  object Join {
    case object Inner extends JoinKind
    case object Outer extends JoinKind
    case object Left extends JoinKind
    case object Right extends JoinKind
  }
  case class Join(table: Table, cond: Bool, kind: JoinKind)
  class JoinCondition(projection: Select, table: Table, kind: JoinKind) {
    def on(cond: Bool): Select =
      new Select(projection.fields, projection.table, new Join(table, cond, kind) :: projection.joins, None, Nil)
  }
  sealed trait Value
  object Value {
    case class QField(f: Field) extends Value
    // Literal refers to a query param since we never allow direct embedding of literals in SQL.
    case class Literal(v: Any) extends Value
    case class InList(list: Seq[Any]) extends Value
    object Null extends Value
    object True extends Value
    object False extends Value
    case class ADD(p: Value, q: Value) extends Value
    case class SUB(p: Value, q: Value) extends Value
    case class MUL(p: Value, q: Value) extends Value
    case class DIV(p: Value, q: Value) extends Value
  }
  implicit def `Integer to Value`(i: Int): Value.Literal = Value.Literal(i)
  implicit def `Long to Value`(i: Long): Value.Literal = Value.Literal(i)
  implicit def `String to Value`(i: String): Value.Literal = Value.Literal(i)
  implicit def `Float to Value`(i: Float): Value.Literal = Value.Literal(i)
  implicit def `Double to Value`(i: Double): Value.Literal = Value.Literal(i)
  implicit def `Boolean to Value`(i: Double): Value.Literal = Value.Literal(i)
  /**
    * This is needed to coerce a field to a value for arithmetic that already requires
    * implicits to implement.
    */
  implicit class `Field to Value`(i: Field) {
    // Pronounced 'dammit!'.
    def !(): Value.QField = Value.QField(i)
  }
  implicit class `String to Value Dammit`(i: String) {
    def !(): Value.Literal = Value.Literal(i)
  }
  implicit class `Value arithmetic`(val p: Value) {
    def +(q: Value): Value.ADD = Value.ADD(p, q)
    def -(q: Value): Value.SUB = Value.SUB(p, q)
    def *(q: Value): Value.MUL = Value.MUL(p, q)
    def /(q: Value): Value.DIV = Value.DIV(p, q)
  }
  implicit class `Field comparisons`(val p: Field) {
    // RHS Field
    def ===(q: Field): Pred = Pred.EQ(Value.QField(p), Value.QField(q))
    def !==(q: Field): Pred = Pred.NEQ(Value.QField(p), Value.QField(q))
    def <(q: Field): Pred = Pred.LT(Value.QField(p), Value.QField(q))
    def <=(q: Field): Pred = Pred.LTE(Value.QField(p), Value.QField(q))
    def >(q: Field): Pred = Pred.GT(Value.QField(p), Value.QField(q))
    def >=(q: Field): Pred = Pred.GTE(Value.QField(p), Value.QField(q))
    def like(q: Field): Pred = Pred.Like(Value.QField(p), Value.QField(q))
    // RHS Value
    def ===(q: Value): Pred = Pred.EQ(Value.QField(p), q)
    def !==(q: Value): Pred = Pred.NEQ(Value.QField(p), q)
    def <(q: Value): Pred = Pred.LT(Value.QField(p), q)
    def <=(q: Value): Pred = Pred.LTE(Value.QField(p), q)
    def >(q: Value): Pred = Pred.GT(Value.QField(p), q)
    def >=(q: Value): Pred = Pred.GTE(Value.QField(p), q)
    def like(q: Value): Pred = Pred.Like(Value.QField(p), q)
    // Special RHSs
    def in(list: Seq[Any]): Pred = Pred.In(Value.QField(p), Value.InList(list))
    def like(q: String): Pred = Pred.Like(Value.QField(p), Value.Literal(q))
  }
  implicit class `Value comparisons`(val p: Value) {
    // RHS Field
    def ===(q: Field): Pred = Pred.EQ(p, Value.QField(q))
    def !==(q: Field): Pred = Pred.NEQ(p, Value.QField(q))
    def <(q: Field): Pred = Pred.LT(p, Value.QField(q))
    def <=(q: Field): Pred = Pred.LTE(p, Value.QField(q))
    def >(q: Field): Pred = Pred.GT(p, Value.QField(q))
    def >=(q: Field): Pred = Pred.GTE(p, Value.QField(q))
    def like(q: Field): Pred = Pred.Like(p, Value.QField(q))
    // RHS Value
    def ===(q: Value): Pred = Pred.EQ(p, q)
    def !==(q: Value): Pred = Pred.NEQ(p, q)
    def <(q: Value): Pred = Pred.LT(p, q)
    def <=(q: Value): Pred = Pred.LTE(p, q)
    def >(q: Value): Pred = Pred.GT(p, q)
    def >=(q: Value): Pred = Pred.GTE(p, q)
    def like(q: Value): Pred = Pred.Like(p, q)
    // Special RHSs
    def like(q: String): Pred = Pred.Like(p, Value.Literal(q))
    def in(list: List[Any]): Pred = Pred.In(p, Value.InList(list))
  }
  implicit class `Bool operations`(val p: Bool) {
    def &&(q: Bool): Bool = Bool.AND(p, q)
    def ||(q: Bool): Bool = Bool.OR(p, q)
  }
  val NULL: Value = Value.Null
  val TRUE: Value = Value.True
  val FALSE: Value = Value.False
  def isNull(q: Field): Pred = Pred.IsNull(Value.QField(q))
  def isNotNull(q: Field): Pred = Pred.IsNotNull(Value.QField(q))
  abstract class Bool {
    def unary_! = Bool.NOT(this)
  }
  object Bool {
    case class NOT(b: Bool) extends Bool
    case class AND(p: Bool, q: Bool) extends Bool
    case class OR(p: Bool, q: Bool) extends Bool
  }
  abstract class Pred extends Bool
  object Pred {
    case class EQ(p: Value, q: Value) extends Pred
    case class NEQ(p: Value, q: Value) extends Pred
    case class LT(p: Value, q: Value) extends Pred
    case class GT(p: Value, q: Value) extends Pred
    case class LTE(p: Value, q: Value) extends Pred
    case class GTE(p: Value, q: Value) extends Pred
    case class Like(p: Value, q: Value) extends Pred
    case class In(p: Value, q: Value) extends Pred
    case class IsNull(v: Value) extends Pred
    case class IsNotNull(v: Value) extends Pred
  }
  sealed abstract class SortDirection
  object SortDirection {
    case object ASC extends SortDirection
    case object DESC extends SortDirection
  }
  case class SortKey(field: Field, direction: SortDirection)
  implicit class `field to sort key`(val field: Field) {
    def asc: SortKey = SortKey(field, SortDirection.ASC)
    def desc: SortKey = SortKey(field, SortDirection.DESC)
  }
  trait ShowSQL {
    def sql: (String, List[Any])
  }
  private object Show {
    import Pred._
    import Bool._
    def collectValueParams(arg: Value): List[Any] = arg match {
      case Value.Literal(x) => List(x)
      case Value.InList(x) => x.toList
      case Value.ADD(p, q) => collectValueParams(p) ++ collectValueParams(q)
      case Value.SUB(p, q) => collectValueParams(p) ++ collectValueParams(q)
      case Value.MUL(p, q) => collectValueParams(p) ++ collectValueParams(q)
      case Value.DIV(p, q) => collectValueParams(p) ++ collectValueParams(q)
      case _ => Nil
    }
    def collectCondParams(cond: Bool): List[Any] = cond match {
      case c: Pred => c match {
        case EQ(p, q) => collectValueParams(p) ++ collectValueParams(q)
        case NEQ(p, q) => collectValueParams(p) ++ collectValueParams(q)
        case LT(p, q) => collectValueParams(p) ++ collectValueParams(q)
        case LTE(p, q) => collectValueParams(p) ++ collectValueParams(q)
        case GT(p, q) => collectValueParams(p) ++ collectValueParams(q)
        case GTE(p, q) => collectValueParams(p) ++ collectValueParams(q)
        case Like(p, q) => collectValueParams(p) ++ collectValueParams(q)
        case In(p, q) => collectValueParams(p) ++ collectValueParams(q)
      }
      case AND(p, q) => collectCondParams(p) ++ collectCondParams(q)
      case OR(p, q) => collectCondParams(p) ++ collectCondParams(q)
      case NOT(p) => collectCondParams(p)
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
    def value(v: Value)(implicit fn: FieldName): String = v match {
      case Value.QField(f) => fn(f)
      case Value.Null => "NULL"
      case Value.True => "TRUE"
      case Value.False => "FALSE"
      case Value.Literal(_) => "?"
      case Value.InList(list) => s"(${Array.fill(list.length)("?") mkString ", "})"
      case Value.ADD(p, q) => s"(${value(p)} + ${value(q)})"
      case Value.SUB(p, q) => s"(${value(p)} - ${value(q)})"
      case Value.MUL(p, q) => s"(${value(p)} * ${value(q)})"
      case Value.DIV(p, q) => s"(${value(p)} / ${value(q)})"
    }
    def bool(cond: Bool)(implicit fn: FieldName): String = cond match {
      case c: Pred => c match {
        case Pred.EQ(p, q) => s"(${value(p)} = ${value(q)})"
        case Pred.NEQ(p, q) => s"(${value(p)} <> ${value(q)})"
        case Pred.LT(p, q) => s"(${value(p)} < ${value(q)})"
        case Pred.LTE(p, q) => s"(${value(p)} <= ${value(q)})"
        case Pred.GT(p, q) => s"(${value(p)} > ${value(q)})"
        case Pred.GTE(p, q) => s"(${value(p)} >= ${value(q)})"
        case Pred.Like(p, q) => s"(${value(p)} LIKE ${value(q)})"
        case Pred.In(p, q) => s"(${value(p)} IN ${value(q)})"
      }
      case Bool.AND(p, q) => s"(${bool(p)} AND ${bool(q)})"
      case Bool.OR(p, q) => s"(${bool(p)} OR ${bool(q)})"
      case Bool.NOT(p) => s"(NOT ${bool(p)})"
    }
    def join(j: Join): String = {
      val k = j.kind match {
        case Join.Inner => "INNER"
        case Join.Outer => "OUTER"
        case Join.Left => "LEFT"
        case Join.Right => "RIGHT"
      }
      s"$k JOIN ${j.table.qname} ON ${bool(j.cond)(FieldNameFQ)}"
    }
    def sortKey(sk: SortKey): String = {
      val dir = sk.direction match {
        case SortDirection.ASC => "ASC"
        case SortDirection.DESC => "DESC"
      }
      s"${sk.field.qname} $dir"
    }
  }
  class Insert(table: Table, fields: List[(Field, Value.Literal)]) extends ShowSQL {
    def sql: (String, List[Any]) =
      (s"INSERT INTO ${table.tableName} (${fields.map(_._1.name) mkString ", "}) VALUES (${List.fill(fields.size)("?") mkString ", "})",
          fields.map(_._2.v))
  }
  class Update(table: Table, fields: List[(Field, Value)], where: Bool) extends ShowSQL {
    def sql: (String, List[Any]) =
      (s"UPDATE ${table.tableName} SET ${fields.map(f => s"${f._1.name} = ${Show.value(f._2)(Show.FieldNamePlain)}") mkString ", "} WHERE ${Show.bool(where)(Show.FieldNamePlain)}",
          fields.foldRight(List[Any]())((q, p) => Show.collectValueParams(q._2) ++ p) ++ Show.collectCondParams(where))
    // Repeated calls are ANDed together.
    def where(cond: Bool): Update = new Update(table, fields, Bool.AND(where, cond))
  }
  // By not including a conversion to SQL we preclude accidental global updates.
  class UpdateWhere(table: Table, fields: List[(Field, Value)]) {
    def where(cond: Bool): Update = new Update(table, fields, cond)
  }
  class Delete(table: Table, where: Bool) extends ShowSQL {
    def sql: (String, List[Any]) = (s"DELETE FROM ${table.tableName} WHERE ${Show.bool(where)(Show.FieldNamePlain)}",
        Show.collectCondParams(where))
  }
  implicit class `param list concat head`(val p: (Field, Any)) {
    def ?(q: (Field, Any)): ArrayBuffer[(Field, Any)] = {
      val b = new ArrayBuffer[(Field, Any)](20)
      b append p
      b append q
      b
    }
  }
  implicit class `param list concat tail`(val p: ArrayBuffer[(Field, Any)]) {
    def ?(q: (Field, Any)): ArrayBuffer[(Field, Any)] = {
      p append q
      p
    }
  }
  implicit def `param to param list`(p: (Field, Any)): ArrayBuffer[(Field, Any)] = {
    val b = new ArrayBuffer[(Field, Any)]
    b append p
    b
  }
  /**
    * Provides conversion to SQL string plus parameters.
    */
  implicit def `show sql`(showSQL: ShowSQL): (String, List[Any]) = showSQL.sql
  def select(fields: Field*): SelectFields =
    new SelectFields(fields.toSeq)
  def insert(table: Table)(fields: (Field, Any)*): Insert =
    new Insert(table, fields.toList.map(f => f._1 -> Value.Literal(f._2)))
  def update(table: Table)(fields: (Field, Value)*): UpdateWhere =
    new UpdateWhere(table, fields.toList)
  def delete(table: Table)(where: Bool): Delete =
    new Delete(table, where)
  private def stringWriter(use: java.io.PrintWriter => Unit): String = {
    import java.io._
    val buffer = new ByteArrayOutputStream(1024)
    val writer = new PrintWriter(buffer)
    try use(writer) finally writer.close()
    buffer toString java.nio.charset.StandardCharsets.UTF_8.name()
  }
  implicit class `literal Int`(val i: Int) {
    def q: Value = Value.Literal(i)
  }
}
