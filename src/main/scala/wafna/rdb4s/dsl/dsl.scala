package wafna.rdb4s
import scala.collection.mutable.ArrayBuffer
import scala.language.implicitConversions
package object dsl {
  implicit def `show select`(s: Select): (String, List[Any]) = ShowSQL(_.showSelect(s))
  implicit def `show insert`(s: Insert): (String, List[Any]) = ShowSQL(_.showInsert(s))
  implicit def `show update`(s: Update): (String, List[Any]) = ShowSQL(_.showUpdate(s))
  implicit def `show delete`(s: Delete): (String, List[Any]) = ShowSQL(_.showDelete(s))
  /**
    * The name of a field in a relation.
    */
  abstract class Field(val name: String) {
    /**
      * The qualified name when the field is in an aliased table.
      */
    def qname: String
    def as(alias: String): Selection
  }
  // Every relation has an alias.
  abstract class Relation(val alias: String) {
    case class TField protected(fieldName: String) extends Field(fieldName) {
      override def qname: String = s"$alias.$name"
      override def as(alias: String): Selection = Selection(TableFunction(this), Some(alias))
    }
    implicit def `String to Field`(fieldName: String): TField = field(fieldName)
    private var fields = Set[String]()
    protected[this] def field(name: String): TField = {
      val f = TField(name)
      if (fields contains name)
        sys error s"Field $name already exists."
      else {
        fields += name
        f
      }
    }
  }
  class SubQuery(alias: String, val select: Select) extends Relation(alias)
  implicit class `Select to Relation`(val s: Select) {
    def as(alias: String): SubQuery = new SubQuery(alias, s)
  }
  abstract class Table(val tableName: String, alias: String) extends Relation(alias)
  /**
    * Intermediate syntactic element.
    */
  class SelectFields(val selections: Seq[Selection]) {
    def from(tables: Relation*): Select = new Select(selections, tables, Nil, None, Nil, Nil, None)
  }
  /**
    * A SELECT statement.
    */
  class Select(val selections: Seq[Selection], val tables: Seq[Relation], val joins: List[Join], val whereClause: Option[Bool], val order: List[SortKey], val group: List[Field], val limit: Option[Int]) {
    override def toString: String = ShowSQL(_.showSelect(this))._1
    def join(joinTable: Relation, kind: JoinKind = Join.Inner): JoinCondition = new JoinCondition(this, joinTable, kind)
    def innerJoin(joinTable: Relation): JoinCondition = join(joinTable, Join.Inner)
    def outerJoin(joinTable: Relation): JoinCondition = join(joinTable, Join.Outer)
    def leftJoin(joinTable: Relation): JoinCondition = join(joinTable, Join.Left)
    def rightJoin(joinTable: Relation): JoinCondition = join(joinTable, Join.Right)
    // Repeated calls are ANDed together.
    def where(cond: Bool): Select =
      new Select(selections, tables, joins, Some(whereClause.map(w => Bool.AND(w, cond)).getOrElse(cond)), order, group, limit)
    def orderBy(sortKeys: SortKey*): Select =
      if (order.nonEmpty) sys error "Ordering already defined."
      else new Select(selections, tables, joins, whereClause, sortKeys.toList, group, limit)
    def groupBy(grouping: Field*): Select =
      new Select(selections, tables, joins, whereClause, order, group ++ grouping.toList, limit)
    def limit(n: Int): Select =
      new Select(selections, tables, joins, whereClause, order, group, Some(n))
/*
    def sql: (String, List[Any]) = {
      val sql = stringWriter { w =>
        w println s"SELECT ${selections.map(Show.selection) mkString ", "}"
        w println s"FROM ${tables.map(Show.showRelation) mkString ", "}"
        // reverse preserves the syntactic order.
        joins.reverse foreach { j =>
          w println Show.join(j)
        }
        whereClause foreach { c => w println s"WHERE ${Show.bool(c)(Show.FieldNameFQ)}" }
        if (order.nonEmpty) {
          w println s"ORDER BY ${order map Show.sortKey mkString ", "}"
        }
        if (group.nonEmpty) {
          w println s"GROUP BY ${group.map(_.qname) mkString ", "}"
        }
      }
      (sql, whereClause.map(Show.collectCondParams) getOrElse Nil)
    }
*/
  }
  sealed abstract class JoinKind
  object Join {
    case object Inner extends JoinKind
    case object Outer extends JoinKind
    case object Left extends JoinKind
    case object Right extends JoinKind
  }
  case class Join(table: Relation, cond: Bool, kind: JoinKind)
  class JoinCondition(projection: Select, table: Relation, kind: JoinKind) {
    def on(cond: Bool): Select =
      new Select(projection.selections, projection.tables, new Join(table, cond, kind) :: projection.joins, None, Nil, Nil, None)
  }
  sealed trait Value
  sealed abstract class ArithBinOp(val p: Value, val q: Value) extends Value
  object Value {
    case class QField(f: Field) extends Value
    // Literal refers to a query param since we never allow direct embedding of literals in SQL.
    case class Literal(v: Any) extends Value
    case class InList(list: Seq[Any]) extends Value
    object Null extends Value
    object True extends Value
    object False extends Value
    case class ADD(override val p: Value, override val q: Value) extends ArithBinOp(p, q)
    case class SUB(override val p: Value, override val q: Value) extends ArithBinOp(p, q)
    case class MUL(override val p: Value, override val q: Value) extends ArithBinOp(p, q)
    case class DIV(override val p: Value, override val q: Value) extends ArithBinOp(p, q)
  }

  implicit def `Integer to Value`(i: Int): Value.Literal = Value.Literal(i)
  implicit def `Long to Value`(i: Long): Value.Literal = Value.Literal(i)
  implicit def `String to Value`(i: String): Value.Literal = Value.Literal(i)
  implicit def `Float to Value`(i: Float): Value.Literal = Value.Literal(i)
  implicit def `Double to Value`(i: Double): Value.Literal = Value.Literal(i)
  implicit def `Boolean to Value`(i: Boolean): Value.Literal = Value.Literal(i)
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
    // A literal boolean value.
    case class BOOL(b: Boolean) extends Bool
    case class NOT(b: Bool) extends Bool
    case class AND(p: Bool, q: Bool) extends Bool
    case class OR(p: Bool, q: Bool) extends Bool
  }
  /**
    * Embeds literal booleans.
    */
  implicit def `boolean to bool`(b: Boolean): Bool.BOOL = Bool.BOOL(b)
  /**
    * Predicate
    */
  abstract class Pred extends Bool
  abstract class PredUni(val p: Value) extends Pred
  abstract class PredBin(val p: Value, val q: Value) extends Pred
  object Pred {
    case class EQ(override val p: Value, override val q: Value) extends PredBin(p, q)
    case class NEQ(override val p: Value, override val q: Value) extends PredBin(p, q)
    case class LT(override val p: Value, override val q: Value) extends PredBin(p, q)
    case class GT(override val p: Value, override val q: Value) extends PredBin(p, q)
    case class LTE(override val p: Value, override val q: Value) extends PredBin(p, q)
    case class GTE(override val p: Value, override val q: Value) extends PredBin(p, q)
    case class Like(override val p: Value, override val q: Value) extends PredBin(p, q)
    case class In(override val p: Value, override val q: Value) extends PredBin(p, q)
    case class IsNull(v: Value) extends PredUni(v)
    case class IsNotNull(v: Value) extends PredUni(v)
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
  class Insert(val table: Table, val fields: List[(Field, Value.Literal)]) {
    override def toString: String = ShowSQL(_.showInsert(this))._1
  }
  class Update(val table: Table, val fields: List[(Field, Value)], val where: Bool) {
    override def toString: String = ShowSQL(_.showUpdate(this))._1
    // Repeated calls are ANDed together.
    def where(cond: Bool): Update = new Update(table, fields, Bool.AND(where, cond))
  }
  // By not including a conversion to SQL we preclude accidental global updates.
  class UpdateWhere(table: Table, fields: List[(Field, Value)]) {
    def where(cond: Bool): Update = new Update(table, fields, cond)
  }
  class Delete(val table: Table, val where: Bool) {
    override def toString: String = ShowSQL(_.showDelete(this))._1
  }
  implicit def `param to param list`(p: (Field, Any)): ArrayBuffer[(Field, Any)] = {
    val b = new ArrayBuffer[(Field, Any)]
    b append p
    b
  }
  /**
    * Functions generate values with the most trivial being a field reference.
    */
  sealed abstract class Function
  case class TableFunction(field: Field) extends Function
  implicit def `field to table function`(field: Field): TableFunction = TableFunction(field)
  sealed abstract class AggregateFunction extends Function
  case class Max(f: Function) extends AggregateFunction
  case class Min(f: Function) extends AggregateFunction
  case class Avg(f: Function) extends AggregateFunction
  implicit class `field aggregate functions`(f: Field) {
    def max: Max = Max(f)
    def min: Min = Min(f)
    def avg: Avg = Avg(f)
  }
  case class Selection(f: Function, name: Option[String])
  implicit def `Anonymous Function to Selection`(f: Function): Selection = Selection(f, None)
  implicit def `Field to Selection`(f: Field): Selection = Selection(TableFunction(f), None)
  implicit class `Function to Selection`(val f: Function) {
    def as(name: String): Selection = Selection(f, Some(name))
  }
  /**
    * Provides conversion to SQL string plus parameters.
    */
  // implicit def `show sql`(showSQL: ShowSQL): (String, List[Any]) = showSQL.sql
  def select(selections: Selection*): SelectFields =
    new SelectFields(selections.toSeq)
  def insert(table: Table)(fields: (Field, Any)*): Insert =
    new Insert(table, fields.toList.map(f => f._1 -> Value.Literal(f._2)))
  def update(table: Table)(fields: (Field, Value)*): UpdateWhere =
    new UpdateWhere(table, fields.toList)
  def delete(table: Table)(where: Bool): Delete =
    new Delete(table, where)
  implicit class `literal Int`(val i: Int) {
    def q: Value = Value.Literal(i)
  }
}
