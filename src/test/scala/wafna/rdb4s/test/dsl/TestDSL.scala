package wafna.rdb4s.test.dsl
import org.scalatest.FlatSpec
import wafna.rdb4s
class TestDSL extends FlatSpec {
  "dsl" should "emit literal sql" in {
    import rdb4s.dsl._
    class Thing(alias: String) extends Table("something", alias) {
      val id: TField = "id"
      val name: TField = "name"
      val rank: TField = "rank"
      val serialNumber: TField = "serial_number"
    }
    implicit class Straighten(s: String) {
      def straighten(): String = s.replaceAll("[\r\n]+", " ").replaceAll("\\s+", " ").trim
    }
    object thing1 extends Thing("a")
    object thing2 extends Thing("b")
    object thing3 extends Thing("c")
    assertResult(
      """SELECT a.id, a.name, b.id, b.name
        |FROM something a
        |INNER JOIN something b ON (a.id = b.id)
        |WHERE (a.id = ?)""".stripMargin.straighten())(
      select(thing1.id, thing1.name, thing2.id, thing2.name).from(thing1)
          .innerJoin(thing2).on(thing1.id === thing2.id)
          .where(thing1.id === 11)._1.straighten())
    assertResult(
      """SELECT a.id, a.name, a.rank, a.serial_number
        |FROM something a
        |INNER JOIN something b ON (a.id = b.id)
        |INNER JOIN something c ON (c.id = b.id)
        |WHERE (a.id = ?)""".stripMargin.straighten())(
      select(thing1.id, thing1.name, thing1.rank, thing1.serialNumber).from(thing1)
          .innerJoin(thing2).on(thing1.id === thing2.id)
          .innerJoin(thing3).on(thing3.id === thing2.id)
          .where(thing1.id === 11)._1.straighten())
    assertResult(
      """SELECT a.id, a.name, a.rank, a.serial_number
        |FROM something a
        |INNER JOIN something b ON (a.id = b.id)
        |INNER JOIN something c ON (c.id = b.id)
        |WHERE ((a.id = ?) AND (b.id = ?))""".stripMargin.straighten())(
      select(thing1.id, thing1.name, thing1.rank, thing1.serialNumber).from(thing1)
          .innerJoin(thing2).on(thing1.id === thing2.id)
          .innerJoin(thing3).on(thing3.id === thing2.id)
          .where((thing1.id === 11) && (thing2.id === 99))._1.straighten())
    assertResult("""UPDATE something SET rank = (rank + ?) WHERE (id = ?)""".straighten())(
      update(thing1)(thing1.rank -> (thing1.rank.! + 66)).where(thing1.id === 99)._1.straighten())
    assertResult("""UPDATE something SET rank = (rank - ?) WHERE (id = ?)""".straighten())(
      update(thing1)(thing1.rank -> (thing1.rank.! - 66)).where(thing1.id === 99)._1.straighten())
    assertResult("""UPDATE something SET rank = (rank * ?) WHERE (id = ?)""".straighten())(
      update(thing1)(thing1.rank -> (thing1.rank.! * 66)).where(thing1.id === 99)._1.straighten())
    assertResult("""UPDATE something SET rank = (rank / ?) WHERE (id = ?)""".straighten())(
      update(thing1)(thing1.rank -> (thing1.rank.! / 66)).where(thing1.id === 99)._1.straighten())
    assertResult(
      """SELECT a.id
        |FROM something a
        |WHERE (a.id < to_timestamp(b.id))""".stripMargin.straighten())(
      select(thing1.id).from(thing1).where(thing1.id < ("to_timestamp" !! thing2.id))._1.straighten())
    assertResult(
      """SELECT a.id
        |FROM something a
        |WHERE (a.id < to_timestamp(?))""".stripMargin.straighten())(
      select(thing1.id).from(thing1).where(thing1.id < ("to_timestamp" !! 42))._1.straighten())
    // It's important that the values in the where clause are a list of Any to test the type lifting.
    assertResult("SELECT a.id, b.id FROM something a INNER JOIN something b ON ((b.id = a.id) AND (b.name = b.name)) WHERE (((a.id = ?) AND (a.name = ?)) AND (a.rank = ?))")(
      select(thing1.id, thing2.id).from(thing1)
          .innerJoin(thing2).on((thing2.id === thing1.id) && (thing2.name === thing2.name))
          .where((List[Field](thing1.id, thing1.name, thing1.rank) zip List[Any](1, 2, 3)).foldLeft(None: Option[Bool])((w, c) =>
            Some(w.map(_ && (c._1 === c._2)).getOrElse(c._1 === c._2))).get)
          .limit(1)._1.straighten())
  }
}
