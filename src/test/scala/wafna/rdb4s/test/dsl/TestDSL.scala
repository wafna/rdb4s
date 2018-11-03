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
  }
}
