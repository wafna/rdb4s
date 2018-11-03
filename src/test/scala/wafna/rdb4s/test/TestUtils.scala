package wafna.rdb4s.test

object TestUtils {
  implicit class `string padding`(val a: Any) {
    def padLeft(i: Int) : String = {
      val s = a.toString
      (" " * (0 max (i - s.length))) + s
    }
    def padRight(i: Int) : String = {
      val s = a.toString
      s + (" " * (0 max (i - s.length)))
    }
  }
}
