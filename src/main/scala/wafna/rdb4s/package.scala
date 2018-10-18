package wafna

package object rdb4s {
  /**
    * Guards a resource.
    */
  def bracket[R, T](resource: R)(dispose: R => Unit)(consume: R => T): T =
    try consume(resource)
    finally dispose(resource)
}
