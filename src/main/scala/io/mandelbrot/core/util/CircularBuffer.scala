package io.mandelbrot.core.util

/**
 * A circular (or ring) buffer implementation.
 */
class CircularBuffer[T](val size: Int) {

  if (size <= 0) throw new IllegalArgumentException()

  private var curr: Int = 0
  private var last: Int = size - 1
  private var array = Array.fill[Option[T]](size)(None)

  /**
   * add value to the end of the buffer.
   */
  def append(value: T): Unit = {
    array(curr) = Some(value)
    curr = if (curr + 1 == array.size) 0 else curr + 1
  }

  private def lookup(index: Int): Int = {
    if (index < 0 || index >= array.size) throw new IndexOutOfBoundsException()
    if (curr - index - 1 < 0)
      array.size + (curr - index - 1)
    else
      curr - index - 1
  }

  /**
   * get the value at the specified index.
   *
   * @throws NoSuchElementException if there is no element at the specified index
   * @throws IndexOutOfBoundsException if index is larger than the buffer
   */
  def apply(index: Int): T = array(lookup(index)).get

  /**
   * return an Option for the value at the specified index.
   *
   * @throws IndexOutOfBoundsException if index is larger than the buffer
   */
  def get(index: Int): Option[T] = array(lookup(index))

  /**
   * get the last inserted element (index == 0)
   */
  def head: T = apply(0)

  /**
   * return an Option for the last inserted element
   */
  def headOption: Option[T] = get(0)

  /**
   * fold over each element in the buffer.
   */
  def foldLeft[A](z: A)(op: (T, A) => A): A = {
    var out = z
    for (i <- 0.until(array.size)) {
      get(i) match {
        case None => return out
        case Some(v) => out = op(v, out)
      }
    }
    out
  }

  /**
   * grow or shrink the buffer.  when growing, all elements are preserved in order.
   * when shrinking, elements may be trimmed at the end of the buffer if needed.
   */
  def resize(newSize: Int): Unit = {
    if (newSize <= 0) throw new IllegalArgumentException()
    val resized = Array.fill[Option[T]](newSize)(None)
    var next = curr
    var index = 0
    for (_ <- 0.until(array.size)) {
      resized(index) = array(next)
      index = if (index + 1 == resized.size) 0 else index + 1
      next = if (next + 1 == array.size) 0 else next + 1
    }
    curr = index
    array = resized
  }
}
