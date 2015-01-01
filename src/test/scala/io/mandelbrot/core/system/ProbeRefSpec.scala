package io.mandelbrot.core.system

import java.net.URI

import org.scalatest.WordSpec
import org.scalatest.matchers.ShouldMatchers

class ProbeRefSpec extends WordSpec with ShouldMatchers {

  "A ProbeRef" should {

    "be constructed from a string" in {
      val ref = ProbeRef("fqdn:foo.local/probe/path")
      ref.uri shouldEqual new URI("fqdn:foo.local")
      ref.path shouldEqual Vector("probe", "path")
    }

    "obey parent-child relationships" in {
      val parent = ProbeRef("fqdn:foo.local/parent")
      val child = ProbeRef("fqdn:foo.local/parent/child")
      val grandchild = ProbeRef("fqdn:foo.local/parent/child/grandchild")
      // true relationships
      parent.isParentOf(child) should be(true)
      parent.isDirectParentOf(child) should be(true)
      child.isChildOf(parent) should be(true)
      child.isDirectChildOf(parent) should be(true)
      child.isParentOf(grandchild) should be(true)
      child.isDirectParentOf(grandchild) should be(true)
      grandchild.isChildOf(child) should be(true)
      grandchild.isDirectChildOf(child) should be(true)
      // false relationships
      parent.isDirectParentOf(grandchild) should be(false)
      grandchild.isDirectChildOf(parent) should be(false)
    }
  }

  "extract to a URI and a path Vector" in {
    val ref = ProbeRef("fqdn:foo.local/probe/path")
    ref match {
      case ProbeRef(uri, path) =>
        uri shouldEqual new URI("fqdn:foo.local")
        path shouldEqual Vector("probe", "path")
      case other =>
        fail("ProbeRef did not extract to a URI and a path Vector")
    }
  }
}
