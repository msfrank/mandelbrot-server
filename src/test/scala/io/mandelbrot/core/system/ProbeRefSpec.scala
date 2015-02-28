package io.mandelbrot.core.system

import java.net.URI

import org.scalatest.WordSpec
import org.scalatest.ShouldMatchers

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
      parent.isParentOf(child) shouldEqual true
      parent.isDirectParentOf(child) shouldEqual true
      child.isChildOf(parent) shouldEqual true
      child.isDirectChildOf(parent) shouldEqual true
      child.isParentOf(grandchild) shouldEqual true
      child.isDirectParentOf(grandchild) shouldEqual true
      grandchild.isChildOf(child) shouldEqual true
      grandchild.isDirectChildOf(child) shouldEqual true
      // false relationships
      parent.isDirectParentOf(grandchild) shouldEqual false
      grandchild.isDirectChildOf(parent) shouldEqual false
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
