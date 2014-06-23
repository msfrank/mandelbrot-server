package io.mandelbrot.core.registry

import java.net.URI

import org.scalatest.WordSpec
import org.scalatest.matchers.MustMatchers

class ProbeRefSpec extends WordSpec with MustMatchers {

  "A ProbeRef" must {

    "be constructed from a string" in {
      val ref = ProbeRef("fqdn:foo.local/probe/path")
      ref.uri must be === new URI("fqdn:foo.local")
      ref.path must be === Vector("probe", "path")
    }

    "obey parent-child relationships" in {
      val parent = ProbeRef("fqdn:foo.local/parent")
      val child = ProbeRef("fqdn:foo.local/parent/child")
      val grandchild = ProbeRef("fqdn:foo.local/parent/child/grandchild")
      // true relationships
      parent.isParentOf(child) must be(true)
      parent.isDirectParentOf(child) must be(true)
      child.isChildOf(parent) must be(true)
      child.isDirectChildOf(parent) must be(true)
      child.isParentOf(grandchild) must be(true)
      child.isDirectParentOf(grandchild) must be(true)
      grandchild.isChildOf(child) must be(true)
      grandchild.isDirectChildOf(child) must be(true)
      // false relationships
      parent.isDirectParentOf(grandchild) must be(false)
      grandchild.isDirectChildOf(parent) must be(false)
    }
  }

  "extract to a URI and a path Vector" in {
    val ref = ProbeRef("fqdn:foo.local/probe/path")
    ref match {
      case ProbeRef(uri, path) =>
        uri must be === new URI("fqdn:foo.local")
        path must be === Vector("probe", "path")
      case other =>
        fail("ProbeRef did not extract to a URI and a path Vector")
    }
  }
}
