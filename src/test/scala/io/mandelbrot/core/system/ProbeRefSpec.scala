package io.mandelbrot.core.system

import org.scalatest.WordSpec
import org.scalatest.ShouldMatchers

import io.mandelbrot.core.model._

class ProbeRefSpec extends WordSpec with ShouldMatchers {

  "A ProbeRef" should {

    "be constructed from a string" in {
      val ref = ProbeRef("foo.local:probe.path")
      ref.agentId.segments shouldEqual Vector("foo", "local")
      ref.checkId.segments shouldEqual Vector("probe", "path")
    }

    "serialize to a string" in {
      val ref = ProbeRef("foo.local:probe.path")
      ref.toString shouldEqual "foo.local:probe.path"
    }

    "compare equal if agentId and checkId are equal" in {
      val ref1 = ProbeRef("foo.local:probe.path")
      val ref2 = ProbeRef("foo.local:probe.path")
      ref1 shouldEqual ref2
    }

    "compare less than if checkId is a parent" in {
      val ref1 = ProbeRef("foo.local:probe")
      val ref2 = ProbeRef("foo.local:probe.path")
      ref1 < ref2 shouldBe true
    }

    "compare greater than if checkId is a child" in {
      val ref1 = ProbeRef("foo.local:probe")
      val ref2 = ProbeRef("foo.local:probe.path")
      ref2 > ref1 shouldBe true
    }

    "obey parent-child relationships" in {
      val parent = ProbeRef("foo.local:parent")
      val child = ProbeRef("foo.local:parent.child")
      val grandchild = ProbeRef("foo.local:parent.child.grandchild")
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

//  "extract to a URI and a path Vector" in {
//    val ref = ProbeRef("fqdn:foo.local/probe/path")
//    ref match {
//      case ProbeRef(uri, path) =>
//        uri shouldEqual new URI("fqdn:foo.local")
//        path shouldEqual Vector("probe", "path")
//      case other =>
//        fail("ProbeRef did not extract to a URI and a path Vector")
//    }
//  }
}
