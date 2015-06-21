package io.mandelbrot.core.check

import org.scalatest.WordSpec
import org.scalatest.ShouldMatchers

import io.mandelbrot.core.model._

class CheckRefSpec extends WordSpec with ShouldMatchers {

  "A CheckRef" should {

    "be constructed from a string" in {
      val ref = CheckRef("foo.local:check.path")
      ref.agentId.segments shouldEqual Vector("foo", "local")
      ref.checkId.segments shouldEqual Vector("check", "path")
    }

    "serialize to a string" in {
      val ref = CheckRef("foo.local:check.path")
      ref.toString shouldEqual "foo.local:check.path"
    }

    "compare equal if agentId and checkId are equal" in {
      val ref1 = CheckRef("foo.local:check.path")
      val ref2 = CheckRef("foo.local:check.path")
      ref1 shouldEqual ref2
    }

    "compare less than if checkId is a parent" in {
      val ref1 = CheckRef("foo.local:check")
      val ref2 = CheckRef("foo.local:check.path")
      ref1 < ref2 shouldBe true
    }

    "compare greater than if checkId is a child" in {
      val ref1 = CheckRef("foo.local:check")
      val ref2 = CheckRef("foo.local:check.path")
      ref2 > ref1 shouldBe true
    }

    "obey parent-child relationships" in {
      val parent = CheckRef("foo.local:parent")
      val child = CheckRef("foo.local:parent.child")
      val grandchild = CheckRef("foo.local:parent.child.grandchild")
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
//    val ref = CheckRef("fqdn:foo.local/check/path")
//    ref match {
//      case CheckRef(uri, path) =>
//        uri shouldEqual new URI("fqdn:foo.local")
//        path shouldEqual Vector("check", "path")
//      case other =>
//        fail("CheckRef did not extract to a URI and a path Vector")
//    }
//  }
}
