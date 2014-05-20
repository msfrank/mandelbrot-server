package io.mandelbrot.core.notification

import org.scalatest.WordSpec
import org.scalatest.matchers.MustMatchers
import java.io.StringReader

class NotificationRulesSpec extends WordSpec with MustMatchers {

  "NotificationRules parser" must {

    "process rules" in {
      val rules =
        """always : notifyAll
          |
          |when probe(*/load) : notify(@admins)
          |
          |when probe(*/cpu) : notify(michael.frank)
          |
          |
          |foo :
          |  bar
          |  baz
        """.stripMargin
      val reader = new StringReader(rules)
      NotificationRules.parse(reader)
    }
  }

}
