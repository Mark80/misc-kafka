package misc

import org.scalatest.{Matchers, WordSpec}

class InverseSpec extends WordSpec with Matchers {

  def reverse(s: String): String =
    s.foldLeft("") { (rev, char) =>
      char.toString + rev
    }

  def isPolindromo(str: String): Boolean =
    (0 until str.length / 2).forall(index => str.charAt(index) == str.charAt(str.length - 1 - index))

  "Inverse" should {
    "invert" in {

      val s = "abbcb"

      reverse(s) shouldBe "bcbba"

    }

    "is polindrome" in {

      isPolindromo("a") shouldBe true
      isPolindromo("aba") shouldBe true
      isPolindromo("acbaabca") shouldBe true
      isPolindromo("marco") shouldBe false

    }

    "pattern" in {

      pattern(5)
      println(permutation("abcd"))
      //println("abc".substring(0, 1) + "abc".substring(2))

    }

    def permutation(word: String): List[String] =
      if (word.length == 1)
        List(word)
      else
        word.zipWithIndex.toList.flatMap {
          case (char, i) =>
            permutation(word.substring(0, i) + word.substring(i + 1))
              .map(char + _)

        }

    def pattern(n: Int): Unit =
      (0 until n).foreach { n =>
        println("*" * ((2 * n) + 1) + "\n")
      }

  }

}
