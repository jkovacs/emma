package eu.stratosphere.emma.compiler.lang.core

import java.time.Instant

import eu.stratosphere.emma.api._
import eu.stratosphere.emma.compiler.BaseCompilerSpec
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

/**
 * A spec for holistic optimizations.
 */
@RunWith(classOf[JUnitRunner])
class HolisticOptimizationsSpec extends BaseCompilerSpec {

  import compiler._
  import eu.stratosphere.emma.testschema.Marketing._
  import universe._

  def typeCheckAndANF[T]: Expr[T] => Tree = {
    (_: Expr[T]).tree
  } andThen {
    Type.check(_)
  } andThen {
    Core.destructPatternMatches
  } andThen {
    Core.resolveNameClashes
  } andThen {
    Core.anf
  } andThen {
    Core.simplify
  } andThen {
    Comprehension.resugar(API.bagSymbol)
  } andThen {
    Comprehension.normalize(API.bagSymbol)
  } andThen {
    Owner.at(Owner.enclosing)
  }

  "example 1" in {
    val act = typeCheckAndANF(reify {

      val ucs = for {
        u <- users
        c <- clicks
        if u.id == c.userID
      }  yield (u, c)

      val res = for {
        (u, c) <- ucs
      } yield (u.id, c.time.minusSeconds(600))

      write("result.csv", new CSVOutputFormat[(Long, Instant)])(res)
    })

//    println(asSource("act tree")(act))
    Schema.global(act)
    assert(false)

    val exp = typeCheckAndANF(reify {

      val ucs = for {
        uid <- users map { case u => u.id }
        (cuid, ctime) <- clicks map { case c => (c.userID, c.time) }
        if uid == cuid
      } yield (uid, ctime)

      val res = for {
        (uid, ctime) <- ucs
      } yield (uid, ctime.minusSeconds(600))

      write("result.csv", new CSVOutputFormat[(Long, Instant)])(res)
    })

    println(asSource("exp tree")(exp))
  }

  "example 2" ignore {
    val act = typeCheckAndANF(reify {

      val ucs = for {
        u <- users
        c <- clicks
        if u.id == c.userID
      }  yield (u, c)

      val res = for {
        (u1, c1) <- ucs
        (u2, c2) <- ucs
        if u1.id == u2.id
      } yield (u1.id, c1.time isAfter c2.time)

      write("result.csv", new CSVOutputFormat[(Long, Boolean)])(res)
    })

    println(asSource("act tree")(act))

    val exp = typeCheckAndANF(reify {

      val ucs = for {
        uid <- users map { case u => u.id }
        (cuid, ctime) <- clicks map { case c => (c.userID, c.time) }
        if uid == cuid
      } yield (cuid, ctime)

      val res = for {
        (cuid1, ctime1) <- ucs
        (cuid2, ctime2) <- ucs
        if cuid1 == cuid2
      } yield (cuid1, ctime1 isAfter ctime2)

      write("result.csv", new CSVOutputFormat[(Long, Boolean)])(res)
    })

    /* For the next iteration
    val exp = typeCheckAndANF(reify {

      val ucs = for {
        uid <- users map { case u => u.id }
        (cuid, ctime) <- clicks map { case c => (c.userID, c.time) }
        if uid == cuid
      } yield (cuid, ctime)

      val res = for {
        (cuid1, ctime1) <- ucs
        (cuid2, ctime2) <- ucs
        if cuid1 == cuid2
      } yield (cuid1, ctime1 isAfter ctime2)

      write("result.csv", new CSVOutputFormat[(Long, Boolean)])(res)
    })
    */

    println(asSource("exp tree")(exp))
  }

  "example 3" ignore {
    val fn = typeCheckAndANF(reify {
      val cbu = for {
        u <- users
        Group(k, v) <- clicks.groupBy(_.userID)
        if u.id == k
      } yield (u.name, v.fold(0L)(_ => 1L, _ + _))

      val sum = cbu.map { case (uName, cSize) => cSize }.fold(0L)(_ => 1L, _ + _)
      val cnt = cbu.size
      val avg = sum / cnt

      avg
    })

    println(asSource("exp tree")(fn))
  }

  "example 4" ignore {
    val addition = true

    val act = typeCheckAndANF(reify {

      def block$01(addition: Boolean) = {

        val ucs = for {
          u <- users
          c <- clicks
          if u.id == c.userID
        }  yield (u, c)

        def block$02() = {
          val res = for ((u, c) <- ucs) yield (u.id, c.time.minusSeconds(600))
          block$04(res)
        }

        def block$03() = {
          val res = for ((u, c) <- ucs) yield (c.userID, c.time.plusSeconds(600))
          block$04(res)
        }

        def block$04(res: DataBag[(Long, Instant)]) = {
          res
        }

        if (addition) block$02()
        else block$03()
      }

      block$01(addition)
    })

    println(asSource("act tree")(act))

    val exp = typeCheckAndANF(reify {

      def block$01(addition: Boolean) = {

        val ucs = for {
          uid <- users map { case u => u.id }
          (cuid, ctime) <- clicks map { case c => (c.userID, c.time) }
          if uid == cuid
        } yield (uid, ctime)

        def block$02() = {
          val res = for ((uid, ctime) <- ucs) yield (uid, ctime.minusSeconds(600))
          block$04(res)
        }

        def block$03() = {
          val res = for ((uid, ctime) <- ucs) yield (uid, ctime.plusSeconds(600))
          block$04(res)
        }

        def block$04(res: DataBag[(Long, Instant)]) = {
          res
        }

        if (addition) block$02()
        else block$03()
      }

      block$01(addition)
    })

    println(asSource("exp tree")(exp))
  }

}