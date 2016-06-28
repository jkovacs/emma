package eu.stratosphere.emma.compiler.lang.core

import eu.stratosphere.emma.api.DataBag
import eu.stratosphere.emma.compiler.{ir, BaseCompilerSpec}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class TODSExampleSpec extends BaseCompilerSpec {

  import compiler._
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

  case class Student(id: Int, name: String)
  case class Course(id: Int, name: String, description: String)
  case class Attends(sid: Int, cid: Int, points: Int)

  val students = DataBag(Seq(Student(1, "John Doe"), Student(2, "Jane Doe"), Student(3, "Max Mustermann")))
  val courses = DataBag(Seq(Course(1, "CS 101", "Long description..."), Course(2, "DB 101", "Long description..."), Course(3, "LinAlg", "Long description...")))
  val attends = DataBag(Seq(Attends(1, 1, 95), Attends(1, 2, 85), Attends(2, 1, 100), Attends(2, 3, 90), Attends(3, 1, 30), Attends(3, 2, 80), Attends(3, 3, 90)))

  "TODS example" - {
    "simple" in {
      val src = typeCheckAndANF(reify {
        val studentsCourses = for {
          s <- students
          c <- courses
          a <- attends
          if a.sid == s.id && a.cid == c.id
        } yield (s.name, c.name, a.points)

        studentsCourses
      })

      println(asSource("src tree")(src))
    }

    "with control flow" - {
      "example" in {
        val src = typeCheckAndANF(reify {
          val studentsCourses = for {
            s <- students
            c <- courses
            a <- attends
            if a.sid == s.id && a.cid == c.id
          } yield (s.id, s.name, c.name, a.points)

          val avgPoints = attends.map(_.points).sum.toFloat / attends.size

          if (avgPoints >= 70) {
            studentsCourses.map(t => (t._2, t._3, t._4))
          } else {
            studentsCourses.map(t => (t._1, t._3, t._4))
          }
        })

        println(asSource("src tree")(src))
      }

      "runnable" in {
        val studentsCourses = for {
          s <- students
          c <- courses
          a <- attends
          if a.sid == s.id && a.cid == c.id
        } yield (s.id, s.name, c.name, a.points)

        val avgPoints = attends.map(_.points).sum.toFloat / attends.size

        val result = if (avgPoints >= 80) {
          studentsCourses.map(t => (t._2, t._3, t._4))
        } else {
          studentsCourses.map(t => (t._1, t._3, t._4))
        }
        println(result.fetch())
      }

      "in LNF" in {
        val fn = typeCheck(reify {
          val students$1 = students
          val courses$1 = courses
          val attends$1 = attends
          ir.comprehension[(String, String, Int), DataBag]({
            val s = ir.generator[Student, DataBag]({
              students$1
            });
            val c = ir.generator[Course, DataBag]({
              courses$1
            });
            val a$2 = ir.generator[Attends, DataBag]({
              attends$1
            });
            ir.guard({
              val sid$1: Int = a$2.sid;
              val id$1: Int = s.id;
              val `==$1` : Boolean = sid$1.==(id$1);
              val cid$1: Int = a$2.cid;
              val id$2: Int = c.id;
              val `==$2` : Boolean = cid$1.==(id$2);
              `==$1`.&&(`==$2`)
            });
            ir.head[(String, String, Int)]({
              val name$1: String = s.name;
              val name$2: String = c.name;
              val points$1: Int = a$2.points;
              Tuple3.apply[String, String, Int](name$1, name$2, points$1)
            })
          })
        })
      }
    }
  }

}
