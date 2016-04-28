package eu.stratosphere.emma.compiler.lang.core

import eu.stratosphere.emma.api._
import eu.stratosphere.emma.compiler.{BaseCompilerSpec, ir}
import eu.stratosphere.emma.testschema.Marketing
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

/**
 * A spec for schema information analysis.
 */
@RunWith(classOf[JUnitRunner])
class SchemaInfoSpec extends BaseCompilerSpec {

  import compiler._
  import Schema.{Field, Info, MemberField, SimpleField}
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
    Core.dce
  } andThen {
    Comprehension.resugar(API.bagSymbol)
  } andThen {
    Comprehension.normalize(API.bagSymbol)
  } andThen {
    Owner.at(Owner.enclosing)
  }

  def symbolOf(name: String)(tree: Tree): Symbol = tree.collect {
    case vd@ValDef(_, TermName(`name`), _, _) => vd.symbol
  }.head

  def simpleField(name: String)(tree: Tree): SimpleField = {
    SimpleField(symbolOf(name)(tree))
  }

  def memberField(symbolName: String, memberName: String)(tree: Tree): MemberField = {
    val symbol = symbolOf(symbolName)(tree)
    MemberField(symbol, Term.member(symbol, TermName(memberName)))
  }

  def memberField(field: Field, memberName: String)(tree: Tree): MemberField = {
    val symbol = field.symbol
    MemberField(symbol, Term.member(symbol, TermName(memberName)))
  }

  def freeField(name: String, tpe: Type): SimpleField = {
    SimpleField(Term.sym.free(Term.name(name), tpe))
  }

  "tuple types playground" - {

    val examples = Seq(
      typeCheck(reify {
        (42, "foobar")
      }),
      typeCheck(reify {
        new Tuple2(42, "foobar")
      }),
      typeCheck(reify {
        Ad(1, "foobar", AdClass.FASHION)
      }),
      typeCheck(reify {
        new Ad(1, "foobar", AdClass.FASHION)
      })
    )

    // extract the types of the objects constructed the examples
    val types = for (e <- examples) yield {
      Type.of(e).typeSymbol.asClass
    }

    // assert that all are case classes
    for (e <- types) assert(e.isCaseClass)

    // extract the symbols of the constructing functions
    val funsyms = for (e <- examples) yield e match {
      case Apply(fun, args) => fun.symbol
    }

    // assert that the funsyms belong to recognized constructors
    for ((f, t) <- funsyms zip types) {
      // the owning class of the function symbol should be the class of the target
      val cmp = f.owner.companionSymbol
      val cls = cmp.companion
      assert(cls == t)

      // the function symbol should be either of a constructor of of a synthetic apply method
      val isCtr = f.owner.companionSymbol.isModule && f.isConstructor
      val isApp = f.owner.isModuleClass && f.name == TermName("apply") && f.isSynthetic

      assert(isCtr || isApp)
    }

    // extract the projections associated with the function applications
    for (f <- funsyms) yield {
      // the owning class of the function symbol should be the class of the target
      val cmp = f.owner.companionSymbol
      val cls = cmp.companion

      for (param <- f.paramLists.head) yield {
        val proj = cls.info.decl(param.name)
        assert(proj.isAccessor)
        (param, cls.info.decl(param.name))
      }
    }
  }

  "local schema" - {

    "without control flow" in {
      // ANF representation with `desugared` comprehensions
      val fn = typeCheck(reify {
        (c: Click, d: Double) => {
          val t = c.time
          val p = t.plusSeconds(600L)
          val m = t.minusSeconds(600L)
          val a = (c, p, m)
          a
        }
      }).asInstanceOf[Function]

      // construct expected local schema

      // 1) get schema fields
      val fld$c /*       */ = SimpleField(symbolOf("c")(fn))
//      val fld$c$adID /*  */ = MemberField(fld$c.symbol, Term.member(fld$c.symbol, TermName("adID")))
//      val fld$c$userID /**/ = MemberField(fld$c.symbol, Term.member(fld$c.symbol, TermName("userID")))
      val fld$c$time /*  */ = MemberField(fld$c.symbol, Term.member(fld$c.symbol, TermName("time")))

      val fld$t /*       */ = SimpleField(symbolOf("t")(fn))
      val fld$p /*       */ = SimpleField(symbolOf("p")(fn))
      val fld$m /*       */ = SimpleField(symbolOf("m")(fn))

      val fld$a /*       */ = SimpleField(symbolOf("a")(fn))
      val fld$a$_1 /*    */ = MemberField(fld$a.symbol, Term.member(fld$a.symbol, TermName("_1")))
      val fld$a$_2 /*    */ = MemberField(fld$a.symbol, Term.member(fld$a.symbol, TermName("_2")))
      val fld$a$_3 /*    */ = MemberField(fld$a.symbol, Term.member(fld$a.symbol, TermName("_3")))

      // 2) construct equivalence classes of fields
      val cls$01 /*      */ = Set[Field](fld$c, fld$a$_1)
//      val cls$02 /*      */ = Set[Field](fld$c$adID)
//      val cls$03 /*      */ = Set[Field](fld$c$userID)
      val cls$04 /*      */ = Set[Field](fld$c$time, fld$t)
      val cls$05 /*      */ = Set[Field](fld$p, fld$a$_2)
      val cls$06 /*      */ = Set[Field](fld$m, fld$a$_3)
      val cls$07 /*      */ = Set[Field](fld$a)

      // 3) construct the expected local schema information
      val exp = Info(Set(cls$01, /*cls$02, cls$03,*/ cls$04, cls$05, cls$06, cls$07))

      // compute actual local schema
      val act = Schema.local(fn)

      act.fieldClasses should contain allOf (cls$01, cls$04, cls$05, cls$06)
    }

    "with nested case classes" in {
      val fn = typeCheck(reify {
        (t: (Long, User)) => {
          val k = t._1
          val l = t._2
          val m = l.name
          val n = m.first
          val a = (k, n)
          a
        }
      }).asInstanceOf[Function]

//      val cls$01 = Set[Field](simpleField("t")(fn))
      val cls$02 = Set[Field](simpleField("l")(fn), memberField("t", "_2")(fn))
      val cls$03 = Set[Field](simpleField("m")(fn), memberField("l", "name")(fn))
      val cls$04 = Set[Field](
        simpleField("n")(fn),
        memberField("m", "first")(fn),
        memberField("a", "_2")(fn))

      val schema = Schema.local(fn)
      println(schema)

      schema.fieldClasses should contain allOf (cls$02, cls$03, cls$04)
    }


    "with control flow" in {
      // TODO
    }
  }

  "comprehension schema" - {
    "of simple join" in {
      val program2 = typeCheckAndANF(reify {
        val ucs = for {
          u <- Marketing.users
          c <- Marketing.clicks
          if u.id == c.userID
        } yield (u, c)
        write("result.csv", new CSVOutputFormat[(User, Click)])(ucs)
      }).asInstanceOf[Block]
      val program = typeCheck(reify {
        val users$1: DataBag[User] = Marketing.users
        val clicks$1: DataBag[Click] = Marketing.clicks
        val ucs: DataBag[(User, Click)] = ir.comprehension[(User, Click), DataBag]({
          val u: User = ir.generator[User, DataBag]({
            users$1
          })
          val c$2: Click = ir.generator[Click, DataBag]({
            clicks$1
          })
          ir.guard({
            val id$1: Long = u.id
            val userID$1: Long = c$2.userID
            id$1.==(userID$1)
          })
          ir.head[(User, Click)]({
            Tuple2.apply[User, Click](u, c$2)
          })
        })
        ucs
      }).asInstanceOf[Block]

      val comp = program2.collect { case Tree.val_(Term.sym(TermName("ucs"), _), rhs, _) => rhs }.head
      val meta = new Core.Meta(program2)

      val schema = Schema.comprehensionSchema(comp, meta)
      val equivSchema = schema.schema

      println(schema)

      schema.consumes should have size 2

      val u = simpleField("u")(program2)
      val c = simpleField("c$2")(program2)
      val users = simpleField("users$1")(program2)
      val clicks = simpleField("clicks$1")(program2)
      for (g <- schema.consumes) {
        g should matchPattern {
          case Schema.Generate(`u`, f) if equivSchema.equivalences(f).contains(users) =>
          case Schema.Generate(`c`, f) if equivSchema.equivalences(f).contains(clicks) =>
        }
      }

      val resultField = schema.yields.result
      equivSchema.equivalences(memberField(resultField, "_1")(program2)) should contain (u)
      equivSchema.equivalences(memberField(resultField, "_2")(program2)) should contain (c)

    }
  }

  "function analysis" in {

    val inp = typeCheckAndANF(reify {

      val f = (c: Click) => {
      }
      f(clicks.fetch().head)
    })

    println(asSource("exp tree")(inp))
  }

}
