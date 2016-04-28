package eu.stratosphere.emma.compiler.lang

import eu.stratosphere.emma.compiler.Common
import eu.stratosphere.emma.compiler.lang.core.Core

import scala.collection.mutable
import scalax.collection.GraphEdge.UnDiEdge
import scalax.collection.immutable.Graph


trait SchemaOptimizations extends Common {
  self: Core =>

  import Core.Language._
  import Tree.block
  import universe._

  // ---------------------------------------------------------------------------
  // Schema Analysis & Optimizations
  // ---------------------------------------------------------------------------

  object Schema {

    import CaseClassMeta._

    /** Root trait for all fields. */
    sealed trait Field {
      def symbol: Symbol
    }

    /** A simple field representing a ValDef name. */
    case class SimpleField(symbol: Symbol) extends Field

    /** A field representing a member selection. */
    case class MemberField(symbol: Symbol, member: Symbol) extends Field

    /** A class of equivalent fields. */
    type FieldClass = Set[Field]

    /** Fields can be either */
    case class Info(equivalences: Iterable[(Field, Field)]) {
      private val edges = equivalences.map { case (f, t) => UnDiEdge(f, t) }
      private val nodes = edges.flatMap(_.toSet)
      private val graph: Graph[Field, UnDiEdge] = Graph.from[Field, UnDiEdge](nodes, edges)

      val fieldClasses = graph.componentTraverser().map(_.nodes.map(_.value)).toSet

      def equivalences(field: Field): FieldClass = {
        graph.get(field).neighbors.map(_.value)
      }

      def union(info: Info): Info = {
        Info(this.equivalences ++ info.equivalences)
      }

      def withEquivalence(eq: (Field, Field)): Info = {
        Info(this.equivalences ++ Set(eq))
      }

      override def toString: String =
        s"""Schema(
           |	${fieldClasses.map(_.toString()).mkString(",\n\t")}
           |)""".stripMargin
    }

    object Info {
      def apply(fieldClasses: Set[FieldClass]): Info = {
        Info(fieldClasses.flatMap(fc => fc.subsets(2).map(s => s.head -> s.last)))
      }
    }

    case class Generate(bag: Field, target: Field)
    case class Yield(result: Field)

    case class BlockSchema(equivalenceClasses: Set[FieldClass], expr: Field)
    case class ComprehensionSchema(schema: Info, consumes: Set[Generate], yields: Yield)

    private[emma] def comprehensionSchema(tree: Tree, meta: Core.Meta): ComprehensionSchema = {
      val cs = new Comprehension.Syntax(API.bagSymbol)

      var info: Info = Info(Set.empty)
      var headExpr: Field = null
      val consumes = mutable.ArrayBuffer[Generate]()

      traverse(tree) {
        case cs.generator(lhs, block) =>
          val ls = local(block, meta)
          println(s"$lhs -> $block")
          info = info.union(Info(ls.equivalenceClasses))

          consumes += Generate(SimpleField(lhs), SimpleField(ls.expr.symbol))

        case cs.guard(block) =>
          val ls = local(block, meta)
          info = info.union(Info(ls.equivalenceClasses))

        case cs.head(block) =>
          val ls = local(block, meta)
          info = info.union(Info(ls.equivalenceClasses))

          headExpr = ls.expr
      }

      ComprehensionSchema(schema = info, consumes = consumes.toSet, yields = Yield(headExpr))
    }

    /**
     * Compute the (global) schema information for a tree fragment.
     *
     * @param tree The ANF [[Tree]] to be analyzed.
     * @return The schema information for the input tree.
     */
    private[emma] def global(tree: Tree): Unit = {
      val meta = new Core.Meta(tree)
      val cs = new Comprehension.Syntax(API.bagSymbol)

      traverse(tree) {
        case val_(lhs, comprehension@cs.comprehension(qs, cs.head(block)), _) =>
          println("Comprehension:")
          val schema = comprehensionSchema(comprehension, meta)
          println(s"Schema:\n\tConsumes: ${schema.consumes}\n\tYields: ${schema.yields}\n\tEquivalences: ${schema.schema.fieldClasses}")
        case v@val_(_, _, _) =>
          println(v)
      }
    }

    /**
     * Compute the (local) schema information for an anonymous function.
     *
     * @param tree The ANF [[Function]] to be analyzed.
     * @return The schema information for the input tree.
     */
    private[emma] def local(tree: Function): Schema.Info = tree match {
      case Function(_, block@block(_, _)) =>
        val bs = local(block, new Core.Meta(tree))
        Info(bs.equivalenceClasses)
    }

    private[emma] def local(tree: Block, meta: Core.Meta): BlockSchema = {
      type Equivalence = (Field, Field)

      def findEquivalences(lhs: Symbol, term: Tree): Set[Equivalence] = term match {
        case lit(value) =>
          Set.empty

        case this_(sym) =>
          // TODO: relevant?
          Set.empty

        case ref(sym) =>
          findBackwardEquivalences(sym) + (SimpleField(lhs) -> SimpleField(sym))

        case qref(pkg, sym) =>
          findBackwardEquivalences(sym) + (SimpleField(lhs) -> SimpleField(sym))

        case sel(target, member) =>
          val selection = SimpleField(lhs) -> MemberField(Term sym target, member)
          findBackwardEquivalences(Term sym target) + selection

        case app(method, types, args) =>
          args.flatMap(arg => findBackwardEquivalences(arg.symbol)).toSet

        // N.B. order is important, as we want to catch case class constructor applications
        // before other method calls or class instantiations

        case call(target, method, types, args) if isCaseClassConstructor(method) =>
          val meta = CaseClassMeta(Type.of(lhs).typeSymbol)
          args.zip(meta.constructorProjections(method)).collect {
            case (arg, param) if Is valid arg.symbol =>
              findBackwardEquivalences(arg.symbol) + (SimpleField(arg.symbol) -> MemberField(lhs, param))
          }.flatten.toSet

        case call(target, method, types, args) =>
          findBackwardEquivalences(Term sym target) ++
            args.collect {
              case arg if Is valid arg.symbol => findBackwardEquivalences(arg.symbol)
            }.flatten.toSet

        case inst(target, types, args) =>
          args.collect {
            case arg if Is valid arg.symbol => findBackwardEquivalences(arg.symbol)
          }.flatten.toSet

        case lambda(sym, args, body) =>
          // TODO: huh
          Set.empty

        case typed(expr, _) =>
          findEquivalences(lhs, expr)

        // TODO: conditionals (Term')
        // TODO: Defs?

        case _ =>
          val identity = SimpleField(lhs) -> SimpleField(lhs)
          Set(identity)
      }

      def findBackwardEquivalences(sym: Symbol): Set[Equivalence] = {
        meta.valdef(sym) match {
          case Some(vd@val_(lhs, rhs, _)) =>
            val identity = SimpleField(sym) -> SimpleField(sym)
            findEquivalences(lhs, rhs) + identity
          case None =>
            Set.empty
        }
      }

      tree match {
        case block(_, expr) =>
          val returnSymbol = Term.sym.free(Term.name.fresh("return"), Type.of(expr))
          val equivalences = findEquivalences(returnSymbol, expr)
          BlockSchema(equivalenceClasses(equivalences), SimpleField(returnSymbol))
      }
    }

    private def equivalenceClasses(equivalences: Iterable[(Field, Field)]): Set[FieldClass] = {
      val edges = equivalences.map { case (f, t) => UnDiEdge(f, t) }
      val nodes = edges.flatMap(_.toSet)
      val graph = Graph.from[Field, UnDiEdge](nodes, edges)

      // each connected component forms an equivalence class
      graph.componentTraverser().map(_.nodes.map(_.value)).toSet
    }

    class CaseClassMeta(sym: ClassSymbol) {

      // assert that the input argument is a case class symbol
      assert(sym.isCaseClass)

      def constructorProjections(fun: Symbol): Seq[Symbol] = {
        assert(isCaseClassConstructor(fun))
        val parameters = fun.asMethod.paramLists.head
        parameters.map(p => sym.info.decl(p.name))
      }

      def members(): Set[Symbol] = {
        Type.of(sym).members.filter {
          case m: TermSymbol => m.isGetter && m.isCaseAccessor
        }.toSet
      }

      def memberFields(symbol: Symbol): Set[MemberField] = {
        members().map(member => MemberField(symbol, member))
      }

      def memberEquivalences(symbol: Symbol): Set[(Field, Field)] = {
        memberFields(symbol).map(f => f -> f)
      }
    }

    object CaseClassMeta {

      def apply(s: Symbol) = new CaseClassMeta(s.asClass)

      def isCaseClass(s: Symbol): Boolean = s.isClass && s.asClass.isCaseClass

      def isCaseClassConstructor(fun: Symbol): Boolean = {
        assert(fun.isMethod)
        val isCtr = fun.owner.companion.isModule && fun.isConstructor
        val isApp = fun.owner.isModuleClass && fun.name == TermName("apply") && fun.isSynthetic

        isCtr || isApp
      }

      def caseClassMemberEquivalences(s: Symbol): Set[(Field, Field)] = {
        val tpe = Type.of(s).typeSymbol
        if (isCaseClass(tpe)) {
          CaseClassMeta(tpe).memberEquivalences(s)
        } else {
          Set.empty
        }
      }
    }

  }

}
