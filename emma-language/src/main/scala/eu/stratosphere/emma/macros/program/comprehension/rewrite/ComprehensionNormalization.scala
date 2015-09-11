package eu.stratosphere.emma.macros.program.comprehension.rewrite

import eu.stratosphere.emma.macros.program.util.ProgramUtils

trait ComprehensionNormalization extends ComprehensionRewriteEngine with ProgramUtils {
  import universe._

  def normalize(root: ExpressionRoot) = {
    applyExhaustively(UnnestHead, UnnestGenerator, SimplifyTupleProjection)(root)
    applyExhaustively(FoldFusion)(root)
    root
  }

  /**
   * Unnests a comprehended head in its parent.
   *
   * ==Rule Description==
   *
   * '''Matching Pattern''':
   * {{{ [[ e | qs, x ← [[ e' | qs' ]], qs'' ]] }}}
   *
   * '''Rewrite''':
   * {{{ [[ e[e'\x] | qs,  qs', qs''[e'\x] ]] }}}
   */
  object UnnestGenerator extends Rule {

    case class RuleMatch(parent: Comprehension, generator: Generator, child: Comprehension)

    override protected def bind(r: Expression) = r match {
      case parent@Comprehension(_, qualifiers) =>
        qualifiers.find({
          case Generator(_, Comprehension(ScalaExpr(_, _), _)) => true
          case _ => false
        }) match {
          case Some(generator@Generator(_, child@Comprehension(ScalaExpr(_, _), _))) => Some(RuleMatch(parent, generator, child))
          case _ => Option.empty[RuleMatch]
        }
      case _ =>
        Option.empty[RuleMatch]
    }

    override protected def guard(m: RuleMatch) = true

    override protected def fire(m: RuleMatch) = {
      val name = m.generator.lhs.name
      val term = m.child.hd.asInstanceOf[ScalaExpr]
      val rest = m.parent
        .span { _ != m.generator }._2.tail // trim prefix
        .span { x => !x.isInstanceOf[Generator] ||
          x.as[Generator].lhs.fullName != m.generator.toString
        }._1 // trim suffix

      val (xs, ys) = m.parent.qualifiers span { _ != m.generator }
      m.parent.qualifiers = xs ::: m.child.qualifiers ::: ys.tail

      for (e <- rest if e.isInstanceOf[ScalaExpr])
        e.as[ScalaExpr].substitute(name, term)

      // new parent
      m.parent
    }
  }

  /**
   * Unnests a comprehended head in its parent.
   *
   * ==Rule Description==
   *
   * '''Matching Pattern''':
   * {{{ join [[ [[ e | qs' ]] | qs ]] }}}
   *
   * '''Rewrite''':
   * {{{ [[ e | qs, qs' ]] }}}
   */
  object UnnestHead extends Rule {

    case class RuleMatch(parent: Comprehension, child: Comprehension)

    override protected def bind(r: Expression) = r match {
      case MonadJoin(parent@Comprehension(child@Comprehension(_, _), _)) =>
        Some(RuleMatch(parent, child))
      case _ =>
        Option.empty[RuleMatch]
    }

    override protected def guard(m: RuleMatch) = true //FIXME

    override protected def fire(m: RuleMatch) = {
      m.parent.qualifiers = m.parent.qualifiers ++ m.child.qualifiers
      m.parent.hd = m.child.hd

      // return new root
      m.parent
    }
  }

  object SimplifyTupleProjection extends Rule {

    val projectionPattern = "_(\\d{1,2})".r

    case class RuleMatch(expr: ScalaExpr)

    override protected def bind(r: Expression) = r match {
      case expr@ScalaExpr(_, _) =>
        Some(RuleMatch(expr))
      case _ =>
        Option.empty[RuleMatch]
    }

    override protected def guard(m: RuleMatch) = containsPattern(m.expr.tree)

    override protected def fire(m: RuleMatch) = {
      m.expr.tree = simplify(m.expr.tree)

      // return new root
      m.expr
    }

    object containsPattern extends Traverser with (Tree => Boolean) {

      var result = false

      override def traverse(tree: Tree): Unit = tree match {
        case Select(Apply(TypeApply(Select(tuple@Select(Ident(TermName("scala")), _), TermName("apply")), types), args), TermName(_)) =>
          val isTupleConstructor = tuple.name.toString.startsWith("Tuple") && tuple.symbol.isModule && tuple.symbol.companion.asClass.baseClasses.contains(symbolOf[Product])
          val isProjectionMethod = tree.symbol.isMethod && projectionPattern.findFirstIn(tree.symbol.name.toString).isDefined
          if (isTupleConstructor && isProjectionMethod) {
            result = true
          } else {
            super.traverse(tree)
          }
        case _ =>
          super.traverse(tree)
      }

      override def apply(tree: Tree): Boolean = {
        containsPattern.result = false
        containsPattern.traverse(tree)
        containsPattern.result
      }
    }

    object simplify extends Transformer with (Tree => Tree) {

      var result = false

      override def transform(tree: Tree): Tree = tree match {
        case Select(Apply(TypeApply(Select(tuple@Select(Ident(TermName("scala")), _), TermName("apply")), types), args), TermName(_)) =>
          val isTupleConstructor = tuple.name.toString.startsWith("Tuple") && tuple.symbol.isModule && tuple.symbol.companion.asClass.baseClasses.contains(symbolOf[Product])
          val projectionMatch = projectionPattern.findFirstMatchIn(tree.symbol.name.toString)
          if (isTupleConstructor && tree.symbol.isMethod && projectionMatch.isDefined) {
            val offset = projectionMatch.get.group(1).toInt
            if (args.size >= offset && types.size >= offset)
              q"${args(offset - 1)}"
            else
              super.transform(tree)
          } else {
            super.transform(tree)
          }
        case _ =>
          super.transform(tree)
      }

      override def apply(tree: Tree): Tree = transform(tree)
    }

  }

  /**
   * Fuses a fold with a child comprehension consisting of a single generator.
   *
   * ==Rule Description==
   *
   * '''Matching Pattern''':
   * {{{ fold( empty, sng, union, [[ e | x ← e' ]] ) }}}
   *
   * '''Rewrite''':
   * {{{ fold( empty, sng[e[x\z]\x], union[e[x\z]\x, e[y\z]\y], e' ]] ) }}}
   */
  object FoldFusion extends Rule {

    case class RuleMatch(fold: combinator.Fold, map: Comprehension, child: Generator)

    override protected def bind(r: Expression) = r match {
      case fold@combinator.Fold(_, _, _, map@Comprehension(ScalaExpr(_, _), List(child@Generator(_, _))), _) =>
        Some(RuleMatch(fold, map, child))
      case _ =>
        Option.empty[RuleMatch]
    }

    override protected def guard(m: RuleMatch) = true //FIXME

    override protected def fire(m: RuleMatch) = {
      val x      = freshName("x$")
      val head   = m.map.hd.as[ScalaExpr].tree.rename(m.child.lhs.name, x)
      val oldSng = m.fold.sng.as[Function]
      val newSng = q"""($x: ${m.child.tpe}) => {
        ${oldSng.body.substitute(oldSng.vparams.head.name, head)}
      }""".typeChecked

      m.fold.sng = newSng
      m.fold.xs  = m.child.rhs
      // return new root
      m.fold
    }
  }

}
