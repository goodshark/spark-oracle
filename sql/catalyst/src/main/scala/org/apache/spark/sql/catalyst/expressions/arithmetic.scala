/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.catalyst.expressions

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.expressions.codegen._
import org.apache.spark.sql.catalyst.util.{BinToNumUtil, TypeUtils}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.{CalendarInterval, UTF8String}

import scala.tools.scalap.scalax.rules.scalasig.Children

@ExpressionDescription(
  usage = "_FUNC_(expr) - Returns the negated value of `expr`.",
  extended =
    """
    Examples:
      > SELECT _FUNC_(1);
       -1
  """)
case class UnaryMinus(child: Expression) extends UnaryExpression
  with ExpectsInputTypes with NullIntolerant {

  override def inputTypes: Seq[AbstractDataType] = Seq(TypeCollection.NumericAndInterval)

  override def dataType: DataType = child.dataType

  override def toString: String = s"-$child"

  private lazy val numeric = TypeUtils.getNumeric(dataType)

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = dataType match {
    case dt: DecimalType => defineCodeGen(ctx, ev, c => s"$c.unary_$$minus()")
    case dt: NumericType => nullSafeCodeGen(ctx, ev, eval => {
      val originValue = ctx.freshName("origin")
      // codegen would fail to compile if we just write (-($c))
      // for example, we could not write --9223372036854775808L in code
      s"""
        ${ctx.javaType(dt)} $originValue = (${ctx.javaType(dt)})($eval);
        ${ev.value} = (${ctx.javaType(dt)})(-($originValue));
      """
    })
    case dt: CalendarIntervalType => defineCodeGen(ctx, ev, c => s"$c.negate()")
  }

  protected override def nullSafeEval(input: Any): Any = {
    if (dataType.isInstanceOf[CalendarIntervalType]) {
      input.asInstanceOf[CalendarInterval].negate()
    } else {
      numeric.negate(input)
    }
  }

  override def sql: String = s"(- ${child.sql})"
}

@ExpressionDescription(
  usage = "_FUNC_(expr) - Returns the value of `expr`.")
case class UnaryPositive(child: Expression)
  extends UnaryExpression with ExpectsInputTypes with NullIntolerant {
  override def prettyName: String = "positive"

  override def inputTypes: Seq[AbstractDataType] = Seq(TypeCollection.NumericAndInterval)

  override def dataType: DataType = child.dataType

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode =
    defineCodeGen(ctx, ev, c => c)

  protected override def nullSafeEval(input: Any): Any = input

  override def sql: String = s"(+ ${child.sql})"
}

/**
  * A function that get the absolute value of the numeric value.
  */
@ExpressionDescription(
  usage = "_FUNC_(expr) - Returns the absolute value of the numeric value.",
  extended =
    """
    Examples:
      > SELECT _FUNC_(-1);
       1
  """)
case class Abs(child: Expression)
  extends UnaryExpression with ExpectsInputTypes with NullIntolerant {

  override def inputTypes: Seq[AbstractDataType] = Seq(NumericType)

  override def dataType: DataType = child.dataType

  private lazy val numeric = TypeUtils.getNumeric(dataType)

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = dataType match {
    case dt: DecimalType =>
      defineCodeGen(ctx, ev, c => s"$c.abs()")
    case dt: NumericType =>
      defineCodeGen(ctx, ev, c => s"(${ctx.javaType(dt)})(java.lang.Math.abs($c))")
  }

  protected override def nullSafeEval(input: Any): Any = numeric.abs(input)
}

abstract class BinaryArithmetic extends BinaryOperator {

  override def dataType: DataType = left.dataType

  override lazy val resolved = childrenResolved && checkInputDataTypes().isSuccess

  /** Name of the function for this expression on a [[Decimal]] type. */
  def decimalMethod: String =
    sys.error("BinaryArithmetics must override either decimalMethod or genCode")

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = dataType match {
    case dt: DecimalType =>
      defineCodeGen(ctx, ev, (eval1, eval2) => s"$eval1.$decimalMethod($eval2)")
    // byte and short are casted into int when add, minus, times or divide
    case ByteType | ShortType =>
      defineCodeGen(ctx, ev,
        (eval1, eval2) => s"(${ctx.javaType(dataType)})($eval1 $symbol $eval2)")
    case _ =>
      defineCodeGen(ctx, ev, (eval1, eval2) => s"$eval1 $symbol $eval2")
  }
}

object BinaryArithmetic {
  def unapply(e: BinaryArithmetic): Option[(Expression, Expression)] = Some((e.left, e.right))
}

@ExpressionDescription(
  usage = "expr1 _FUNC_ expr2 - Returns `expr1`+`expr2`.",
  extended =
    """
    Examples:
      > SELECT 1 _FUNC_ 2;
       3
  """)
case class Add(left: Expression, right: Expression) extends BinaryArithmetic with NullIntolerant {

   override def inputType: AbstractDataType = TypeCollection.NumericAndInterval
//  override def inputType: AbstractDataType = TypeCollection.NumericAndIntervalAddString

  override def symbol: String = "+"

  private lazy val numeric = TypeUtils.getNumeric(dataType)

  protected override def nullSafeEval(input1: Any, input2: Any): Any = {
    if (dataType.isInstanceOf[CalendarIntervalType]) {
      input1.asInstanceOf[CalendarInterval].add(input2.asInstanceOf[CalendarInterval])
    } else if (dataType.isInstanceOf[StringType]) {
      input1.asInstanceOf[UTF8String].toString + input2.asInstanceOf[UTF8String].toString
    } else {
      numeric.plus(input1, input2)
    }
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = dataType match {
    case dt: DecimalType =>
      defineCodeGen(ctx, ev, (eval1, eval2) => s"$eval1.$$plus($eval2)")
    case ByteType | ShortType =>
      defineCodeGen(ctx, ev,
        (eval1, eval2) => s"(${ctx.javaType(dataType)})($eval1 $symbol $eval2)")
    case CalendarIntervalType =>
      defineCodeGen(ctx, ev, (eval1, eval2) => s"$eval1.add($eval2)")
    case _ =>
      defineCodeGen(ctx, ev, (eval1, eval2) => s"$eval1 $symbol $eval2")
  }
}

@ExpressionDescription(
  usage = "expr1 _FUNC_ expr2 - Returns `expr1`-`expr2`.",
  extended =
    """
    Examples:
      > SELECT 2 _FUNC_ 1;
       1
  """)
case class Subtract(left: Expression, right: Expression)
  extends BinaryArithmetic with NullIntolerant {

  override def inputType: AbstractDataType = TypeCollection.NumericAndInterval

  override def symbol: String = "-"

  private lazy val numeric = TypeUtils.getNumeric(dataType)

  protected override def nullSafeEval(input1: Any, input2: Any): Any = {
    if (dataType.isInstanceOf[CalendarIntervalType]) {
      input1.asInstanceOf[CalendarInterval].subtract(input2.asInstanceOf[CalendarInterval])
    } else {
      numeric.minus(input1, input2)
    }
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = dataType match {
    case dt: DecimalType =>
      defineCodeGen(ctx, ev, (eval1, eval2) => s"$eval1.$$minus($eval2)")
    case ByteType | ShortType =>
      defineCodeGen(ctx, ev,
        (eval1, eval2) => s"(${ctx.javaType(dataType)})($eval1 $symbol $eval2)")
    case CalendarIntervalType =>
      defineCodeGen(ctx, ev, (eval1, eval2) => s"$eval1.subtract($eval2)")
    case _ =>
      defineCodeGen(ctx, ev, (eval1, eval2) => s"$eval1 $symbol $eval2")
  }
}

@ExpressionDescription(
  usage = "expr1 _FUNC_ expr2 - Returns `expr1`*`expr2`.",
  extended =
    """
    Examples:
      > SELECT 2 _FUNC_ 3;
       6
  """)
case class Multiply(left: Expression, right: Expression)
  extends BinaryArithmetic with NullIntolerant {

  override def inputType: AbstractDataType = NumericType

  override def symbol: String = "*"

  override def decimalMethod: String = "$times"

  private lazy val numeric = TypeUtils.getNumeric(dataType)

  protected override def nullSafeEval(input1: Any, input2: Any): Any = numeric.times(input1, input2)
}

// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = "expr1 _FUNC_ expr2 - Returns `expr1`/`expr2`. It always performs floating point division.",
  extended =
    """
    Examples:
      > SELECT 3 _FUNC_ 2;
       1.5
      > SELECT 2L _FUNC_ 2L;
       1.0
  """)
// scalastyle:on line.size.limit
case class Divide(left: Expression, right: Expression)
  extends BinaryArithmetic with NullIntolerant {

  override def inputType: AbstractDataType = TypeCollection(DoubleType, DecimalType)

  override def symbol: String = "/"

  override def decimalMethod: String = "$div"

  override def nullable: Boolean = true

  private lazy val div: (Any, Any) => Any = dataType match {
    case ft: FractionalType => ft.fractional.asInstanceOf[Fractional[Any]].div
  }

  override def eval(input: InternalRow): Any = {
    val input2 = right.eval(input)
    if (input2 == null || input2 == 0) {
      null
    } else {
      val input1 = left.eval(input)
      if (input1 == null) {
        null
      } else {
        div(input1, input2)
      }
    }
  }

  /**
    * Special case handling due to division by 0 => null.
    */
  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val eval1 = left.genCode(ctx)
    val eval2 = right.genCode(ctx)
    val isZero = if (dataType.isInstanceOf[DecimalType]) {
      s"${eval2.value}.isZero()"
    } else {
      s"${eval2.value} == 0"
    }
    val javaType = ctx.javaType(dataType)
    val divide = if (dataType.isInstanceOf[DecimalType]) {
      s"${eval1.value}.$decimalMethod(${eval2.value})"
    } else {
      s"($javaType)(${eval1.value} $symbol ${eval2.value})"
    }
    if (!left.nullable && !right.nullable) {
      ev.copy(code =
        s"""
        ${eval2.code}
        boolean ${ev.isNull} = false;
        $javaType ${ev.value} = ${ctx.defaultValue(javaType)};
        if ($isZero) {
          ${ev.isNull} = true;
        } else {
          ${eval1.code}
          ${ev.value} = $divide;
        }""")
    } else {
      ev.copy(code =
        s"""
        ${eval2.code}
        boolean ${ev.isNull} = false;
        $javaType ${ev.value} = ${ctx.defaultValue(javaType)};
        if (${eval2.isNull}|| $isZero) {
          ${ev.isNull} = true;
        } else {
          ${eval1.code}
          if (${eval1.isNull}) {
            ${ev.isNull} = true;
          } else {
            ${ev.value} = $divide;
          }
        }""")
    }
  }
}

@ExpressionDescription(
  usage = "expr1 _FUNC_ expr2 - Returns the remainder after `expr1`/`expr2`.",
  extended =
    """
    Examples:
      > SELECT 2 _FUNC_ 1.8;
       0.2
  """)
case class Remainder(left: Expression, right: Expression)
  extends BinaryArithmetic with NullIntolerant {

  override def inputType: AbstractDataType = NumericType

  override def symbol: String = "%"

  override def decimalMethod: String = "remainder"

  override def nullable: Boolean = true

  private lazy val integral = dataType match {
    case i: IntegralType => i.integral.asInstanceOf[Integral[Any]]
    case i: FractionalType => i.asIntegral.asInstanceOf[Integral[Any]]
  }

  override def eval(input: InternalRow): Any = {
    val input2 = right.eval(input)
    if (input2 == null || input2 == 0) {
      null
    } else {
      val input1 = left.eval(input)
      if (input1 == null) {
        null
      } else {
        input1 match {
          case d: Double => d % input2.asInstanceOf[java.lang.Double]
          case f: Float => f % input2.asInstanceOf[java.lang.Float]
          case _ => integral.rem(input1, input2)
        }
      }
    }
  }

  /**
    * Special case handling for x % 0 ==> null.
    */
  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val eval1 = left.genCode(ctx)
    val eval2 = right.genCode(ctx)
    val isZero = if (dataType.isInstanceOf[DecimalType]) {
      s"${eval2.value}.isZero()"
    } else {
      s"${eval2.value} == 0"
    }
    val javaType = ctx.javaType(dataType)
    val remainder = if (dataType.isInstanceOf[DecimalType]) {
      s"${eval1.value}.$decimalMethod(${eval2.value})"
    } else {
      s"($javaType)(${eval1.value} $symbol ${eval2.value})"
    }
    if (!left.nullable && !right.nullable) {
      ev.copy(code =
        s"""
        ${eval2.code}
        boolean ${ev.isNull} = false;
        $javaType ${ev.value} = ${ctx.defaultValue(javaType)};
        if ($isZero) {
          ${ev.isNull} = true;
        } else {
          ${eval1.code}
          ${ev.value} = $remainder;
        }""")
    } else {
      ev.copy(code =
        s"""
        ${eval2.code}
        boolean ${ev.isNull} = false;
        $javaType ${ev.value} = ${ctx.defaultValue(javaType)};
        if (${eval2.isNull}|| $isZero) {
          ${ev.isNull} = true;
        } else {
          ${eval1.code}
          if (${eval1.isNull}) {
            ${ev.isNull} = true;
          } else {
            ${ev.value} = $remainder;
          }
        }""")
    }
  }
}


@ExpressionDescription(
  usage = "_FUNC_(expr1, expr2) - Returns value of `expr1` mod `expr2`.",
  extended =
    """
    Examples:
      > SELECT _FUNC_(10, 3);
       1
      > SELECT _FUNC_(-10, 3);
       -1
  """)
case class Mod(left: Expression, right: Expression) extends BinaryArithmetic with NullIntolerant {
  /**
    * Expected input type from both left/right child expressions, similar to the
    * [[ImplicitCastInputTypes]] trait.
    */
  override def inputType: AbstractDataType = NumericType

  override def symbol: String = "mod"

  override def toString: String = s"mod($left, $right)"

  override def sql: String = s"$prettyName(${left.sql}, ${right.sql})"

  /**
    * Called by default [[eval]] implementation.  If subclass of BinaryExpression keep the default
    * nullability, they can override this method to save null-check code.  If we need full control
    * of evaluation process, we should override [[eval]].
    */
  override protected def nullSafeEval(left: Any, right: Any): Any = {
    dataType match {
      case IntegerType => mod(left.asInstanceOf[Int], right.asInstanceOf[Int])
      case LongType => mod(left.asInstanceOf[Long], right.asInstanceOf[Long])
      case ShortType => mod(left.asInstanceOf[Short], right.asInstanceOf[Short])
      case ByteType => mod(left.asInstanceOf[Byte], right.asInstanceOf[Byte])
      case FloatType => mod(left.asInstanceOf[Float], right.asInstanceOf[Float])
      case DoubleType => mod(left.asInstanceOf[Double], right.asInstanceOf[Double])
      case _: DecimalType => mod(left.asInstanceOf[Decimal], right.asInstanceOf[Decimal])
    }
  }


  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    nullSafeCodeGen(ctx, ev, (leftVal, rightVal) => {
      s"""
         |${ev.value} = $rightVal == 0 ? $leftVal : ($leftVal % $rightVal);
            """.stripMargin
    })
  }

  private def mod(a: Int, n: Int): Int = {
    if (0 == n) a else a % n
  }

  private def mod(a: Long, n: Long): Long = {
    if (0 == n) a else a % n
  }

  private def mod(a: Byte, n: Byte): Byte = {
    if (0 == n) a else (a % n).toByte
  }

  private def mod(a: Double, n: Double): Double = {
    if (0 == n) a else a % n
  }

  private def mod(a: Short, n: Short): Short = {
    if (0 == n) a else (a % n).toShort
  }

  private def mod(a: Float, n: Float): Float = {
    if (0 == n) a else a % n
  }

  private def mod(a: Decimal, n: Decimal): Decimal = {
    if (0 == n) a else a % n
  }

}


@ExpressionDescription(
  usage = "_FUNC_(expr1, expr2) - Returns the positive value of `expr1` mod `expr2`.",
  extended =
    """
    Examples:
      > SELECT _FUNC_(10, 3);
       1
      > SELECT _FUNC_(-10, 3);
       2
  """)
case class Pmod(left: Expression, right: Expression) extends BinaryArithmetic with NullIntolerant {

  override def toString: String = s"pmod($left, $right)"

  override def symbol: String = "pmod"

  protected def checkTypesInternal(t: DataType) =
    TypeUtils.checkForNumericExpr(t, "pmod")

  override def inputType: AbstractDataType = NumericType

  protected override def nullSafeEval(left: Any, right: Any) =
    dataType match {
      case IntegerType => pmod(left.asInstanceOf[Int], right.asInstanceOf[Int])
      case LongType => pmod(left.asInstanceOf[Long], right.asInstanceOf[Long])
      case ShortType => pmod(left.asInstanceOf[Short], right.asInstanceOf[Short])
      case ByteType => pmod(left.asInstanceOf[Byte], right.asInstanceOf[Byte])
      case FloatType => pmod(left.asInstanceOf[Float], right.asInstanceOf[Float])
      case DoubleType => pmod(left.asInstanceOf[Double], right.asInstanceOf[Double])
      case _: DecimalType => pmod(left.asInstanceOf[Decimal], right.asInstanceOf[Decimal])
    }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    nullSafeCodeGen(ctx, ev, (eval1, eval2) => {
      val remainder = ctx.freshName("remainder")
      dataType match {
        case dt: DecimalType =>
          val decimalAdd = "$plus"
          s"""
            ${ctx.javaType(dataType)} $remainder = $eval1.remainder($eval2);
            if ($remainder.compare(new org.apache.spark.sql.types.Decimal().set(0)) < 0) {
              ${ev.value} = ($remainder.$decimalAdd($eval2)).remainder($eval2);
            } else {
              ${ev.value} = $remainder;
            }
          """
        // byte and short are casted into int when add, minus, times or divide
        case ByteType | ShortType =>
          s"""
            ${ctx.javaType(dataType)} $remainder = (${ctx.javaType(dataType)})($eval1 % $eval2);
            if ($remainder < 0) {
              ${ev.value} = (${ctx.javaType(dataType)})(($remainder + $eval2) % $eval2);
            } else {
              ${ev.value} = $remainder;
            }
          """
        case _ =>
          s"""
            ${ctx.javaType(dataType)} $remainder = $eval1 % $eval2;
            if ($remainder < 0) {
              ${ev.value} = ($remainder + $eval2) % $eval2;
            } else {
              ${ev.value} = $remainder;
            }
          """
      }
    })
  }

  private def pmod(a: Int, n: Int): Int = {
    val r = a % n
    if (r < 0) {
      (r + n) % n
    } else r
  }

  private def pmod(a: Long, n: Long): Long = {
    val r = a % n
    if (r < 0) {
      (r + n) % n
    } else r
  }

  private def pmod(a: Byte, n: Byte): Byte = {
    val r = a % n
    if (r < 0) {
      ((r + n) % n).toByte
    } else r.toByte
  }

  private def pmod(a: Double, n: Double): Double = {
    val r = a % n
    if (r < 0) {
      (r + n) % n
    } else r
  }

  private def pmod(a: Short, n: Short): Short = {
    val r = a % n
    if (r < 0) {
      ((r + n) % n).toShort
    } else r.toShort
  }

  private def pmod(a: Float, n: Float): Float = {
    val r = a % n
    if (r < 0) {
      (r + n) % n
    } else r
  }

  private def pmod(a: Decimal, n: Decimal): Decimal = {
    val r = a % n
    if (r.compare(Decimal.ZERO) < 0) {
      (r + n) % n
    } else r
  }

  override def sql: String = s"$prettyName(${left.sql}, ${right.sql})"
}

/**
  * A function that returns the least value of all parameters, skipping null values.
  * It takes at least 2 parameters, and returns null iff all parameters are null.
  */
@ExpressionDescription(
  usage = "_FUNC_(expr, ...) - Returns the least value of all parameters, skipping null values.",
  extended =
    """
    Examples:
      > SELECT _FUNC_(10, 9, 2, 4, 3);
       2
  """)
case class Least(children: Seq[Expression]) extends Expression {

  override def nullable: Boolean = children.forall(_.nullable)

  override def foldable: Boolean = children.forall(_.foldable)

  private lazy val ordering = TypeUtils.getInterpretedOrdering(dataType)

  override def checkInputDataTypes(): TypeCheckResult = {
    if (children.length <= 1) {
      TypeCheckResult.TypeCheckFailure(s"LEAST requires at least 2 arguments")
    } else if (children.map(_.dataType).distinct.count(_ != NullType) > 1) {
      TypeCheckResult.TypeCheckFailure(
        s"The expressions should all have the same type," +
          s" got LEAST(${children.map(_.dataType.simpleString).mkString(", ")}).")
    } else {
      TypeUtils.checkForOrderingExpr(dataType, "function " + prettyName)
    }
  }

  override def dataType: DataType = children.head.dataType

  override def eval(input: InternalRow): Any = {
    children.foldLeft[Any](null)((r, c) => {
      val evalc = c.eval(input)
      if (evalc != null) {
        if (r == null || ordering.lt(evalc, r)) evalc else r
      } else {
        r
      }
    })
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val evalChildren = children.map(_.genCode(ctx))
    val first = evalChildren(0)
    val rest = evalChildren.drop(1)

    def updateEval(eval: ExprCode): String = {
      s"""
        ${eval.code}
        if (!${eval.isNull} && (${ev.isNull}||
          ${ctx.genGreater(dataType, ev.value, eval.value)})) {
          ${ev.isNull} = false;
          ${ev.value} = ${eval.value};
        }
      """
    }

    ev.copy(code =
      s"""
      ${first.code}
      boolean ${ev.isNull} = ${first.isNull};
      ${ctx.javaType(dataType)} ${ev.value} = ${first.value};
      ${rest.map(updateEval).mkString("\n")}""")
  }
}


@ExpressionDescription(
  usage = "_FUNC_(expr, ...) - Returns the greatest value of all parameters, skipping null values.",
  extended =
    """
    Examples:
      > SELECT _FUNC_(1, 0, 1, 0);
       10
  """)
case class BinToNum(children: Seq[Expression]) extends Expression {

  override def nullable: Boolean = true


  /**
    * Returns true when an expression is a candidate for static evaluation before the query is
    * executed.
    *
    * The following conditions are used to determine suitability for constant folding:
    *  - A [[Coalesce]] is foldable if all of its children are foldable
    *  - A [[BinaryExpression]] is foldable if its both left and right child are foldable
    *  - A [[Not]], [[IsNull]], or [[IsNotNull]] is foldable if its child is foldable
    *  - A [[Literal]] is foldable
    *  - A [[Cast]] or [[UnaryMinus]] is foldable if its child is foldable
    */
  override def foldable: Boolean = children.forall(_.foldable)

  /** Returns the result of evaluating this expression on a given input Row */
  override def eval(input: InternalRow): Any = {
    children.map(_.eval(input).asInstanceOf[Int].toLong).
      reverse.zipWithIndex.foldLeft[Long](0)((result, current) => {
      result + {
        current._1 match {
          case 0 => 0L
          case 1 => scala.math.pow(2, current._2).toLong
        }
      }
    })
  }

  /**
    * Returns Java source code that can be compiled to evaluate this expression.
    * The default behavior is to call the eval method of the expression. Concrete expression
    * implementations should override this to do actual code generation.
    *
    * @param ctx a [[CodegenContext]]
    * @param ev  an [[ExprCode]] with unique terms.
    * @return an [[ExprCode]] containing the Java source code to generate the given expression
    */
  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val childCodes = children.map(_.genCode(ctx))
    val childVals = childCodes.map(_.value).reverse.zipWithIndex
    val sum = ctx.freshName("sum")
    val str = childVals.map(attr =>
      s"""
        | if (Long.valueOf(${attr._1}) == 1L) {
        |   sum += Math.pow(2, ${attr._2});
        | }
      """.stripMargin).mkString("\n")

    ev.copy(code = childCodes.map(_.code).mkString("\n") +
      s"""
        | boolean ${ev.isNull} = false;
        | long $sum = 0L;
        | $str
        | ${ctx.javaType(dataType)} ${ev.value} = $sum;
      """.stripMargin)
  }

  /**
    * Returns the [[DataType]] of the result of evaluating this expression.  It is
    * invalid to query the dataType of an unresolved expression (i.e., when `resolved` == false).
    */
  override def dataType: DataType = LongType
}

/**
  * A function that returns the greatest value of all parameters, skipping null values.
  * It takes at least 2 parameters, and returns null iff all parameters are null.
  */
@ExpressionDescription(
  usage = "_FUNC_(expr, ...) - Returns the greatest value of all parameters, skipping null values.",
  extended =
    """
    Examples:
      > SELECT _FUNC_(10, 9, 2, 4, 3);
       10
  """)
case class Greatest(children: Seq[Expression]) extends Expression {

  override def nullable: Boolean = children.forall(_.nullable)

  override def foldable: Boolean = children.forall(_.foldable)

  private lazy val ordering = TypeUtils.getInterpretedOrdering(dataType)

  override def checkInputDataTypes(): TypeCheckResult = {
    if (children.length <= 1) {
      TypeCheckResult.TypeCheckFailure(s"GREATEST requires at least 2 arguments")
    } else if (children.map(_.dataType).distinct.count(_ != NullType) > 1) {
      TypeCheckResult.TypeCheckFailure(
        s"The expressions should all have the same type," +
          s" got GREATEST(${children.map(_.dataType.simpleString).mkString(", ")}).")
    } else {
      TypeUtils.checkForOrderingExpr(dataType, "function " + prettyName)
    }
  }

  override def dataType: DataType = children.head.dataType

  override def eval(input: InternalRow): Any = {
    children.foldLeft[Any](null)((r, c) => {
      val evalc = c.eval(input)
      if (evalc != null) {
        if (r == null || ordering.gt(evalc, r)) evalc else r
      } else {
        r
      }
    })
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val evalChildren = children.map(_.genCode(ctx))
    val first = evalChildren(0)
    val rest = evalChildren.drop(1)

    def updateEval(eval: ExprCode): String = {
      s"""
        ${eval.code}
        if (!${eval.isNull} && (${ev.isNull}||
          ${ctx.genGreater(dataType, eval.value, ev.value)})) {
          ${ev.isNull} = false;
          ${ev.value} = ${eval.value};
        }
      """
    }

    ev.copy(code =
      s"""
      ${first.code}
      boolean ${ev.isNull} = ${first.isNull};
      ${ctx.javaType(dataType)} ${ev.value} = ${first.value};
      ${rest.map(updateEval).mkString("\n")}""")
  }
}
