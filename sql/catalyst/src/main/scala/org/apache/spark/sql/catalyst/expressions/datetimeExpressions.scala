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

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.{Calendar, Locale, TimeZone}

import scala.util.Try

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, CodegenFallback, ExprCode}
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.{CalendarInterval, UTF8String}

/**
 * Returns the current date at the start of query evaluation.
 * All calls of current_date within the same query return the same value.
 *
 * There is no code generation since this expression should get constant folded by the optimizer.
 */
@ExpressionDescription(
  usage = "_FUNC_() - Returns the current date at the start of query evaluation.")
case class CurrentDate() extends LeafExpression with CodegenFallback {
  override def foldable: Boolean = true
  override def nullable: Boolean = false

  override def dataType: DataType = DateType

  override def eval(input: InternalRow): Any = {
    DateTimeUtils.millisToDays(System.currentTimeMillis())
  }

  override def prettyName: String = "current_date"
}

/**
 * Returns the current timestamp at the start of query evaluation.
 * All calls of current_timestamp within the same query return the same value.
 *
 * There is no code generation since this expression should get constant folded by the optimizer.
 */
@ExpressionDescription(
  usage = "_FUNC_() - Returns the current timestamp at the start of query evaluation.")
case class CurrentTimestamp() extends LeafExpression with CodegenFallback {
  override def foldable: Boolean = true
  override def nullable: Boolean = false

  override def dataType: DataType = TimestampType

  override def eval(input: InternalRow): Any = {
    System.currentTimeMillis() * 1000L
  }

  override def prettyName: String = "current_timestamp"
}

/**
 * Expression representing the current batch time, which is used by StreamExecution to
 * 1. prevent optimizer from pushing this expression below a stateful operator
 * 2. allow IncrementalExecution to substitute this expression with a Literal(timestamp)
 *
 * There is no code generation since this expression should be replaced with a literal.
 */
case class CurrentBatchTimestamp(timestampMs: Long, dataType: DataType)
  extends LeafExpression with Nondeterministic with CodegenFallback {

  override def nullable: Boolean = false

  override def prettyName: String = "current_batch_timestamp"

  override protected def initializeInternal(partitionIndex: Int): Unit = {}

  /**
   * Need to return literal value in order to support compile time expression evaluation
   * e.g., select(current_date())
   */
  override protected def evalInternal(input: InternalRow): Any = toLiteral.value

  def toLiteral: Literal = dataType match {
    case _: TimestampType =>
      Literal(DateTimeUtils.fromJavaTimestamp(new Timestamp(timestampMs)), TimestampType)
    case _: DateType => Literal(DateTimeUtils.millisToDays(timestampMs), DateType)
  }
}


case class DateExtract(dateExpr: Expression, unit: Expression)
  extends BinaryExpression with ImplicitCastInputTypes {

  override def left: Expression = dateExpr

  override def right: Expression = unit

  /**
    * Expected input types from child expressions. The i-th position in the returned seq indicates
    * the type requirement for the i-th child.
    *
    * The possible values at each position are:
    * 1. a specific data type, e.g. LongType, StringType.
    * 2. a non-leaf abstract data type, e.g. NumericType, IntegralType, FractionalType.
    */
  override def inputTypes: Seq[AbstractDataType] = Seq(DateType, StringType)


  lazy val unitDate = right.eval().toString.toUpperCase

  /**
    * Default behavior of evaluation according to the default nullability of BinaryExpression.
    * If subclass of BinaryExpression override nullable, probably should also override this.
    */
  override def eval(input: InternalRow): Any = {
    val value = left.eval(input).asInstanceOf[Int]
    unitDate match {
      case "YEAR" => DateTimeUtils.getYear(value)
      case "MONTH" => DateTimeUtils.getMonth(value)
      case "DAY" => DateTimeUtils.getDayOfMonth(value)
    }
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
    val dtu = DateTimeUtils.getClass.getName.stripSuffix("$")

    nullSafeCodeGen(ctx, ev, (exprValue, unitValue) => {
      s"""
        | ${ev.value} = $dtu.getExtract($exprValue, $unitValue);
      """.stripMargin
    })
  }


  /**
    * Returns the [[DataType]] of the result of evaluating this expression.  It is
    * invalid to query the dataType of an unresolved expression (i.e., when `resolved` == false).
    */
  override def dataType: DataType = IntegerType

  override def prettyName: String = "extract"
}



/**
 * Adds a number of days to startdate.
 */
@ExpressionDescription(
  usage = "_FUNC_(start_date, num_days) - Returns the date that is `num_days` after `start_date`.",
  extended = """
    Examples:
      > SELECT _FUNC_('2016-07-30', 1);
       2016-07-31
  """)
case class DateAdd(startDate: Expression, days: Expression)
  extends BinaryExpression with ImplicitCastInputTypes {

  override def left: Expression = startDate
  override def right: Expression = days

  override def inputTypes: Seq[AbstractDataType] = Seq(DateType, IntegerType)

  override def dataType: DataType = DateType

  override def nullSafeEval(start: Any, d: Any): Any = {
    start.asInstanceOf[Int] + d.asInstanceOf[Int]
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    nullSafeCodeGen(ctx, ev, (sd, d) => {
      s"""${ev.value} = $sd + $d;"""
    })
  }

  override def prettyName: String = "date_add"
}

/**
 * Subtracts a number of days to startdate.
 */
@ExpressionDescription(
  usage = "_FUNC_(start_date, num_days) - Returns the date that is `num_days` before `start_date`.",
  extended = """
    Examples:
      > SELECT _FUNC_('2016-07-30', 1);
       2016-07-29
  """)
case class DateSub(startDate: Expression, days: Expression)
  extends BinaryExpression with ImplicitCastInputTypes {
  override def left: Expression = startDate
  override def right: Expression = days

  override def inputTypes: Seq[AbstractDataType] = Seq(DateType, IntegerType)

  override def dataType: DataType = DateType

  override def nullSafeEval(start: Any, d: Any): Any = {
    start.asInstanceOf[Int] - d.asInstanceOf[Int]
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    nullSafeCodeGen(ctx, ev, (sd, d) => {
      s"""${ev.value} = $sd - $d;"""
    })
  }

  override def prettyName: String = "date_sub"
}

@ExpressionDescription(
  usage = "_FUNC_(timestamp) - Returns the hour component of the string/timestamp.",
  extended = """
    Examples:
      > SELECT _FUNC_('2009-07-30 12:58:59');
       12
  """)
case class Hour(child: Expression) extends UnaryExpression with ImplicitCastInputTypes {

  override def inputTypes: Seq[AbstractDataType] = Seq(TimestampType)

  override def dataType: DataType = IntegerType

  override protected def nullSafeEval(timestamp: Any): Any = {
    DateTimeUtils.getHours(timestamp.asInstanceOf[Long])
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val dtu = DateTimeUtils.getClass.getName.stripSuffix("$")
    defineCodeGen(ctx, ev, c => s"$dtu.getHours($c)")
  }
}

@ExpressionDescription(
  usage = "_FUNC_(timestamp) - Returns the minute component of the string/timestamp.",
  extended = """
    Examples:
      > SELECT _FUNC_('2009-07-30 12:58:59');
       58
  """)
case class Minute(child: Expression) extends UnaryExpression with ImplicitCastInputTypes {

  override def inputTypes: Seq[AbstractDataType] = Seq(TimestampType)

  override def dataType: DataType = IntegerType

  override protected def nullSafeEval(timestamp: Any): Any = {
    DateTimeUtils.getMinutes(timestamp.asInstanceOf[Long])
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val dtu = DateTimeUtils.getClass.getName.stripSuffix("$")
    defineCodeGen(ctx, ev, c => s"$dtu.getMinutes($c)")
  }
}

@ExpressionDescription(
  usage = "_FUNC_(timestamp) - Returns the second component of the string/timestamp.",
  extended = """
    Examples:
      > SELECT _FUNC_('2009-07-30 12:58:59');
       59
  """)
case class Second(child: Expression) extends UnaryExpression with ImplicitCastInputTypes {

  override def inputTypes: Seq[AbstractDataType] = Seq(TimestampType)

  override def dataType: DataType = IntegerType

  override protected def nullSafeEval(timestamp: Any): Any = {
    DateTimeUtils.getSeconds(timestamp.asInstanceOf[Long])
  }

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val dtu = DateTimeUtils.getClass.getName.stripSuffix("$")
    defineCodeGen(ctx, ev, c => s"$dtu.getSeconds($c)")
  }
}

@ExpressionDescription(
  usage = "_FUNC_(date) - Returns the day of year of the date/timestamp.",
  extended = """
    Examples:
      > SELECT _FUNC_('2016-04-09');
       100
  """)
case class DayOfYear(child: Expression) extends UnaryExpression with ImplicitCastInputTypes {

  override def inputTypes: Seq[AbstractDataType] = Seq(DateType)

  override def dataType: DataType = IntegerType

  override protected def nullSafeEval(date: Any): Any = {
    DateTimeUtils.getDayInYear(date.asInstanceOf[Int])
  }

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val dtu = DateTimeUtils.getClass.getName.stripSuffix("$")
    defineCodeGen(ctx, ev, c => s"$dtu.getDayInYear($c)")
  }
}

@ExpressionDescription(
  usage = "_FUNC_(date) - Returns the year component of the date/timestamp.",
  extended = """
    Examples:
      > SELECT _FUNC_('2016-07-30');
       2016
  """)
case class Year(child: Expression) extends UnaryExpression with ImplicitCastInputTypes {

  override def inputTypes: Seq[AbstractDataType] = Seq(DateType)

  override def dataType: DataType = IntegerType

  override protected def nullSafeEval(date: Any): Any = {
    DateTimeUtils.getYear(date.asInstanceOf[Int])
  }

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val dtu = DateTimeUtils.getClass.getName.stripSuffix("$")
    defineCodeGen(ctx, ev, c => s"$dtu.getYear($c)")
  }
}

@ExpressionDescription(
  usage = "_FUNC_(date) - Returns the quarter of the year for date, in the range 1 to 4.",
  extended = """
    Examples:
      > SELECT _FUNC_('2016-08-31');
       3
  """)
case class Quarter(child: Expression) extends UnaryExpression with ImplicitCastInputTypes {

  override def inputTypes: Seq[AbstractDataType] = Seq(DateType)

  override def dataType: DataType = IntegerType

  override protected def nullSafeEval(date: Any): Any = {
    DateTimeUtils.getQuarter(date.asInstanceOf[Int])
  }

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val dtu = DateTimeUtils.getClass.getName.stripSuffix("$")
    defineCodeGen(ctx, ev, c => s"$dtu.getQuarter($c)")
  }
}

@ExpressionDescription(
  usage = "_FUNC_(date) - Returns the month component of the date/timestamp.",
  extended = """
    Examples:
      > SELECT _FUNC_('2016-07-30');
       7
  """)
case class Month(child: Expression) extends UnaryExpression with ImplicitCastInputTypes {

  override def inputTypes: Seq[AbstractDataType] = Seq(DateType)

  override def dataType: DataType = IntegerType

  override protected def nullSafeEval(date: Any): Any = {
    DateTimeUtils.getMonth(date.asInstanceOf[Int])
  }

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val dtu = DateTimeUtils.getClass.getName.stripSuffix("$")
    defineCodeGen(ctx, ev, c => s"$dtu.getMonth($c)")
  }
}

@ExpressionDescription(
  usage = "_FUNC_(date) - Returns the day of month of the date/timestamp.",
  extended = """
    Examples:
      > SELECT _FUNC_('2009-07-30');
       30
  """)
case class DayOfMonth(child: Expression) extends UnaryExpression with ImplicitCastInputTypes {

  override def inputTypes: Seq[AbstractDataType] = Seq(DateType)

  override def dataType: DataType = IntegerType

  override protected def nullSafeEval(date: Any): Any = {
    DateTimeUtils.getDayOfMonth(date.asInstanceOf[Int])
  }

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val dtu = DateTimeUtils.getClass.getName.stripSuffix("$")
    defineCodeGen(ctx, ev, c => s"$dtu.getDayOfMonth($c)")
  }
}

@ExpressionDescription(
  usage = "_FUNC_(date) - Returns the week of the year of the given date.",
  extended = """
    Examples:
      > SELECT _FUNC_('2008-02-20');
       8
  """)
case class WeekOfYear(child: Expression) extends UnaryExpression with ImplicitCastInputTypes {

  override def inputTypes: Seq[AbstractDataType] = Seq(DateType)

  override def dataType: DataType = IntegerType

  @transient private lazy val c = {
//    val c = Calendar.getInstance(TimeZone.getTimeZone("UTC"))
    val c = Calendar.getInstance()
//    c.setFirstDayOfWeek(Calendar.MONDAY)
//    c.setMinimalDaysInFirstWeek(4)
    c
  }

  override protected def nullSafeEval(date: Any): Any = {
    c.setTimeInMillis(date.asInstanceOf[Int] * 1000L * 3600L * 24L)
    c.get(Calendar.WEEK_OF_YEAR)
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    nullSafeCodeGen(ctx, ev, time => {
      val cal = classOf[Calendar].getName
      val c = ctx.freshName("cal")
      ctx.addMutableState(cal, c,
        s"""
          $c = $cal.getInstance(java.util.TimeZone.getTimeZone("UTC"));
          $c.setFirstDayOfWeek($cal.MONDAY);
          $c.setMinimalDaysInFirstWeek(4);
         """)
      s"""
        $c.setTimeInMillis($time * 1000L * 3600L * 24L);
        ${ev.value} = $c.get($cal.WEEK_OF_YEAR);
      """
    })
  }
}

// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = "_FUNC_(timestamp, fmt) - Converts `timestamp` to a value of string in the format specified by the date format `fmt`.",
  extended = """
    Examples:
      > SELECT _FUNC_('2016-04-08', 'y');
       2016
  """)
// scalastyle:on line.size.limit
case class DateFormatClass(left: Expression, right: Expression) extends BinaryExpression
  with ImplicitCastInputTypes {

  override def dataType: DataType = StringType

  override def inputTypes: Seq[AbstractDataType] = Seq(TimestampType, StringType)

  override protected def nullSafeEval(timestamp: Any, format: Any): Any = {
    val sdf = new SimpleDateFormat(format.toString, Locale.US)
    UTF8String.fromString(sdf.format(new java.util.Date(timestamp.asInstanceOf[Long] / 1000)))
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val sdf = classOf[SimpleDateFormat].getName
    defineCodeGen(ctx, ev, (timestamp, format) => {
      s"""UTF8String.fromString((new $sdf($format.toString()))
          .format(new java.util.Date($timestamp / 1000)))"""
    })
  }

  override def prettyName: String = "date_format"
}

/**
 * Converts time string with given pattern.
 * Deterministic version of [[UnixTimestamp]], must have at least one parameter.
 */
@ExpressionDescription(
  usage = "_FUNC_(expr[, pattern]) - Returns the UNIX timestamp of the give time.",
  extended = """
    Examples:
      > SELECT _FUNC_('2016-04-08', 'yyyy-MM-dd');
       1460041200
  """)
case class ToUnixTimestamp(timeExp: Expression, format: Expression) extends UnixTime {
  override def left: Expression = timeExp
  override def right: Expression = format

  def this(time: Expression) = {
    this(time, Literal("yyyy-MM-dd HH:mm:ss"))
  }

  override def prettyName: String = "to_unix_timestamp"
}

/**
 * Converts time string with given pattern.
 * (see [http://docs.oracle.com/javase/tutorial/i18n/format/simpleDateFormat.html])
 * to Unix time stamp (in seconds), returns null if fail.
 * Note that hive Language Manual says it returns 0 if fail, but in fact it returns null.
 * If the second parameter is missing, use "yyyy-MM-dd HH:mm:ss".
 * If no parameters provided, the first parameter will be current_timestamp.
 * If the first parameter is a Date or Timestamp instead of String, we will ignore the
 * second parameter.
 */
@ExpressionDescription(
  usage = "_FUNC_([expr[, pattern]]) - Returns the UNIX timestamp of current or specified time.",
  extended = """
    Examples:
      > SELECT _FUNC_();
       1476884637
      > SELECT _FUNC_('2016-04-08', 'yyyy-MM-dd');
       1460041200
  """)
case class UnixTimestamp(timeExp: Expression, format: Expression) extends UnixTime {
  override def left: Expression = timeExp
  override def right: Expression = format

  def this(time: Expression) = {
    this(time, Literal("yyyy-MM-dd HH:mm:ss"))
  }

  def this() = {
    this(CurrentTimestamp())
  }

  override def prettyName: String = "unix_timestamp"
}

abstract class UnixTime extends BinaryExpression with ExpectsInputTypes {

  override def inputTypes: Seq[AbstractDataType] =
    Seq(TypeCollection(StringType, DateType, TimestampType), StringType)

  override def dataType: DataType = LongType
  override def nullable: Boolean = true

  private lazy val constFormat: UTF8String = right.eval().asInstanceOf[UTF8String]
  private lazy val formatter: SimpleDateFormat =
    Try(new SimpleDateFormat(constFormat.toString, Locale.US)).getOrElse(null)

  override def eval(input: InternalRow): Any = {
    val t = left.eval(input)
    if (t == null) {
      null
    } else {
      left.dataType match {
        case DateType =>
          DateTimeUtils.daysToMillis(t.asInstanceOf[Int]) / 1000L
        case TimestampType =>
          t.asInstanceOf[Long] / 1000000L
        case StringType if right.foldable =>
          if (constFormat == null || formatter == null) {
            null
          } else {
            Try(formatter.parse(
              t.asInstanceOf[UTF8String].toString).getTime / 1000L).getOrElse(null)
          }
        case StringType =>
          val f = right.eval(input)
          if (f == null) {
            null
          } else {
            val formatString = f.asInstanceOf[UTF8String].toString
            Try(new SimpleDateFormat(formatString, Locale.US).parse(
              t.asInstanceOf[UTF8String].toString).getTime / 1000L).getOrElse(null)
          }
      }
    }
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    left.dataType match {
      case StringType if right.foldable =>
        val sdf = classOf[SimpleDateFormat].getName
        if (formatter == null) {
          ExprCode("", "true", ctx.defaultValue(dataType))
        } else {
          val formatterName = ctx.addReferenceObj("formatter", formatter, sdf)
          val eval1 = left.genCode(ctx)
          ev.copy(code = s"""
            ${eval1.code}
            boolean ${ev.isNull} = ${eval1.isNull};
            ${ctx.javaType(dataType)} ${ev.value} = ${ctx.defaultValue(dataType)};
            if (!${ev.isNull}) {
              try {
                ${ev.value} = $formatterName.parse(${eval1.value}.toString()).getTime() / 1000L;
              } catch (java.text.ParseException e) {
                ${ev.isNull} = true;
              }
            }""")
        }
      case StringType =>
        val sdf = classOf[SimpleDateFormat].getName
        nullSafeCodeGen(ctx, ev, (string, format) => {
          s"""
            try {
              ${ev.value} =
                (new $sdf($format.toString())).parse($string.toString()).getTime() / 1000L;
            } catch (java.lang.IllegalArgumentException e) {
              ${ev.isNull} = true;
            } catch (java.text.ParseException e) {
              ${ev.isNull} = true;
            }
          """
        })
      case TimestampType =>
        val eval1 = left.genCode(ctx)
        ev.copy(code = s"""
          ${eval1.code}
          boolean ${ev.isNull} = ${eval1.isNull};
          ${ctx.javaType(dataType)} ${ev.value} = ${ctx.defaultValue(dataType)};
          if (!${ev.isNull}) {
            ${ev.value} = ${eval1.value} / 1000000L;
          }""")
      case DateType =>
        val dtu = DateTimeUtils.getClass.getName.stripSuffix("$")
        val eval1 = left.genCode(ctx)
        ev.copy(code = s"""
          ${eval1.code}
          boolean ${ev.isNull} = ${eval1.isNull};
          ${ctx.javaType(dataType)} ${ev.value} = ${ctx.defaultValue(dataType)};
          if (!${ev.isNull}) {
            ${ev.value} = $dtu.daysToMillis(${eval1.value}) / 1000L;
          }""")
    }
  }
}

/**
 * Converts the number of seconds from unix epoch (1970-01-01 00:00:00 UTC) to a string
 * representing the timestamp of that moment in the current system time zone in the given
 * format. If the format is missing, using format like "1970-01-01 00:00:00".
 * Note that hive Language Manual says it returns 0 if fail, but in fact it returns null.
 */
@ExpressionDescription(
  usage = "_FUNC_(unix_time, format) - Returns `unix_time` in the specified `format`.",
  extended = """
    Examples:
      > SELECT _FUNC_(0, 'yyyy-MM-dd HH:mm:ss');
       1970-01-01 00:00:00
  """)
case class FromUnixTime(sec: Expression, format: Expression)
  extends BinaryExpression with ImplicitCastInputTypes {

  override def left: Expression = sec
  override def right: Expression = format

  override def prettyName: String = "from_unixtime"

  def this(unix: Expression) = {
    this(unix, Literal("yyyy-MM-dd HH:mm:ss"))
  }

  override def dataType: DataType = StringType
  override def nullable: Boolean = true

  override def inputTypes: Seq[AbstractDataType] = Seq(LongType, StringType)

  private lazy val constFormat: UTF8String = right.eval().asInstanceOf[UTF8String]
  private lazy val formatter: SimpleDateFormat =
    Try(new SimpleDateFormat(constFormat.toString, Locale.US)).getOrElse(null)

  override def eval(input: InternalRow): Any = {
    val time = left.eval(input)
    if (time == null) {
      null
    } else {
      if (format.foldable) {
        if (constFormat == null || formatter == null) {
          null
        } else {
          Try(UTF8String.fromString(formatter.format(
            new java.util.Date(time.asInstanceOf[Long] * 1000L)))).getOrElse(null)
        }
      } else {
        val f = format.eval(input)
        if (f == null) {
          null
        } else {
          Try(
            UTF8String.fromString(new SimpleDateFormat(f.toString, Locale.US).
              format(new java.util.Date(time.asInstanceOf[Long] * 1000L)))
          ).getOrElse(null)
        }
      }
    }
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val sdf = classOf[SimpleDateFormat].getName
    if (format.foldable) {
      if (formatter == null) {
        ExprCode("", "true", "(UTF8String) null")
      } else {
        val formatterName = ctx.addReferenceObj("formatter", formatter, sdf)
        val t = left.genCode(ctx)
        ev.copy(code = s"""
          ${t.code}
          boolean ${ev.isNull} = ${t.isNull};
          ${ctx.javaType(dataType)} ${ev.value} = ${ctx.defaultValue(dataType)};
          if (!${ev.isNull}) {
            try {
              ${ev.value} = UTF8String.fromString($formatterName.format(
                new java.util.Date(${t.value} * 1000L)));
            } catch (java.lang.IllegalArgumentException e) {
              ${ev.isNull} = true;
            }
          }""")
      }
    } else {
      nullSafeCodeGen(ctx, ev, (seconds, f) => {
        s"""
        try {
          ${ev.value} = UTF8String.fromString((new $sdf($f.toString())).format(
            new java.util.Date($seconds * 1000L)));
        } catch (java.lang.IllegalArgumentException e) {
          ${ev.isNull} = true;
        }""".stripMargin
      })
    }
  }
}

/**
 * Returns the last day of the month which the date belongs to.
 */
@ExpressionDescription(
  usage = "_FUNC_(date) - Returns the last day of the month which the date belongs to.",
  extended = """
    Examples:
      > SELECT _FUNC_('2009-01-12');
       2009-01-31
  """)
case class LastDay(startDate: Expression) extends UnaryExpression with ImplicitCastInputTypes {
  override def child: Expression = startDate

  override def inputTypes: Seq[AbstractDataType] = Seq(DateType)

  override def dataType: DataType = DateType

  override def nullSafeEval(date: Any): Any = {
    DateTimeUtils.getLastDayOfMonth(date.asInstanceOf[Int])
  }

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val dtu = DateTimeUtils.getClass.getName.stripSuffix("$")
    defineCodeGen(ctx, ev, sd => s"$dtu.getLastDayOfMonth($sd)")
  }

  override def prettyName: String = "last_day"
}

/**
 * Returns the first date which is later than startDate and named as dayOfWeek.
 * For example, NextDay(2015-07-27, Sunday) would return 2015-08-02, which is the first
 * Sunday later than 2015-07-27.
 *
 * Allowed "dayOfWeek" is defined in [[DateTimeUtils.getDayOfWeekFromString]].
 */
// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = "_FUNC_(start_date, day_of_week) - Returns the first date which is later than `start_date` and named as indicated.",
  extended = """
    Examples:
      > SELECT _FUNC_('2015-01-14', 'TU');
       2015-01-20
  """)
// scalastyle:on line.size.limit
case class NextDay(startDate: Expression, dayOfWeek: Expression)
  extends BinaryExpression with ImplicitCastInputTypes {

  override def left: Expression = startDate
  override def right: Expression = dayOfWeek

  override def inputTypes: Seq[AbstractDataType] = Seq(DateType, StringType)

  override def dataType: DataType = DateType
  override def nullable: Boolean = true

  override def nullSafeEval(start: Any, dayOfW: Any): Any = {
    val dow = DateTimeUtils.getDayOfWeekFromString(dayOfW.asInstanceOf[UTF8String])
    if (dow == -1) {
      null
    } else {
      val sd = start.asInstanceOf[Int]
      DateTimeUtils.getNextDateForDayOfWeek(sd, dow)
    }
  }

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    nullSafeCodeGen(ctx, ev, (sd, dowS) => {
      val dateTimeUtilClass = DateTimeUtils.getClass.getName.stripSuffix("$")
      val dayOfWeekTerm = ctx.freshName("dayOfWeek")
      if (dayOfWeek.foldable) {
        val input = dayOfWeek.eval().asInstanceOf[UTF8String]
        if ((input eq null) || DateTimeUtils.getDayOfWeekFromString(input) == -1) {
          s"""
             |${ev.isNull} = true;
           """.stripMargin
        } else {
          val dayOfWeekValue = DateTimeUtils.getDayOfWeekFromString(input)
          s"""
             |${ev.value} = $dateTimeUtilClass.getNextDateForDayOfWeek($sd, $dayOfWeekValue);
           """.stripMargin
        }
      } else {
        s"""
           |int $dayOfWeekTerm = $dateTimeUtilClass.getDayOfWeekFromString($dowS);
           |if ($dayOfWeekTerm == -1) {
           |  ${ev.isNull} = true;
           |} else {
           |  ${ev.value} = $dateTimeUtilClass.getNextDateForDayOfWeek($sd, $dayOfWeekTerm);
           |}
         """.stripMargin
      }
    })
  }

  override def prettyName: String = "next_day"
}

/**
 * Adds an interval to timestamp.
 */
case class TimeAdd(start: Expression, interval: Expression)
  extends BinaryExpression with ImplicitCastInputTypes {

  override def left: Expression = start
  override def right: Expression = interval

  override def toString: String = s"$left + $right"
  override def sql: String = s"${left.sql} + ${right.sql}"
  override def inputTypes: Seq[AbstractDataType] = Seq(TimestampType, CalendarIntervalType)

  override def dataType: DataType = TimestampType

  override def nullSafeEval(start: Any, interval: Any): Any = {
    val itvl = interval.asInstanceOf[CalendarInterval]
    DateTimeUtils.timestampAddInterval(
      start.asInstanceOf[Long], itvl.months, itvl.microseconds)
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val dtu = DateTimeUtils.getClass.getName.stripSuffix("$")
    defineCodeGen(ctx, ev, (sd, i) => {
      s"""$dtu.timestampAddInterval($sd, $i.months, $i.microseconds)"""
    })
  }
}

/**
 * Given a timestamp, which corresponds to a certain time of day in UTC, returns another timestamp
 * that corresponds to the same time of day in the given timezone.
 */
// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = "_FUNC_(timestamp, timezone) - Given a timestamp, which corresponds to a certain time of day in UTC, returns another timestamp that corresponds to the same time of day in the given timezone.",
  extended = """
    Examples:
      > SELECT from_utc_timestamp('2016-08-31', 'Asia/Seoul');
       2016-08-31 09:00:00
  """)
// scalastyle:on line.size.limit
case class FromUTCTimestamp(left: Expression, right: Expression)
  extends BinaryExpression with ImplicitCastInputTypes {

  override def inputTypes: Seq[AbstractDataType] = Seq(TimestampType, StringType)
  override def dataType: DataType = TimestampType
  override def prettyName: String = "from_utc_timestamp"

  override def nullSafeEval(time: Any, timezone: Any): Any = {
    DateTimeUtils.fromUTCTime(time.asInstanceOf[Long],
      timezone.asInstanceOf[UTF8String].toString)
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val dtu = DateTimeUtils.getClass.getName.stripSuffix("$")
    if (right.foldable) {
      val tz = right.eval()
      if (tz == null) {
        ev.copy(code = s"""
           |boolean ${ev.isNull} = true;
           |long ${ev.value} = 0;
         """.stripMargin)
      } else {
        val tzTerm = ctx.freshName("tz")
        val utcTerm = ctx.freshName("utc")
        val tzClass = classOf[TimeZone].getName
        ctx.addMutableState(tzClass, tzTerm, s"""$tzTerm = $tzClass.getTimeZone("$tz");""")
        ctx.addMutableState(tzClass, utcTerm, s"""$utcTerm = $tzClass.getTimeZone("UTC");""")
        val eval = left.genCode(ctx)
        ev.copy(code = s"""
           |${eval.code}
           |boolean ${ev.isNull} = ${eval.isNull};
           |long ${ev.value} = 0;
           |if (!${ev.isNull}) {
           |  ${ev.value} = $dtu.convertTz(${eval.value}, $utcTerm, $tzTerm);
           |}
         """.stripMargin)
      }
    } else {
      defineCodeGen(ctx, ev, (timestamp, format) => {
        s"""$dtu.fromUTCTime($timestamp, $format.toString())"""
      })
    }
  }
}

/**
 * Subtracts an interval from timestamp.
 */
case class TimeSub(start: Expression, interval: Expression)
  extends BinaryExpression with ImplicitCastInputTypes {

  override def left: Expression = start
  override def right: Expression = interval

  override def toString: String = s"$left - $right"
  override def sql: String = s"${left.sql} - ${right.sql}"
  override def inputTypes: Seq[AbstractDataType] = Seq(TimestampType, CalendarIntervalType)

  override def dataType: DataType = TimestampType

  override def nullSafeEval(start: Any, interval: Any): Any = {
    val itvl = interval.asInstanceOf[CalendarInterval]
    DateTimeUtils.timestampAddInterval(
      start.asInstanceOf[Long], 0 - itvl.months, 0 - itvl.microseconds)
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val dtu = DateTimeUtils.getClass.getName.stripSuffix("$")
    defineCodeGen(ctx, ev, (sd, i) => {
      s"""$dtu.timestampAddInterval($sd, 0 - $i.months, 0 - $i.microseconds)"""
    })
  }
}

/**
 * Returns the date that is num_months after start_date.
 */
// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = "_FUNC_(start_date, num_months) - Returns the date that is `num_months` after `start_date`.",
  extended = """
    Examples:
      > SELECT _FUNC_('2016-08-31', 1);
       2016-09-30
  """)
// scalastyle:on line.size.limit
case class AddMonths(startDate: Expression, numMonths: Expression)
  extends BinaryExpression with ImplicitCastInputTypes {

  override def left: Expression = startDate
  override def right: Expression = numMonths

  override def inputTypes: Seq[AbstractDataType] = Seq(DateType, IntegerType)

  override def dataType: DataType = DateType

  override def nullSafeEval(start: Any, months: Any): Any = {
    DateTimeUtils.dateAddMonths(start.asInstanceOf[Int], months.asInstanceOf[Int])
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val dtu = DateTimeUtils.getClass.getName.stripSuffix("$")
    defineCodeGen(ctx, ev, (sd, m) => {
      s"""$dtu.dateAddMonths($sd, $m)"""
    })
  }

  override def prettyName: String = "add_months"
}

/**
 * Returns number of months between dates date1 and date2.
 */
// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = "_FUNC_(timestamp1, timestamp2) - Returns number of months between `timestamp1` and `timestamp2`.",
  extended = """
    Examples:
      > SELECT _FUNC_('1997-02-28 10:30:00', '1996-10-30');
       3.94959677
  """)
// scalastyle:on line.size.limit
case class MonthsBetween(date1: Expression, date2: Expression)
  extends BinaryExpression with ImplicitCastInputTypes {

  override def left: Expression = date1
  override def right: Expression = date2

  override def inputTypes: Seq[AbstractDataType] = Seq(TimestampType, TimestampType)

  override def dataType: DataType = DoubleType

  override def nullSafeEval(t1: Any, t2: Any): Any = {
    DateTimeUtils.monthsBetween(t1.asInstanceOf[Long], t2.asInstanceOf[Long])
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val dtu = DateTimeUtils.getClass.getName.stripSuffix("$")
    defineCodeGen(ctx, ev, (l, r) => {
      s"""$dtu.monthsBetween($l, $r)"""
    })
  }

  override def prettyName: String = "months_between"
}

/**
 * Given a timestamp, which corresponds to a certain time of day in the given timezone, returns
 * another timestamp that corresponds to the same time of day in UTC.
 */
// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = "_FUNC_(timestamp, timezone) - Given a timestamp, which corresponds to a certain time of day in the given timezone, returns another timestamp that corresponds to the same time of day in UTC.",
  extended = """
    Examples:
      > SELECT _FUNC_('2016-08-31', 'Asia/Seoul');
       2016-08-30 15:00:00
  """)
// scalastyle:on line.size.limit
case class ToUTCTimestamp(left: Expression, right: Expression)
  extends BinaryExpression with ImplicitCastInputTypes {

  override def inputTypes: Seq[AbstractDataType] = Seq(TimestampType, StringType)
  override def dataType: DataType = TimestampType
  override def prettyName: String = "to_utc_timestamp"

  override def nullSafeEval(time: Any, timezone: Any): Any = {
    DateTimeUtils.toUTCTime(time.asInstanceOf[Long],
      timezone.asInstanceOf[UTF8String].toString)
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val dtu = DateTimeUtils.getClass.getName.stripSuffix("$")
    if (right.foldable) {
      val tz = right.eval()
      if (tz == null) {
        ev.copy(code = s"""
           |boolean ${ev.isNull} = true;
           |long ${ev.value} = 0;
         """.stripMargin)
      } else {
        val tzTerm = ctx.freshName("tz")
        val utcTerm = ctx.freshName("utc")
        val tzClass = classOf[TimeZone].getName
        ctx.addMutableState(tzClass, tzTerm, s"""$tzTerm = $tzClass.getTimeZone("$tz");""")
        ctx.addMutableState(tzClass, utcTerm, s"""$utcTerm = $tzClass.getTimeZone("UTC");""")
        val eval = left.genCode(ctx)
        ev.copy(code = s"""
           |${eval.code}
           |boolean ${ev.isNull} = ${eval.isNull};
           |long ${ev.value} = 0;
           |if (!${ev.isNull}) {
           |  ${ev.value} = $dtu.convertTz(${eval.value}, $tzTerm, $utcTerm);
           |}
         """.stripMargin)
      }
    } else {
      defineCodeGen(ctx, ev, (timestamp, format) => {
        s"""$dtu.toUTCTime($timestamp, $format.toString())"""
      })
    }
  }
}

/**
 * Returns the date part of a timestamp or string.
 */
@ExpressionDescription(
  usage = "_FUNC_(expr) - Extracts the date part of the date or timestamp expression `expr`.",
  extended = """
    Examples:
      > SELECT _FUNC_('2009-07-30 04:17:52');
       2009-07-30
  """)
case class ToDate(child: Expression) extends UnaryExpression with ImplicitCastInputTypes {

  // Implicit casting of spark will accept string in both date and timestamp format, as
  // well as TimestampType.
  override def inputTypes: Seq[AbstractDataType] = Seq(DateType)

  override def dataType: DataType = DateType

  override def eval(input: InternalRow): Any = child.eval(input)

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    defineCodeGen(ctx, ev, d => d)
  }

  override def prettyName: String = "to_date"
}

@ExpressionDescription(
  usage = "_FUNC_(expr, fmt) - Returns `expr` specified by the format model `fmt`.",
  extended = """
    Examples:
      > SELECT _FUNC_('1234.56789', 3);
       1234.567
  """)
case class Trunc(expr: Expression, format: Expression)
  extends BinaryExpression with ImplicitCastInputTypes {

  override def left: Expression = expr

  override def right: Expression = format

  override def dataType: DataType = StringType

  override def inputTypes: Seq[AbstractDataType] = Seq(StringType, IntegerType)

  override def prettyName: String = "truncNum"

  override def nullable: Boolean = true

  private lazy val fmt = format.eval().asInstanceOf[Int]


  /**
    * Default behavior of evaluation according to the default nullability of BinaryExpression.
    * If subclass of BinaryExpression override nullable, probably should also override this.
    */
  override def eval(input: InternalRow): Any = {
    val v = left.eval(input)
    if (null == v) null else {
      val bigDecimal = BigDecimal(v.toString).setScale(fmt, BigDecimal.RoundingMode.DOWN)
      if (fmt > 0) {
        UTF8String.fromString(bigDecimal.doubleValue().toString)
      } else {
        UTF8String.fromString(bigDecimal.longValue().toString)
      }
    }
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
    nullSafeCodeGen(ctx, ev, (leftVal, fmt) => {

      val bigDecimal = ctx.freshName("bigDecimal")
      s"""
        |if (null == $leftVal) {
        | ${ev.value} = null;
        |} else {
        | java.math.BigDecimal $bigDecimal = new java.math.BigDecimal($leftVal.toString()).setScale($fmt, java.math.RoundingMode.DOWN);
        | if ($fmt > 0) {
        |   ${ev.value} = UTF8String.fromString(String.valueOf($bigDecimal.doubleValue()));
        | } else {
        |   ${ev.value} = UTF8String.fromString(String.valueOf($bigDecimal.longValue()));
        | }
        |}
      """.stripMargin
    })
  }
}



/**
 * Returns date truncated to the unit specified by the format.
 */
// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = "_FUNC_(date, fmt) - Returns `date` with the time portion of the day truncated to the unit specified by the format model `fmt`.",
  extended = """
    Examples:
      > SELECT _FUNC_('2009-02-12', 'MM');
       2009-02-01
      > SELECT _FUNC_('2015-10-27', 'YEAR');
       2015-01-01
  """)
// scalastyle:on line.size.limit
case class TruncDate(date: Expression, format: Expression)
  extends BinaryExpression with ImplicitCastInputTypes {
  override def left: Expression = date
  override def right: Expression = format

  override def inputTypes: Seq[AbstractDataType] = Seq(DateType, StringType)
  override def dataType: DataType = DateType
  override def nullable: Boolean = true
  override def prettyName: String = "trunc"

  private lazy val truncLevel: Int =
    DateTimeUtils.parseTruncLevel(format.eval().asInstanceOf[UTF8String])

  override def eval(input: InternalRow): Any = {
    val level = if (format.foldable) {
      truncLevel
    } else {
      DateTimeUtils.parseTruncLevel(format.eval().asInstanceOf[UTF8String])
    }
    if (level == -1) {
      // unknown format
      null
    } else {
      val d = date.eval(input)
      if (d == null) {
        null
      } else {
        DateTimeUtils.truncDate(d.asInstanceOf[Int], level)
      }
    }
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val dtu = DateTimeUtils.getClass.getName.stripSuffix("$")

    if (format.foldable) {
      if (truncLevel == -1) {
        ev.copy(code = s"""
          boolean ${ev.isNull} = true;
          ${ctx.javaType(dataType)} ${ev.value} = ${ctx.defaultValue(dataType)};""")
      } else {
        val d = date.genCode(ctx)
        ev.copy(code = s"""
          ${d.code}
          boolean ${ev.isNull} = ${d.isNull};
          ${ctx.javaType(dataType)} ${ev.value} = ${ctx.defaultValue(dataType)};
          if (!${ev.isNull}) {
            ${ev.value} = $dtu.truncDate(${d.value}, $truncLevel);
          }""")
      }
    } else {
      nullSafeCodeGen(ctx, ev, (dateVal, fmt) => {
        val form = ctx.freshName("form")
        s"""
          int $form = $dtu.parseTruncLevel($fmt);
          if ($form == -1) {
            ${ev.isNull} = true;
          } else {
            ${ev.value} = $dtu.truncDate($dateVal, $form);
          }
        """
      })
    }
  }
}

/**
 * Returns the number of days from startDate to endDate.
 */
@ExpressionDescription(
  usage = "_FUNC_(endDate, startDate) - Returns the number of days from `startDate` to `endDate`.",
  extended = """
    Examples:
      > SELECT _FUNC_('2009-07-31', '2009-07-30');
       1

      > SELECT _FUNC_('2009-07-30', '2009-07-31');
       -1
  """)
case class DateDiff(endDate: Expression, startDate: Expression)
  extends BinaryExpression with ImplicitCastInputTypes {

  override def left: Expression = endDate
  override def right: Expression = startDate
  override def inputTypes: Seq[AbstractDataType] = Seq(DateType, DateType)
  override def dataType: DataType = IntegerType

  override def nullSafeEval(end: Any, start: Any): Any = {
    end.asInstanceOf[Int] - start.asInstanceOf[Int]
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    defineCodeGen(ctx, ev, (end, start) => s"$end - $start")
  }
}
