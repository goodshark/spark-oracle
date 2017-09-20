
package org.apache.spark.sql.catalyst.expressions

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.{CharacterFunctionUtils, DateFormatTrans}
import org.apache.spark.unsafe.types.{CalendarInterval, UTF8String}


/**
  * Divide the range from min to max into num number of bucket, and
  * return the bucket number the field belongs to.Only support Date and Timestamp.
  */
@ExpressionDescription(
  usage = "_FUNC_(field,min,max,num) - Divide the range from min to max into num number of" +
    " bucket,and return the bucket number the field belongs to.Only support Date and Timestamp.",
  extended = """
    Examples:
      > select width_bucket(to_timestamp('2017-8-20'),to_timestamp(" +
       "'2017-1-1'), to_timestamp('2017-12-31'), 12);
          8
      > select width_bucket(to_date('2017-11-20'),to_date(" +
       "'2017-1-1'), to_date('2017-12-31'), 12);
         11
  """)
case class WidthBucket2(fieldExpr: Expression,
                       minExpr: Expression,
                       maxExpr: Expression,
                       numExpr: Expression)
  extends Expression with ImplicitCastInputTypes{

  override def children: Seq[Expression] = Seq(fieldExpr, minExpr, maxExpr, numExpr)
  override def inputTypes: Seq[AbstractDataType] =
    Seq(TypeCollection(DateType, TimestampType), TypeCollection(DateType, TimestampType),
      TypeCollection(DateType, TimestampType), IntegerType)
  override def dataType: DataType = IntegerType
  override def nullable: Boolean = children.exists(_.nullable)

  override def checkInputDataTypes(): TypeCheckResult = {

    (fieldExpr.dataType, minExpr.dataType, maxExpr.dataType, numExpr.dataType) match {
      case (DateType, DateType, DateType, IntegerType)|
           (TimestampType, TimestampType, TimestampType, IntegerType) =>
        return TypeCheckResult.TypeCheckSuccess
      case _ => return TypeCheckResult.TypeCheckFailure(s"type of the input is not valid")
    }
  }

  def eval(input: InternalRow): Any = {

    val num = numExpr.eval(input).asInstanceOf[Int]
    val fieldValue = fieldExpr.eval(input).asInstanceOf[Long]
    val minValue = minExpr.eval(input).asInstanceOf[Long]
    val maxValue = maxExpr.eval(input).asInstanceOf[Long]
    val interval = (maxValue - minValue) / num.toDouble
    // require(field_value>= min_value && field_value<= max_value, "field must between min and max")

    for(i <- 1 to num) {
      if (fieldValue>= (minValue + (i - 1) * interval)
        && (fieldValue< maxValue + i * interval)) {
        return i
      }
    }

    if(fieldValue == maxValue) {
      return num + 1
    }
  }

  override protected def doGenCode(ctx: CodegenContext,
                                   ev: ExprCode): ExprCode = {

    val eval1 = fieldExpr.genCode(ctx)
    val eval2 = minExpr.genCode(ctx)
    val eval3 = maxExpr.genCode(ctx)
    val eval4 = numExpr.genCode(ctx)
    val numDouble = eval4.value.toDouble

    val field = ctx.freshName("valueField")
    val min = ctx.freshName("valueMin")
    val max = ctx.freshName("valueMax")
    val interval = ctx.freshName("valueInterval")

    val initial =
      s""" long ${field} = ${eval1.value};
        long ${min} = ${eval2.value};
        long ${max} = ${eval3.value};
        double ${interval} = (${max} - ${min}) / ${numDouble};
        ${ctx.javaType(IntegerType)} ${ev.value} = ${ctx.defaultValue(IntegerType)};
        boolean ${ev.isNull} = ${eval1.isNull};
        """

    val endpoint =
      s"""if(${field}== ${max}) {
            ${ev.value}= (${eval4.value}+ 1);
          }"""

    val other =
      s"""for(int i = 0; i< ${eval4.value}; i++) {
              if(${field}>= (${min}+ (i- 1)* ${interval}) && ${field}< (${min}+ i* ${interval})){
                  ${ev.value} = i;
                  break;
              }
          }"""

    ev.copy(code = eval1.code + eval2.code + eval3.code + eval4.code + initial + endpoint + other)
  }

}

/**
  * Convert input string that constructed by hexadecimal into string that every characters
  * are determined by corresponding ascii of there hexadecimal.
  */
@ExpressionDescription(
  usage = "_FUNC_(input) - Convert input string that constructed by hexadecimal into string " +
    "that every characters are determined by corresponding ascii of there hexadecimal.",
  extended = """
    Examples:
      > select hextoraw('7D');
         }
      > select hextoraw('4041424344');
         @ABCD
  """)
case class HexToRaw(inputString: Expression)
  extends Expression with ImplicitCastInputTypes{

  override def children: Seq[Expression] = Seq(inputString)
  override def inputTypes: Seq[AbstractDataType] = Seq(StringType)
  override def dataType: DataType = StringType
  override def nullable: Boolean = children.exists(_.nullable)

  override def checkInputDataTypes(): TypeCheckResult = {
    if (inputString.dataType != StringType) {
      TypeCheckResult.TypeCheckFailure(s"type of the input is not valid")
    } else {
      TypeCheckResult.TypeCheckSuccess
    }
  }

  def eval(input: InternalRow): Any = {

    var hex = inputString.eval(input).asInstanceOf[UTF8String].toString

    if(hex.length%2 == 1) {
      hex = "0" + hex
    }

    val temp = new StringBuilder()
    for (i <- 0 until hex.length / 2) {
      temp.append(Integer.valueOf(hex.substring(i * 2, i * 2 + 2), 16).toChar)
    }

    return temp.toString()
  }

  override protected def doGenCode(ctx: CodegenContext,
                                   ev: ExprCode): ExprCode = {

    val eval1 = inputString.genCode(ctx)

    val str = ctx.freshName("resultStr")
    val input = ctx.freshName("inputStr")
    val a = ctx.freshName("tempA")

    val other =
      s"""
         StringBuilder ${str} = new StringBuilder();
         String ${input} = ${eval1.value}.toString();
         if(${input}.length()%2 == 1) {
             ${input} = "0" + ${input};
         }
         int ${a}=0;
         for(int i = 0; i< ${input}.length()/2; i++) {
            ${a}= Integer.valueOf(${input}.substring(i*2,i*2 + 2),16);
            ${str}.append((char)${a});
         }
       """

    ev.copy(code = eval1.code +
      s"""boolean ${ev.isNull} = ${eval1.isNull};
         ${ctx.javaType(StringType)} ${ev.value} = ${ctx.defaultValue(StringType)};
         ${other};
         ${ev.value} = UTF8String.fromString(${str}.toString());""")
  }

}

/**
  * Convert the input of string or datetime or number into string, when the input is
  * datetime(date or timestamp),the string format can be specified, if not, defualt
  * is used.
  */
@ExpressionDescription(
  usage = "_FUNC_(input,format) - Convert the input of string or datetime or number into string," +
    "when the input is datetime(date or timestamp),the string format can be specified,if not, defualt is used.",
  extended = """
    Examples:
      > select to_char('xiayongzhao')
          xiayongzhao
      > select to_char(123);
          123
      > select to_char(123d);
          123.0
      > select to_char(123L);
          123
      > select to_char(123s);
          123
      > select to_char(to_date('2017-08-21'), 'dd.mm.yyyy')
          21.08.2017
      > select to_char(to_timestamp('2017-08-21'), 'dd.mm.yyyy hh/mi/ss.ff');
          21.08.2017 12/00/00.000000000
      > select to_char(to_timestamp('2017-08-21'));
          2017-08-21 00:00:00.000000000
  """)
case class ToChar(inputExpr: Expression, formatExpr: Expression)
  extends Expression with ImplicitCastInputTypes{

  def this(input: Expression) {
    this(input, Literal(null))
  }

  override def children: Seq[Expression] = Seq(inputExpr, formatExpr)
  override def inputTypes: Seq[AbstractDataType] =
    Seq(TypeCollection(NumericType, ShortType, IntegerType, LongType,
      FloatType, DoubleType, DecimalType, TimestampType, DateType,
      StringType), TypeCollection(NullType, StringType))
  override def dataType: DataType = StringType
  override def nullable: Boolean = true

  override def checkInputDataTypes(): TypeCheckResult = {
    inputExpr.dataType match {
      case StringType | DecimalType() | DoubleType | FloatType |
           LongType | IntegerType | ShortType => return TypeCheckResult.TypeCheckSuccess
      case DateType | TimestampType => formatExpr.dataType match {
        case NullType | StringType => return TypeCheckResult.TypeCheckSuccess
        case _ => return TypeCheckResult.TypeCheckFailure(s"type of the input is not valid")
      }
      case _ => return TypeCheckResult.TypeCheckFailure(s"type of the input is not valid")
    }
  }

  private lazy val constFormat = formatExpr.dataType match {
    case StringType => formatExpr.eval().asInstanceOf[UTF8String].toString;
    case NullType => inputExpr.dataType match {
      case DateType => "yyyy-MM-dd"
      case TimestampType => "yyyy-MM-dd HH24:mi:ss.ff"
    }
  }

  def eval(input: InternalRow): Any = {

    val result = inputExpr.eval(input)
    inputExpr.dataType match {
      case DateType =>
        return UTF8String.fromString(DateFormatTrans.sparkDateToSpecifiedDate(
          DateTimeUtils.dateToString(result.asInstanceOf[Int]), constFormat));
      case TimestampType =>
        return UTF8String.fromString(DateFormatTrans.sparkTimestampToSpecifiedDate(
          DateTimeUtils.timestampToString(result.asInstanceOf[Long]), constFormat));
      case _ => return UTF8String.fromString(result.toString);
    }
  }

  override protected def doGenCode(ctx: CodegenContext,
                                   ev: ExprCode): ExprCode = {

    val eval1 = inputExpr.genCode(ctx)
    val dtu = DateTimeUtils.getClass.getName.stripSuffix("$")

    val result = ctx.freshName("resultStr")

    val other = inputExpr.dataType match {
      case DateType =>
        s"""${result}= org.apache.spark.sql.util.DateFormatTrans.sparkDateToSpecifiedDate(
         $dtu.dateToString(${eval1.value}), "${constFormat}");"""
      case TimestampType =>
        s"""${result}= org.apache.spark.sql.util.DateFormatTrans.sparkTimestampToSpecifiedDate(
        $dtu.timestampToString(${eval1.value}), "${constFormat}");"""
      case _ => s"""${result}= UTF8String.fromString(${eval1.value}+ "").toString() ;"""
    }

    ev.copy(code = eval1.code +
      s"""boolean ${ev.isNull} = ${eval1.isNull};
         ${ctx.javaType(StringType)} ${ev.value} = ${ctx.defaultValue(StringType)};
         String ${result}= "";
         ${other}
         ${ev.value} = UTF8String.fromString(${result});""")
  }
}

/**
  *  Convert input string that representing the date to DateType depending on the specified format
  */
@ExpressionDescription(
  usage = "_FUNC_(input,format) - Convert input string that representing the date into DateType" +
    "depending on the specified format",
  extended = """
    Examples:
      > select to_date2('2017-/08::21', 'yyyy-/mm::dd');
         2017-08-21
  """)
case class ToDate2(inputExpr: Expression, formatExpr: Expression)
  extends Expression with ImplicitCastInputTypes{

  def this(input: Expression) {
    this(input, Literal("yyyy-MM-dd"))
  }

  override def children: Seq[Expression] = Seq(inputExpr, formatExpr)
  override def inputTypes: Seq[AbstractDataType] = Seq(StringType, StringType)
  override def dataType: DataType = DateType
  override def nullable: Boolean = children.exists(_.nullable)

  override def checkInputDataTypes(): TypeCheckResult = {
    (inputExpr.dataType, formatExpr.dataType) match {
      case (StringType, StringType) => return TypeCheckResult.TypeCheckSuccess
      case _ => return TypeCheckResult.TypeCheckFailure(s"type of the input is not valid")
    }
  }

  def eval(input: InternalRow): Any = {

    val dateString = inputExpr.eval(input).asInstanceOf[UTF8String].toString.toUpperCase
    val formatString = formatExpr.eval(input).asInstanceOf[UTF8String].toString.toUpperCase

    return DateTimeUtils.stringToDate(UTF8String.fromString(
      DateFormatTrans.specifiedDateToSparkDate(dateString, formatString)))

  }

  override protected def doGenCode(ctx: CodegenContext,
                                   ev: ExprCode): ExprCode = {

    val eval1 = inputExpr.genCode(ctx)
    val eval2 = formatExpr.genCode(ctx)

    val dtu = DateTimeUtils.getClass.getName.stripSuffix("$")

    ev.copy(code = eval1.code + eval2.code +
      s"""boolean ${ev.isNull} = ${eval1.isNull};
         ${ctx.javaType(DateType)} ${ev.value} = ${ctx.defaultValue(DateType)};
         if(${dtu}.stringToDate(UTF8String.fromString(
         org.apache.spark.sql.util.DateFormatTrans.specifiedDateToSparkDate(
                        ${eval1.value}.toString(), ${eval2.value}.toString()))).get() != null) {
            ${ev.value} = Integer.parseInt(${dtu}.stringToDate(UTF8String.fromString(
            org.apache.spark.sql.util.DateFormatTrans.specifiedDateToSparkDate(
                        ${eval1.value}.toString(), ${eval2.value}.toString()))).get().toString());
         }
         """)
  }
}

/**
  * Convert input string that representing the timestamp to TimestampType
  * depending on the specified format
  */
@ExpressionDescription(
  usage = "_FUNC_(input,format) - Convert input string that representing the timestamp to" +
    " TimestampType depending on the specified format",
  extended = """
    Examples:
      > select to_timestamp('2017-/08::21 10:12:13', 'yyyy-/mm::dd mi:hh:ss');
         2017-08-21 00:10:13.0
  """)
case class ToTimestamp(inputExpr: Expression, formatExpr: Expression)
  extends Expression with ImplicitCastInputTypes{

  def this(input: Expression) {
    this(input, Literal("yyyy-MM-dd hh:mi:ss.FF"))
  }

  override def children: Seq[Expression] = Seq(inputExpr, formatExpr)
  override def inputTypes: Seq[AbstractDataType] = Seq(StringType, StringType)
  override def dataType: DataType = TimestampType
  override def nullable: Boolean = children.exists(_.nullable)

  override def checkInputDataTypes(): TypeCheckResult = {
    (inputExpr.dataType, formatExpr.dataType) match {
      case (StringType, StringType) => return TypeCheckResult.TypeCheckSuccess
      case _ => return TypeCheckResult.TypeCheckFailure(s"type of the input is not valid")
    }
  }

  def eval(input: InternalRow): Any = {

    val dateString = inputExpr.eval(input).asInstanceOf[UTF8String].toString.toUpperCase
    val formatString = formatExpr.eval(input).asInstanceOf[UTF8String].toString.toUpperCase

    return DateTimeUtils.stringToTimestamp(UTF8String.fromString(
      DateFormatTrans.specifiedDateToSparkTimestamp(dateString, formatString)))

  }

  override protected def doGenCode(ctx: CodegenContext,
                                   ev: ExprCode): ExprCode = {

    val eval1 = inputExpr.genCode(ctx)
    val eval2 = formatExpr.genCode(ctx)

    val dtu = DateTimeUtils.getClass.getName.stripSuffix("$")

    ev.copy(code = eval1.code + eval2.code +
      s"""boolean ${ev.isNull} = ${eval1.isNull};
         ${ctx.javaType(TimestampType)} ${ev.value} = ${ctx.defaultValue(TimestampType)};
         if(${dtu}.stringToTimestamp(UTF8String.fromString(
         org.apache.spark.sql.util.DateFormatTrans.specifiedDateToSparkTimestamp(
                        ${eval1.value}.toString(), ${eval2.value}.toString()))).get() != null) {
            ${ev.value} = Long.parseLong(${dtu}.stringToTimestamp(
            UTF8String.fromString(org.apache.spark.sql.util.DateFormatTrans.specifiedDateToSparkTimestamp(
                        ${eval1.value}.toString(), ${eval2.value}.toString()))).get().toString());
         }
         """)
  }
}

/**
  * create a time interval depending on the num and unit, unit can be "day" or "hour"
  * or "minute" or "second".
  */
@ExpressionDescription(
  usage = "_FUNC_(num, unit) - create a time interval depending on the num and unit, unit can " +
    "be 'day' or 'hour' or 'minute' or 'second'",
  extended = """
    Examples:
      > select current_date+numtodsinterval(8,'day');
         2017-08-29
  """)
case class NumToDSInterval(numberExpr: Expression, unitExpr: Expression)
  extends Expression with ImplicitCastInputTypes{

  override def children: Seq[Expression] = Seq(numberExpr, unitExpr)
  override def inputTypes: Seq[AbstractDataType] = Seq(IntegerType, StringType)
  override def dataType: DataType = CalendarIntervalType
  override def nullable: Boolean = children.exists(_.nullable)

  override def checkInputDataTypes(): TypeCheckResult = {

    (numberExpr.dataType, unitExpr.dataType) match {
      case (IntegerType, StringType) => return TypeCheckResult.TypeCheckSuccess
      case _ => return TypeCheckResult.TypeCheckFailure(s"type of the input is not valid")
    }
  }

  def eval(input: InternalRow): Any = {

    val number = numberExpr.eval(input).asInstanceOf[Int]
    val unit = unitExpr.eval(input).asInstanceOf[UTF8String].toString.toLowerCase

    if(unit.equals("day")|unit.equals("hour")|unit.equals("minute")|unit.equals("second")) {
      return CalendarInterval.fromSingleUnitString(unit, number.toString())
    } else {
      return null;
    }
  }

  override protected def doGenCode(ctx: CodegenContext,
                                   ev: ExprCode): ExprCode = {

    val eval1 = numberExpr.genCode(ctx)
    val eval2 = unitExpr.genCode(ctx)

    val unit = ctx.freshName("timeUnit")

    ev.copy(code = eval1.code + eval2.code +
      s"""boolean ${ev.isNull} = ${eval1.isNull};
         ${ctx.javaType(CalendarIntervalType)} ${ev.value} =
         ${ctx.defaultValue(CalendarIntervalType)};
         String ${unit} = ${eval2.value}.toString();
         if(${unit}.equals("day")|${unit}.equals("hour")|${unit}.equals("minute")|${unit}.equals("second")) {
            ${ev.value} = org.apache.spark.unsafe.types.CalendarInterval.fromSingleUnitString(
         ${unit}, String.valueOf(${eval1.value}));
         }else{
            ${ev.value} = null;
         }
         """)
  }
}

/**
  * create a time interval depending on the num and unit, unit can be "year" or "month"
  */
@ExpressionDescription(
  usage = "_FUNC_(num, unit) - create a time interval depending on the num and unit, unit can " +
    "be 'year' or 'month'",
  extended = """
    Examples:
      > select current_date+numtoyminterval(8,'month');
         2018-04-21
  """)
case class NumToYMInterval(numberExpr: Expression, unitExpr: Expression)
  extends Expression with ImplicitCastInputTypes{

  override def children: Seq[Expression] = Seq(numberExpr, unitExpr)
  override def inputTypes: Seq[AbstractDataType] = Seq(IntegerType, StringType)
  override def dataType: DataType = CalendarIntervalType
  override def nullable: Boolean = children.exists(_.nullable)

  override def checkInputDataTypes(): TypeCheckResult = {

    (numberExpr.dataType, unitExpr.dataType) match {
      case (IntegerType, StringType) => return TypeCheckResult.TypeCheckSuccess
      case _ => return TypeCheckResult.TypeCheckFailure(s"type of the input is not valid")
    }
  }

  def eval(input: InternalRow): Any = {

    val number = numberExpr.eval(input).asInstanceOf[Int]
    val unit = unitExpr.eval(input).asInstanceOf[UTF8String].toString.toLowerCase

    if(unit.equals("year")|unit.equals("month")) {
      return CalendarInterval.fromSingleUnitString(unit, number.toString())
    } else {
      return null;
    }
  }

  override protected def doGenCode(ctx: CodegenContext,
                                   ev: ExprCode): ExprCode = {

    val eval1 = numberExpr.genCode(ctx)
    val eval2 = unitExpr.genCode(ctx)

    val unit = ctx.freshName("timeUnit")

    ev.copy(code = eval1.code + eval2.code +
      s"""boolean ${ev.isNull} = ${eval1.isNull};
         ${ctx.javaType(CalendarIntervalType)} ${ev.value} =
         ${ctx.defaultValue(CalendarIntervalType)};
         String ${unit} = ${eval2.value}.toString().toLowerCase();
         if(${unit}.equals("year")|${unit}.equals("month")) {
            ${ev.value} = org.apache.spark.unsafe.types.CalendarInterval.fromSingleUnitString(
         |${unit}, String.valueOf(${eval1.value}));
         }else{
            ${ev.value} = null;
         }
         """)
  }
}

/**
  * create a time interval depending on the string that representing
  * (day,hour,minute,second) time interval
  */
@ExpressionDescription(
  usage = "_FUNC_(num, unit) - create a time interval depending on the string that representing" +
    " (day,hour,minute,second) time interval",
  extended = """
    Examples:
      > select current_timestamp+to_dsinterval('-1 0:00:12');
         2017-08-20 20:32:29.984
      > select current_timestamp+to_dsinterval('-P10DT05H06M08.88S')
         2017-08-11 15:28:07.693
  """)
case class ToDSInterval(formatExpr: Expression)
  extends Expression with ImplicitCastInputTypes{

  override def children: Seq[Expression] = Seq(formatExpr)
  override def inputTypes: Seq[AbstractDataType] = Seq(StringType)
  override def dataType: DataType = CalendarIntervalType
  override def nullable: Boolean = children.exists(_.nullable)

  override def checkInputDataTypes(): TypeCheckResult = {

    if (formatExpr.dataType != StringType) {
      return TypeCheckResult.TypeCheckFailure(s"type of the input is not valid")
    } else {
      return TypeCheckResult.TypeCheckSuccess
    }
  }

  def eval(input: InternalRow): Any = {

    val formatString = formatExpr.eval(input).asInstanceOf[UTF8String].toString

    return CalendarInterval.fromDayTimeString(
      DateFormatTrans.oracleDSIntervalToSparkInterval(formatString))
  }

  override protected def doGenCode(ctx: CodegenContext,
                                   ev: ExprCode): ExprCode = {

    val eval1 = formatExpr.genCode(ctx)

    ev.copy(code = eval1.code +
      s"""boolean ${ev.isNull} = ${eval1.isNull};
         ${ctx.javaType(CalendarIntervalType)} ${ev.value} =
         ${ctx.defaultValue(CalendarIntervalType)};
         ${ev.value} = CalendarInterval.fromDayTimeString(
         org.apache.spark.sql.util.DateFormatTrans.oracleDSIntervalToSparkInterval(
         ${eval1.value}.toString()));
         """)
  }
}

/**
  * create a time interval depending on the string that representing
  * (year,month) time interval
  */
@ExpressionDescription(
  usage = "_FUNC_(num, unit) - create a time interval depending on the string that representing" +
    " (year,month) time interval",
  extended = """
    Examples:
      > select current_timestamp+to_yminterval('01-02');
         2018-10-21 20:39:05.927
      > select current_timestamp+to_yminterval('-P1Y2M10DT05H06M08.88S')
         2016-06-21 20:40:09.238
  """)
case class ToYMInterval(formatExpr: Expression)
  extends Expression with ImplicitCastInputTypes{

  override def children: Seq[Expression] = Seq(formatExpr)
  override def inputTypes: Seq[AbstractDataType] = Seq(StringType)
  override def dataType: DataType = CalendarIntervalType
  override def nullable: Boolean = children.exists(_.nullable)

  override def checkInputDataTypes(): TypeCheckResult = {

    if (formatExpr.dataType != StringType) {
      return TypeCheckResult.TypeCheckFailure(s"type of the input is not valid")
    } else {
      return TypeCheckResult.TypeCheckSuccess
    }
  }

  def eval(input: InternalRow): Any = {

    val formatString = formatExpr.eval(input).asInstanceOf[UTF8String].toString

    return CalendarInterval.fromYearMonthString(
      DateFormatTrans.oracleYMIntervalToSparkInterval(formatString))
  }

  override protected def doGenCode(ctx: CodegenContext,
                                   ev: ExprCode): ExprCode = {

    val eval1 = formatExpr.genCode(ctx)

    ev.copy(code = eval1.code +
      s"""boolean ${ev.isNull} = ${eval1.isNull};
         ${ctx.javaType(CalendarIntervalType)} ${ev.value} =
         ${ctx.defaultValue(CalendarIntervalType)};
         ${ev.value} = CalendarInterval.fromYearMonthString(
         org.apache.spark.sql.util.DateFormatTrans.oracleYMIntervalToSparkInterval(
         ${eval1.value}.toString()));
         """)
  }
}


/**
  *  create a time interval depending on the string that representing
  * (year,month) time interval
  */
@ExpressionDescription(
  usage = "_FUNC_(string, substring, position, occurrence) - search the occurrence number of " +
    "the substring start from the position of string, return the start position of result",
  extended = """
    Examples:
      > select instr2('CORPORATE FLOOR','OR', 3, 2) result;
         14
      > select instr2('CORPORATE FLOOR','OR', -3, 2) result;
         2
      > select instr2('CORPORATE FLOOR','OR', 3) result;
         5
      > select instr2('CORPORATE FLOOR','OR') result;
         2
  """)
case class Instr2(stringExpr: Expression, substringExpr: Expression,
                  positionExpr: Expression, occurExpr: Expression)
  extends Expression with ImplicitCastInputTypes{

  def this(string: Expression, substring: Expression) {
    this(string, substring, Literal(1), Literal(1))
  }

  def this(string: Expression, substring: Expression, position: Expression) {
    this(string, substring, position, Literal(1))
  }

  override def children: Seq[Expression] =
    Seq(stringExpr, substringExpr, positionExpr, occurExpr)
  override def inputTypes: Seq[AbstractDataType] =
    Seq(StringType, StringType, IntegerType, IntegerType)
  override def dataType: DataType = IntegerType
  override def nullable: Boolean = children.exists(_.nullable)

  override def checkInputDataTypes(): TypeCheckResult = {

    if (stringExpr.dataType == StringType && substringExpr.dataType == StringType
      && positionExpr.dataType == IntegerType && occurExpr.dataType == IntegerType) {
      return TypeCheckResult.TypeCheckSuccess
    }
    return TypeCheckResult.TypeCheckFailure(s"type of the input is not valid")
  }

  def eval(input: InternalRow): Any = {

    val string = stringExpr.eval(input).asInstanceOf[UTF8String].toString
    val substring = substringExpr.eval(input).asInstanceOf[UTF8String].toString
    val position = positionExpr.eval(input).asInstanceOf[Int]
    val occurrence = occurExpr.eval(input).asInstanceOf[Int]

    return CharacterFunctionUtils.stringInstr(string, substring, position, occurrence)
  }

  override protected def doGenCode(ctx: CodegenContext,
                                   ev: ExprCode): ExprCode = {

    val eval1 = stringExpr.genCode(ctx)
    val eval2 = substringExpr.genCode(ctx)
    val eval3 = positionExpr.genCode(ctx)
    val eval4 = occurExpr.genCode(ctx)

    ev.copy(code = eval1.code + eval2.code + eval3.code + eval4.code +
      s"""boolean ${ev.isNull} = ${eval1.isNull};
         ${ctx.javaType(IntegerType)} ${ev.value} = ${ctx.defaultValue(IntegerType)};
         ${ev.value} = org.apache.spark.sql.util.CharacterFunctionUtils.stringInstr(
            ${eval1.value}.toString(),${eval2.value}.toString(), ${eval3.value}, ${eval4.value});
         """)
  }
}

/**
  *search the count of the occurrence that the pattern matched start from the position of string,
  * and matchPara can indicate how to match.
  */
@ExpressionDescription(
  usage = "_FUNC_(string, pattern, position, matchPara) - search the count of the occurrence that" +
    "the regex matched start from the position of string,and matchPara can indicate how to match.",
  extended = """
    Examples:
      > select regexp_count('123123123123', '123', 3, 'i') result;
         3
      > select regexp_count('xia.XIA.xia.XIA x i a x i a', 'xia\\.', 1, 'ix') result;
         3
  """)
case class RegExpCount(stringExpr: Expression, patternExpr: Expression,
                       positionExpr: Expression, matchParaExpr: Expression)
  extends Expression with ImplicitCastInputTypes{

  def this(string: Expression, regex: Expression) {
    this(string, regex, Literal(1), Literal("c"))
  }

  def this(string: Expression, regex: Expression, position: Expression) {
    this(string, regex, position, Literal("c"))
  }

  override def children: Seq[Expression] =
    Seq(stringExpr, patternExpr, positionExpr, matchParaExpr)
  override def inputTypes: Seq[AbstractDataType] =
    Seq(StringType, StringType, IntegerType, StringType)
  override def dataType: DataType = IntegerType
  override def nullable: Boolean = children.exists(_.nullable)

  override def checkInputDataTypes(): TypeCheckResult = {

    if (stringExpr.dataType == StringType && patternExpr.dataType == StringType
      && positionExpr.dataType == IntegerType && matchParaExpr.dataType == StringType) {
      return TypeCheckResult.TypeCheckSuccess
    }
    return TypeCheckResult.TypeCheckFailure(s"type of the input is not valid")
  }

  def eval(input: InternalRow): Any = {

    val string = stringExpr.eval(input).asInstanceOf[UTF8String].toString
    val regex = patternExpr.eval(input).asInstanceOf[UTF8String].toString
    val position = positionExpr.eval(input).asInstanceOf[Int]
    val matchPara = matchParaExpr.eval(input).asInstanceOf[UTF8String].toString

    return CharacterFunctionUtils.regExpCount(string, regex, position, matchPara)
  }

  override protected def doGenCode(ctx: CodegenContext,
                                   ev: ExprCode): ExprCode = {

    val eval1 = stringExpr.genCode(ctx)
    val eval2 = patternExpr.genCode(ctx)
    val eval3 = positionExpr.genCode(ctx)
    val eval4 = matchParaExpr.genCode(ctx)

    ev.copy(code = eval1.code + eval2.code + eval3.code + eval4.code +
      s"""boolean ${ev.isNull} = ${eval1.isNull};
         ${ctx.javaType(IntegerType)} ${ev.value} = ${ctx.defaultValue(IntegerType)};
         ${ev.value} = org.apache.spark.sql.util.CharacterFunctionUtils.regExpCount(
            ${eval1.value}.toString(),${eval2.value}.toString(),
            ${eval3.value}, ${eval4.value}.toString());
         """)
  }
}

/**
  * search the position of the occurrence that the regex matched start from the position of string,
  * and matchPara can indicate how to match, while returnOpt indicate how to return.
  */
@ExpressionDescription(
  usage = "_FUNC_(string, pattern, position, occurrence, returnOpt, matchPara, subExpr) - search" +
    "the position of the occurrence that the regex matched start from the position of string,and matchPara can indicate how to match, while returnOpt indicate how to return",
  extended = """
    Examples:
      > select REGEXP_INSTR('500 Oracle Parkway, Redwood Shores, CA','[^ ]+', 1, 6) result;
         37
      > select REGEXP_INSTR('500 Oracle Parkway, Redwood Shores,CA','[s|r|p][a-z]{6}', 3, 2, 1, 'ix') result;
         28
  """)
case class RegExpInstr(stringExpr: Expression, patternExpr: Expression,
                       positionExpr: Expression, occurExpr: Expression,
                       returnOptExpr: Expression, matchParaExpr: Expression,
                       subExprExpr: Expression)
  extends Expression with ImplicitCastInputTypes{

  def this(string: Expression, regex: Expression) {
    this(string, regex, Literal(1), Literal(1), Literal(0), Literal("c"), Literal(0))
  }

  def this(string: Expression, regex: Expression, position: Expression) {
    this(string, regex, position, Literal(1), Literal(0), Literal("c"), Literal(0))
  }

  def this(string: Expression, regex: Expression, position: Expression, occurrence: Expression) {
    this(string, regex, position, occurrence, Literal(0), Literal("c"), Literal(0))
  }

  def this(string: Expression, regex: Expression, position: Expression, occurrence: Expression,
           returnOpt: Expression) {
    this(string, regex, position, occurrence, returnOpt, Literal("c"), Literal(0))
  }

  def this(string: Expression, regex: Expression, position: Expression, occurrence: Expression,
           returnOpt: Expression, matchPara: Expression) {
    this(string, regex, position, occurrence, returnOpt, matchPara, Literal(0))
  }

  override def children: Seq[Expression] = Seq(stringExpr, patternExpr,
    positionExpr, occurExpr, returnOptExpr, matchParaExpr, subExprExpr)
  override def inputTypes: Seq[AbstractDataType] = Seq(StringType, StringType,
    IntegerType, IntegerType, IntegerType, StringType, IntegerType)
  override def dataType: DataType = IntegerType
  override def nullable: Boolean = children.exists(_.nullable)

  override def checkInputDataTypes(): TypeCheckResult = {

    if (stringExpr.dataType == StringType && patternExpr.dataType == StringType
      && positionExpr.dataType == IntegerType && matchParaExpr.dataType == StringType
      && occurExpr.dataType== IntegerType && returnOptExpr.dataType== IntegerType
      && subExprExpr.dataType== IntegerType) {
      return TypeCheckResult.TypeCheckSuccess
    }
    return TypeCheckResult.TypeCheckFailure(s"type of the input is not valid")
  }

  def eval(input: InternalRow): Any = {

    val string = stringExpr.eval(input).asInstanceOf[UTF8String].toString
    val regex = patternExpr.eval(input).asInstanceOf[UTF8String].toString
    val position = positionExpr.eval(input).asInstanceOf[Int]
    val occurrence = occurExpr.eval(input).asInstanceOf[Int]
    val returnOpt = returnOptExpr.eval(input).asInstanceOf[Int]
    val matchPara = matchParaExpr.eval(input).asInstanceOf[UTF8String].toString
    val subExpr = subExprExpr.eval(input).asInstanceOf[Int]

    return CharacterFunctionUtils.regExpInstr(string, regex, position,
      occurrence, returnOpt, matchPara, subExpr)
  }

  override protected def doGenCode(ctx: CodegenContext,
                                   ev: ExprCode): ExprCode = {

    val eval1 = stringExpr.genCode(ctx)
    val eval2 = patternExpr.genCode(ctx)
    val eval3 = positionExpr.genCode(ctx)
    val eval4 = occurExpr.genCode(ctx)
    val eval5 = returnOptExpr.genCode(ctx)
    val eval6 = matchParaExpr.genCode(ctx)
    val eval7 = subExprExpr.genCode(ctx)

    ev.copy(code = eval1.code + eval2.code + eval3.code + eval4.code +
      eval5.code + eval6.code + eval7.code +
      s"""boolean ${ev.isNull} = ${eval1.isNull};
         ${ctx.javaType(IntegerType)} ${ev.value} = ${ctx.defaultValue(IntegerType)};
         ${ev.value} = org.apache.spark.sql.util.CharacterFunctionUtils.regExpInstr(
            ${eval1.value}.toString(),${eval2.value}.toString(),${eval3.value}, ${eval4.value},
            ${eval5.value},${eval6.value}.toString(), ${eval7.value});
         """)
  }
}

/**
  * removes from the left end of string all of
  * the characters contained in set
  */
@ExpressionDescription(
  usage = "_FUNC_(string, set) - removes from the left end of string" +
    "             all of the characters contained in set",
  extended = """
    Examples:
      > select ltrim2('<=====>BROWNING<=====>', '<>=') result;
         BROWNING<=====>
  """)
case class StringTrimLeft2(stringExpr: Expression, setExpr: Expression)
  extends Expression with ImplicitCastInputTypes{

  def this(string: Expression) {
    this(string, Literal(" "))
  }

  override def children: Seq[Expression] = Seq(stringExpr, setExpr)
  override def inputTypes: Seq[AbstractDataType] = Seq(StringType, StringType)
  override def dataType: DataType = StringType
  override def nullable: Boolean = children.exists(_.nullable)

  override def checkInputDataTypes(): TypeCheckResult = {

    if (stringExpr.dataType == StringType && setExpr.dataType == StringType) {
      return TypeCheckResult.TypeCheckSuccess
    }
    return TypeCheckResult.TypeCheckFailure(s"type of the input is not valid")
  }

  def eval(input: InternalRow): Any = {

    val string = stringExpr.eval(input).asInstanceOf[UTF8String].toString
    val set = setExpr.eval(input).asInstanceOf[UTF8String].toString

    return CharacterFunctionUtils.stringTrim(string, set, "left")
  }

  override protected def doGenCode(ctx: CodegenContext,
                                   ev: ExprCode): ExprCode = {

    val eval1 = stringExpr.genCode(ctx)
    val eval2 = setExpr.genCode(ctx)

    ev.copy(code = eval1.code + eval2.code +
      s"""boolean ${ev.isNull} = ${eval1.isNull};
         ${ctx.javaType(StringType)} ${ev.value} = ${ctx.defaultValue(StringType)};
         ${ev.value} = UTF8String.fromString(org.apache.spark.sql.util.CharacterFunctionUtils.
         stringTrim(${eval1.value}.toString(),${eval2.value}.toString(),"left"));
         """)
  }
}

/**
  * removes from the right end of string all of
  * the characters contained in set
  */
@ExpressionDescription(
  usage = "_FUNC_(string, set) - removes from the right end of string" +
    "             all of the characters contained in set",
  extended = """
    Examples:
      > select rtrim2('<=====>BROWNING<=====>', '<>=') result;
         <=====>BROWNING
  """)
case class StringTrimRight2(stringExpr: Expression, setExpr: Expression)
  extends Expression with ImplicitCastInputTypes{

  def this(string: Expression) {
    this(string, Literal(" "))
  }

  override def children: Seq[Expression] = Seq(stringExpr, setExpr)
  override def inputTypes: Seq[AbstractDataType] = Seq(StringType, StringType)
  override def dataType: DataType = StringType
  override def nullable: Boolean = children.exists(_.nullable)

  override def checkInputDataTypes(): TypeCheckResult = {

    if (stringExpr.dataType == StringType && setExpr.dataType == StringType) {
      return TypeCheckResult.TypeCheckSuccess
    }
    return TypeCheckResult.TypeCheckFailure(s"type of the input is not valid")
  }

  def eval(input: InternalRow): Any = {

    val string = stringExpr.eval(input).asInstanceOf[UTF8String].toString
    val set = setExpr.eval(input).asInstanceOf[UTF8String].toString

    return CharacterFunctionUtils.stringTrim(string, set, "right")
  }

  override protected def doGenCode(ctx: CodegenContext,
                                   ev: ExprCode): ExprCode = {

    val eval1 = stringExpr.genCode(ctx)
    val eval2 = setExpr.genCode(ctx)

    ev.copy(code = eval1.code + eval2.code +
      s"""boolean ${ev.isNull} = ${eval1.isNull};
         ${ctx.javaType(StringType)} ${ev.value} = ${ctx.defaultValue(StringType)};
         ${ev.value} = UTF8String.fromString(org.apache.spark.sql.util.CharacterFunctionUtils.
         stringTrim(${eval1.value}.toString(),${eval2.value}.toString(),"right"));
         """)
  }
}

/**
  * returns string with every occurrence of searchStr replaced with replaceStr.
  */
@ExpressionDescription(
  usage = "_FUNC_(string, searchStr, replaceStr) - returns string with every" +
    "occurrence of searchStr replaced with replaceStr.",
  extended = """
    Examples:
      > select replace2('JACK and JUE','J','BL') result;
         BLACK and BLUE
      > select replace2('JACK and JUE','J') result;
         ACK and UE
  """)
case class StringReplace(stringExpr: Expression, searchStrExpr: Expression,
                         replaceStrExpr: Expression)
  extends Expression with ImplicitCastInputTypes{

  def this(string: Expression, searchStr: Expression) {
    this(string, searchStr, Literal(""))
  }

  override def children: Seq[Expression] = Seq(stringExpr, searchStrExpr, replaceStrExpr)
  override def inputTypes: Seq[AbstractDataType] = Seq(StringType, StringType, StringType)
  override def dataType: DataType = StringType
  override def nullable: Boolean = children.exists(_.nullable)

  override def checkInputDataTypes(): TypeCheckResult = {

    if (stringExpr.dataType == StringType && searchStrExpr.dataType == StringType
      && replaceStrExpr.dataType == StringType) {
      return TypeCheckResult.TypeCheckSuccess
    }
    return TypeCheckResult.TypeCheckFailure(s"type of the input is not valid")
  }

  def eval(input: InternalRow): Any = {

    val string = stringExpr.eval(input).asInstanceOf[UTF8String].toString
    val searchStr = searchStrExpr.eval(input).asInstanceOf[UTF8String].toString
    val replaceStr = replaceStrExpr.eval(input).asInstanceOf[UTF8String].toString

    if(searchStr == null || searchStr.length == 0) {
      return string
    }
    return string.replace(searchStr, replaceStr)
  }

  override protected def doGenCode(ctx: CodegenContext,
                                   ev: ExprCode): ExprCode = {

    val eval1 = stringExpr.genCode(ctx)
    val eval2 = searchStrExpr.genCode(ctx)
    val eval3 = replaceStrExpr.genCode(ctx)

    ev.copy(code = eval1.code + eval2.code + eval3.code +
      s"""boolean ${ev.isNull} = ${eval1.isNull};
         ${ctx.javaType(StringType)} ${ev.value} = ${ctx.defaultValue(StringType)};
         if(${eval2.value}.toString()== null|${eval2.value}.toString().length()== 0) {
             ${ev.value} = ${eval1.value};
         }else{
             ${ev.value} = UTF8String.fromString(${eval1.value}.toString().replace(
                         ${eval2.value}.toString(), ${eval3.value}.toString()));
         }
         """)
  }
}

/**
  * search the subString of the occurrence that the regex matched start from the position of string,
  * and matchPara can indicate how to match.
  */
@ExpressionDescription(
  usage = "_FUNC_(string, pattern, position, occurrence, matchPara, subExpr) - search the subString" +
    "of the occurrence that the regex matched start from the position of string,and matchPara can indicate how to match.",
  extended = """
    Examples:
      > select REGEXP_SUBSTR2('500 Oracle Parkway, Redwood Shores, CA','[^ ]+', 1, 6) result;
          CA
  """)
case class RegExpSubStr(stringExpr: Expression, patternExpr: Expression,
                        positionExpr: Expression, occurExpr: Expression,
                        matchParaExpr: Expression, subExprExpr: Expression)
  extends Expression with ImplicitCastInputTypes{

  def this(string: Expression, regex: Expression) {
    this(string, regex, Literal(1), Literal(1), Literal("c"), Literal(0))
  }

  def this(string: Expression, regex: Expression, position: Expression) {
    this(string, regex, position, Literal(1), Literal("c"), Literal(0))
  }

  def this(string: Expression, regex: Expression, position: Expression, occurrence: Expression) {
    this(string, regex, position, occurrence, Literal("c"), Literal(0))
  }


  def this(string: Expression, regex: Expression, position: Expression, occurrence: Expression,
           matchPara: Expression) {
    this(string, regex, position, occurrence, matchPara, Literal(0))
  }

  override def children: Seq[Expression] = Seq(stringExpr, patternExpr,
    positionExpr, occurExpr, matchParaExpr, subExprExpr)
  override def inputTypes: Seq[AbstractDataType] = Seq(StringType, StringType,
    IntegerType, IntegerType, StringType, IntegerType)
  override def dataType: DataType = StringType
  override def nullable: Boolean = children.exists(_.nullable)

  override def checkInputDataTypes(): TypeCheckResult = {

    if (stringExpr.dataType == StringType && patternExpr.dataType == StringType
      && positionExpr.dataType == IntegerType && matchParaExpr.dataType == StringType
      && occurExpr.dataType== IntegerType && subExprExpr.dataType== IntegerType) {
      return TypeCheckResult.TypeCheckSuccess
    }
    return TypeCheckResult.TypeCheckFailure(s"type of the input is not valid")
  }

  def eval(input: InternalRow): Any = {

    val string = stringExpr.eval(input).asInstanceOf[UTF8String].toString
    val regex = patternExpr.eval(input).asInstanceOf[UTF8String].toString
    val position = positionExpr.eval(input).asInstanceOf[Int]
    val occurrence = occurExpr.eval(input).asInstanceOf[Int]
    val matchPara = matchParaExpr.eval(input).asInstanceOf[UTF8String].toString
    val subExpr = subExprExpr.eval(input).asInstanceOf[Int]

    return UTF8String.fromString(CharacterFunctionUtils.regExpSubStr(string, regex, position,
      occurrence, matchPara, subExpr))
  }

  override protected def doGenCode(ctx: CodegenContext,
                                   ev: ExprCode): ExprCode = {

    val eval1 = stringExpr.genCode(ctx)
    val eval2 = patternExpr.genCode(ctx)
    val eval3 = positionExpr.genCode(ctx)
    val eval4 = occurExpr.genCode(ctx)
    val eval5 = matchParaExpr.genCode(ctx)
    val eval6 = subExprExpr.genCode(ctx)

    ev.copy(code = eval1.code + eval2.code + eval3.code + eval4.code +
      eval5.code + eval6.code +
      s"""boolean ${ev.isNull} = ${eval1.isNull};
         ${ctx.javaType(StringType)} ${ev.value} = ${ctx.defaultValue(StringType)};
         ${ev.value} = UTF8String.fromString(org.apache.spark.sql.util.CharacterFunctionUtils.
         regExpSubStr(${eval1.value}.toString(),${eval2.value}.toString(),${eval3.value},
          ${eval4.value},${eval5.value}.toString(), ${eval6.value}));
         """)
  }
}

/**
  * search the occurrence of the regex start from the position of string and replace it,
  * and matchPara can indicate how to match, and return the string that replaced.
  */
@ExpressionDescription(
  usage = "_FUNC_(string, pattern, replace, position, occurrence, matchPara) - search the occurrence of the" +
    "regex start from the position of string and replace it, and matchPara can indicate how to match, and return the string that replaced.",
  extended = """
    Examples:
      > select REGEXP_REPLACE2('500Oracle     Parkway,    Redw0O0Od  Shores, CA','(0O)', '-', 1,2,'i') result;
          500Oracle     Parkway,    Redw-0Od  Shores, CA
  """)
case class RegExpReplace2(stringExpr: Expression, patternExpr: Expression,
                          replaceExpr: Expression, positionExpr: Expression,
                          occurExpr: Expression, matchParaExpr: Expression)
  extends Expression with ImplicitCastInputTypes{

  def this(string: Expression, regex: Expression) {
    this(string, regex, Literal(""), Literal(1), Literal(0), Literal("c"))
  }

  def this(string: Expression, regex: Expression, replace: Expression) {
    this(string, regex, replace, Literal(1), Literal(0), Literal("c"))
  }

  def this(string: Expression, regex: Expression, replace: Expression, position: Expression) {
    this(string, regex, replace, position, Literal(0), Literal("c"))
  }

  def this(string: Expression, regex: Expression, replace: Expression, position: Expression,
           occurrence: Expression) {
    this(string, regex, replace, position, occurrence, Literal("c"))
  }


  override def children: Seq[Expression] = Seq(stringExpr, patternExpr,
    replaceExpr, positionExpr, occurExpr, matchParaExpr)
  override def inputTypes: Seq[AbstractDataType] = Seq(StringType, StringType,
    StringType, IntegerType, IntegerType, StringType)
  override def dataType: DataType = StringType
  override def nullable: Boolean = children.exists(_.nullable)

  override def checkInputDataTypes(): TypeCheckResult = {

    if (stringExpr.dataType == StringType && patternExpr.dataType == StringType
      && positionExpr.dataType == IntegerType && matchParaExpr.dataType == StringType
      && occurExpr.dataType== IntegerType && replaceExpr.dataType== StringType) {
      return TypeCheckResult.TypeCheckSuccess
    }
    return TypeCheckResult.TypeCheckFailure(s"type of the input is not valid")
  }

  def eval(input: InternalRow): Any = {

    val string = stringExpr.eval(input).asInstanceOf[UTF8String].toString
    val regex = patternExpr.eval(input).asInstanceOf[UTF8String].toString
    val position = positionExpr.eval(input).asInstanceOf[Int]
    val occurrence = occurExpr.eval(input).asInstanceOf[Int]
    val replace = replaceExpr.eval(input).asInstanceOf[UTF8String].toString
    val matchPara = matchParaExpr.eval(input).asInstanceOf[UTF8String].toString

    return UTF8String.fromString(CharacterFunctionUtils.regExpReplace(string, regex, replace,
      position, occurrence, matchPara))
  }

  override protected def doGenCode(ctx: CodegenContext,
                                   ev: ExprCode): ExprCode = {

    val eval1 = stringExpr.genCode(ctx)
    val eval2 = patternExpr.genCode(ctx)
    val eval3 = replaceExpr.genCode(ctx)
    val eval4 = positionExpr.genCode(ctx)
    val eval5 = occurExpr.genCode(ctx)
    val eval6 = matchParaExpr.genCode(ctx)

    ev.copy(code = eval1.code + eval2.code + eval3.code + eval4.code +
      eval5.code + eval6.code +
      s"""boolean ${ev.isNull} = ${eval1.isNull};
         ${ctx.javaType(StringType)} ${ev.value} = ${ctx.defaultValue(StringType)};
         ${ev.value} = UTF8String.fromString(org.apache.spark.sql.util.CharacterFunctionUtils.
              regExpReplace(${eval1.value}.toString(),${eval2.value}.toString(),
             ${eval3.value}.toString(), ${eval4.value},${eval5.value},${eval6.value}.toString()));
         """)
  }
}