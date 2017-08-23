
package org.apache.spark.sql.catalyst.expressions

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.DateFormatTrans
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

    val initial =
      s""" ${ctx.javaType(fieldExpr.dataType)} valueField = ${eval1.value};
        ${ctx.javaType(fieldExpr.dataType)} valueMin = ${eval2.value};
        ${ctx.javaType(fieldExpr.dataType)} valueMax = ${eval3.value};
        ${ctx.javaType(DoubleType)} interval = (valueMax - valueMin) / ${numDouble};
        ${ctx.javaType(IntegerType)} ${ev.value} = ${ctx.defaultValue(IntegerType)};
        boolean ${ev.isNull} = ${eval1.isNull};
        """

    val endpoint =
      s"""if(valueField== valueMax) {
            ${ev.value}= (${eval4.value}+ 1);
          }"""

    val other =
      s"""for(int i = 0; i< ${eval4.value}; i++) {
              if(valueField>= (valueMin+ (i- 1)* interval) && valueField< (valueMin+ i* interval)){
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

    val other =
      s"""
         StringBuilder str = new StringBuilder();
         String input = ${eval1.value}.toString();
         if(input.length()%2 == 1) {
             input = "0" + input;
         }
         int a=0;
         for(int i = 0; i< input.length()/2; i++) {
            a= Integer.valueOf(input.substring(i*2,i*2 + 2),16);
            str.append((char)a);
         }
       """

    ev.copy(code = eval1.code +
      s"""boolean ${ev.isNull} = ${eval1.isNull};
         ${ctx.javaType(StringType)} ${ev.value} = ${ctx.defaultValue(StringType)};
         ${other};
         ${ev.value} = UTF8String.fromString(str.toString());""")
  }

}

/**
  * Convert the input of string or datetime or number into string, when the input is
  * datetime(date or timestamp),the string format can be specified, if not, defualt
  * is used.
  */
@ExpressionDescription(
  usage = "_FUNC_(input,format) - Convert the input of string or datetime or number into string," +
    "when the input is datetime(date or timestamp),the string format can be specified," +
    "if not, defualt is used.",
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
    val other = inputExpr.dataType match {
      case DateType =>
        s"""result= UTF8String.fromString(
         org.apache.spark.sql.util.DateFormatTrans.sparkDateToSpecifiedDate(
         $dtu.dateToString(${eval1.value}), "${constFormat}"));"""
      case TimestampType =>
        s"""result= UTF8String.fromString(
          org.apache.spark.sql.util.DateFormatTrans.sparkTimestampToSpecifiedDate(
        $dtu.timestampToString(${eval1.value}), "${constFormat}"));"""
      case _ => s"""result= UTF8String.fromString(${eval1.value} + "");"""
    }

    ev.copy(code = eval1.code +
      s"""boolean ${ev.isNull} = ${eval1.isNull};
         ${ctx.javaType(StringType)} ${ev.value} = ${ctx.defaultValue(StringType)};
         ${ctx.javaType(StringType)} result= UTF8String.fromString("");
         ${other}
         ${ev.value} = result;""")
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

    ev.copy(code = eval1.code + eval2.code +
      s"""boolean ${ev.isNull} = ${eval1.isNull};
         ${ctx.javaType(CalendarIntervalType)} ${ev.value} =
         ${ctx.defaultValue(CalendarIntervalType)};
         String unit = ${eval2.value}.toString();
         if(unit.equals("day")|unit.equals("hour")|unit.equals("minute")|unit.equals("second")) {
            ${ev.value} = org.apache.spark.unsafe.types.CalendarInterval.fromSingleUnitString(
            unit, String.valueOf(${eval1.value}));
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

    ev.copy(code = eval1.code + eval2.code +
      s"""boolean ${ev.isNull} = ${eval1.isNull};
         ${ctx.javaType(CalendarIntervalType)} ${ev.value} =
         ${ctx.defaultValue(CalendarIntervalType)};
         String unit = ${eval2.value}.toString().toLowerCase();
         if(unit.equals("year")|unit.equals("month")) {
            ${ev.value} = org.apache.spark.unsafe.types.CalendarInterval.fromSingleUnitString(
            unit, String.valueOf(${eval1.value}));
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