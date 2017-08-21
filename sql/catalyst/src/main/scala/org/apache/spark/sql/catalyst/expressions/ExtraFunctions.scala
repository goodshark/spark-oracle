
package org.apache.spark.sql.catalyst.expressions

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.DateFormatTrans
import org.apache.spark.unsafe.types.{CalendarInterval, UTF8String}


// /**
//  * Created by XYZ on 2017/8/1.
//  */

//    *********************************************************************************
//    ************************************Width_Bucket*********************************
//    *********************************************************************************
case class Width_Bucket(fieldExpr: Expression,
                        minExpr: Expression,
                        maxExpr: Expression,
                        numExpr: Expression)
  extends Expression with ImplicitCastInputTypes{

  override def children: Seq[Expression] = Seq(fieldExpr, minExpr, maxExpr, numExpr)
  override def inputTypes: Seq[AbstractDataType] =
    Seq(DateType, DateType, DateType, IntegerType)
  override def dataType: DataType = IntegerType
  override def nullable: Boolean = children.exists(_.nullable)

  override def checkInputDataTypes(): TypeCheckResult = {
    if (fieldExpr.dataType != DateType && minExpr.dataType != DateType
      && maxExpr.dataType != DateType && numExpr.dataType != IntegerType) {
      TypeCheckResult.TypeCheckFailure(s"type of the input is not valid")
    } else {
      TypeCheckResult.TypeCheckSuccess
    }
  }

  def eval(input: InternalRow): Any = {

    val num = numExpr.eval(input).asInstanceOf[Int]
    val field_value = fieldExpr.eval(input).asInstanceOf[Long]
    val min_value = minExpr.eval(input).asInstanceOf[Long]
    val max_value = maxExpr.eval(input).asInstanceOf[Long]
    val interval = (max_value - min_value) / num.toDouble
    // require(field_value>= min_value && field_value<= max_value, "field must between min and max")

    for(i <- 1 to num) {
      if (field_value>= (min_value + (i - 1) * interval)
        && (field_value< max_value + i * interval)) {
        return i
      }
    }

    if(field_value == max_value) {
      return num + 1
    }
  }

  override protected def doGenCode(ctx: CodegenContext,
                                   ev: ExprCode): ExprCode = {

    val eval1 = fieldExpr.genCode(ctx)
    val eval2 = minExpr.genCode(ctx)
    val eval3 = maxExpr.genCode(ctx)
    val eval4 = numExpr.genCode(ctx)
    val num_Double = eval4.value.toDouble

    val endpoint =
      s""" ${ctx.javaType(DateType)} value_field = ${eval1.value};
        ${ctx.javaType(DateType)} value_min = ${eval2.value};
        ${ctx.javaType(DateType)} value_max = ${eval3.value};
        ${ctx.javaType(DoubleType)} interval = (value_max - value_min) / ${num_Double};
        ${ctx.javaType(IntegerType)} ${ev.value} = ${ctx.defaultValue(IntegerType)};
        boolean ${ev.isNull} = ${eval1.isNull};
        if(value_field== value_max)
         ${ev.value}= (${eval4.value}+ 1);"""

    val other = (1 to eval4.value.toInt).map{i =>

      s""" if(value_field>= (value_min+ (${i}- 1)* interval) &&
           value_field< (value_min+ ${i}* interval))
        ${ev.value}= ${i};"""
    }.mkString("\n")


    ev.copy(code = eval1.code + eval2.code + eval3.code + eval4.code + s"""${endpoint}
      ${other}""")
  }

}

//    *********************************************************************************
//    ************************************HexToRaw*************************************
//    *********************************************************************************
case class HexToRaw(inputString: Expression)
  extends Expression with ImplicitCastInputTypes{

  override def children: Seq[Expression] = Seq(inputString)
  override def inputTypes: Seq[AbstractDataType] =
    Seq(StringType)
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

    val hex = inputString.eval(input).asInstanceOf[UTF8String]

    var temp = ""
    for (i <- 0 until hex.numChars()/ 2) {
      temp = temp + Integer.valueOf(hex.toString.substring(i * 2, i * 2 + 2),
        16).toChar.toString;
    }

    return temp
  }

  override protected def doGenCode(ctx: CodegenContext,
                                   ev: ExprCode): ExprCode = {

    val eval1 = inputString.genCode(ctx)

    val other =
      s"""
         String str = "";
         int a=0;
         int i=0;
         while(i< ${eval1.value}.numChars()/2) {
            a= Integer.valueOf(${eval1.value}.toString().substring(i*2,i*2 + 2),16);
            str += (char)a;
            i+=1;
         }
       """

    ev.copy(code = eval1.code +
      s"""boolean ${ev.isNull} = ${eval1.isNull};
         ${ctx.javaType(StringType)} ${ev.value} = ${ctx.defaultValue(StringType)};
         ${other};
         ${ev.value} = UTF8String.fromString(str);""")
  }

}

//    *********************************************************************************
//    ************************************ToChar*************************************
//    *********************************************************************************
case class ToChar(inputExpr: Expression, formatExpr: Expression)
  extends Expression with ImplicitCastInputTypes{

  def this(input: Expression) {
    this(input, Literal(null))
  }

  override def children: Seq[Expression] = Seq(inputExpr, formatExpr)
  override def inputTypes: Seq[AbstractDataType] =
    Seq(TypeCollection(NumericType, ShortType, IntegerType, LongType,
      FloatType, DoubleType, DecimalType,
      TimestampType, DateType,
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
    val theType = inputExpr.dataType match {
      case DateType => 2
      case TimestampType => 3
      case _ => 1
    }

    if (theType == 2) {
      return UTF8String.fromString(DateFormatTrans.sparkDateToSpecifiedDate(
        DateTimeUtils.dateToString(result.asInstanceOf[Int]), constFormat));
    } else if (theType == 3) {
      return UTF8String.fromString(DateFormatTrans.sparkTimestampToSpecifiedDate(
        DateTimeUtils.timestampToString(result.asInstanceOf[Long]), constFormat));
    } else {
      return UTF8String.fromString(result.toString);
    }
  }

  override protected def doGenCode(ctx: CodegenContext,
                                   ev: ExprCode): ExprCode = {

    val eval1 = inputExpr.genCode(ctx)
    val theType = inputExpr.dataType match {
      case DateType => 2
      case TimestampType => 3
      case _ => 1
    }

    val dtu = DateTimeUtils.getClass.getName.stripSuffix("$")
    val other = if (theType == 2) {
      s"""result= UTF8String.fromString(
         org.apache.spark.sql.defined_myself.DateFormatTrans.sparkDateToSpecifiedDate(
         $dtu.dateToString(${eval1.value}), "${constFormat}"));"""
    } else if (theType == 3) {
      s"""result= UTF8String.fromString(
          org.apache.spark.sql.defined_myself.DateFormatTrans.sparkTimestampToSpecifiedDate(
        $dtu.timestampToString(${eval1.value}), "${constFormat}"));"""
    } else {
      s"""result= UTF8String.fromString(${eval1.value} + "");"""
    }

    ev.copy(code = eval1.code +
      s"""boolean ${ev.isNull} = ${eval1.isNull};
         ${ctx.javaType(StringType)} ${ev.value} = ${ctx.defaultValue(StringType)};
         ${ctx.javaType(StringType)} result= UTF8String.fromString("");
         ${other}
         ${ev.value} = result;""")
  }
}

//    *********************************************************************************
//    ************************************ToChar2*************************************
//    *********************************************************************************
case class ToChar2(inputExpr: Expression)
  extends Expression with ImplicitCastInputTypes{

  override def children: Seq[Expression] = Seq(inputExpr)
  override def inputTypes: Seq[AbstractDataType] =
    Seq(TypeCollection(NumericType, ShortType, IntegerType, LongType,
      FloatType, DoubleType, DecimalType,
      TimestampType, DateType,
      StringType))
  override def dataType: DataType = StringType
  override def nullable: Boolean = children.exists(_.nullable)

  override def checkInputDataTypes(): TypeCheckResult = {
    inputExpr.dataType match {
      case StringType | DateType | TimestampType | DecimalType() | DoubleType | FloatType |
           LongType | IntegerType | ShortType => return TypeCheckResult.TypeCheckSuccess
      case _ => return TypeCheckResult.TypeCheckFailure(s"type of the input is not valid")
    }
  }

  def eval(input: InternalRow): Any = {

    val result = inputExpr.eval(input)
    val theType = inputExpr.dataType match {
      case DateType => 2
      case TimestampType => 3
      case _ => 1
    }

    if (theType == 2) {
      return UTF8String.fromString(DateFormatTrans.sparkDateToSpecifiedDate(
        DateTimeUtils.dateToString(result.asInstanceOf[Int]), "yyyy-mm-dd")
      );
    } else if (theType == 3) {
      return UTF8String.fromString(DateFormatTrans.sparkTimestampToSpecifiedDate(
        DateTimeUtils.timestampToString(result.asInstanceOf[Int]), "yyyy-mm-dd hh24:mi:ss.ff"
      ));
    } else {
      return UTF8String.fromString(result.toString);
    }
  }

  override protected def doGenCode(ctx: CodegenContext,
                                   ev: ExprCode): ExprCode = {

    val eval1 = inputExpr.genCode(ctx)
    val theType = inputExpr.dataType match {
      case DateType => 2
      case TimestampType => 3
      case _ => 1
    }

    val dtu = DateTimeUtils.getClass.getName.stripSuffix("$")
    val other = if (theType == 2) {
      s"""result= UTF8String.fromString(
         |org.apache.spark.sql.defined_myself.DateFormatTrans.sparkDateToSpecifiedDate(
         |$dtu.dateToString(${eval1.value}), "yyyy-mm-dd")
      );"""
    } else if (theType == 3) {
      s"""result= UTF8String.fromString(
          org.apache.spark.sql.defined_myself.DateFormatTrans.sparkTimestampToSpecifiedDate(
        $dtu.timestampToString(${eval1.value}
        ), "yyyy-mm-dd hh24:mi:ss.ff"
      ));"""
    } else {
      s"""result= UTF8String.fromString(${eval1.value} + "");"""
    }

    ev.copy(code = eval1.code +
      s"""boolean ${ev.isNull} = ${eval1.isNull};
         ${ctx.javaType(StringType)} ${ev.value} = ${ctx.defaultValue(StringType)};
         ${ctx.javaType(StringType)} result= UTF8String.fromString("");
         ${other}
         ${ev.value} = result;""")
  }
}

//    *********************************************************************************
//    ************************************ToDate2*************************************
//    *********************************************************************************
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
         String resultString =
         org.apache.spark.sql.defined_myself.DateFormatTrans.specifiedDateToSparkDate(
               ${eval1.value}.toString(), ${eval2.value}.toString());
         if(${dtu}.stringToDate(UTF8String.fromString(resultString)).get() != null) {
            ${ev.value} = Integer.parseInt(${dtu}.stringToDate(
            UTF8String.fromString(resultString)).get().toString());
         }
         """)
  }
}

//    *********************************************************************************
//    ************************************ToTimestamp*************************************
//    *********************************************************************************
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
         String resultString =
         org.apache.spark.sql.defined_myself.DateFormatTrans.specifiedDateToSparkTimestamp(
               ${eval1.value}.toString(), ${eval2.value}.toString());
         if(${dtu}.stringToTimestamp(UTF8String.fromString(resultString)).get() != null) {
            ${ev.value} = Long.parseLong(${dtu}.stringToTimestamp(
            UTF8String.fromString(resultString)).get().toString());
         }
         """)
  }
}

//    *********************************************************************************
//    ************************************NumToDSInterval*************************************
//    *********************************************************************************
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

//    *********************************************************************************
//    ************************************NumToYMInterval*************************************
//    *********************************************************************************
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

//    *********************************************************************************
//    ************************************toDSInterval*************************************
//    *********************************************************************************
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
         org.apache.spark.sql.defined_myself.DateFormatTrans.oracleDSIntervalToSparkInterval(
         ${eval1.value}.toString()));
         """)
  }
}

//    *********************************************************************************
//    ************************************toYMInterval*************************************
//    *********************************************************************************
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
         org.apache.spark.sql.defined_myself.DateFormatTrans.oracleYMIntervalToSparkInterval(
         ${eval1.value}.toString()));
         """)
  }
}