/*
  * Created by chenfolin on 2017/7/25.
  */
package org.apache.spark.sql.catalyst.expressions

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.{ArrayData, MapData}
import org.apache.spark.sql.types.{DataType, Decimal}
import org.apache.spark.unsafe.types.{CalendarInterval, UTF8String}

class FunctionArgsRow(val fields: Int) extends InternalRow{

  val objects = new Array[Any](fields);

  override def numFields: Int = fields

  override def setNullAt(i: Int): Unit = {
    objects(i) = null
  }

  override def update(i: Int, value: Any): Unit = {
    objects(i) = value;
  }

  override def copy(): InternalRow = {
    null
  }

  override def anyNull: Boolean = {
    false
  }

  override def isNullAt(ordinal: Int): Boolean = {
    objects(ordinal) == null
  }

  override def getBoolean(ordinal: Int): Boolean = {
    objects(ordinal).asInstanceOf[Boolean]
  }

  override def getByte(ordinal: Int): Byte = {
    objects(ordinal).asInstanceOf[Byte]
  }

  override def getShort(ordinal: Int): Short = {
    objects(ordinal).asInstanceOf[Short]
  }

  override def getInt(ordinal: Int): Int = {
    objects(ordinal).asInstanceOf[Int]
  }

  override def getLong(ordinal: Int): Long = {
    objects(ordinal).asInstanceOf[Long]
  }

  override def getFloat(ordinal: Int): Float = {
    objects(ordinal).asInstanceOf[Float]
  }

  override def getDouble(ordinal: Int): Double = {
    objects(ordinal).asInstanceOf[Double]
  }

  override def getDecimal(ordinal: Int, precision: Int, scale: Int): Decimal = {
    objects(ordinal).asInstanceOf[Decimal]
  }

  override def getUTF8String(ordinal: Int): UTF8String = {
    objects(ordinal).asInstanceOf[UTF8String]
  }

  override def getBinary(ordinal: Int): Array[Byte] = {
    objects(ordinal).asInstanceOf[Array[Byte]]
  }

  override def getInterval(ordinal: Int): CalendarInterval = {
    objects(ordinal).asInstanceOf[CalendarInterval]
  }

  override def getStruct(ordinal: Int, numFields: Int): InternalRow = {
    objects(ordinal).asInstanceOf[InternalRow]
  }

  override def getArray(ordinal: Int): ArrayData = {
    objects(ordinal).asInstanceOf[ArrayData]
  }

  override def getMap(ordinal: Int): MapData = {
    objects(ordinal).asInstanceOf[MapData]
  }

  override def get(ordinal: Int, dataType: DataType): AnyRef = {
    objects(ordinal).asInstanceOf[DataType]
  }

  def get(ordinal: Int): Any = {
    objects(ordinal)
  }

  def update(values: Array[UpdateValue]): FunctionArgsRow = {
    values.foreach(value => update(value.index, value.value))
    this
  }
}

class UpdateValue(val index: Int, val value: Any )
