/*
 * Created by zhongdg1 on 2017/5/17.
 */
package org.apache.spark.sql.catalyst.expressions

case class ForClauseDetail(forType: String = "XML", forXmlClause: ForXmlClause = null) {

}

case class ForXmlClause(xmlType: String = "PATH", rowLabel: String = "row",
                        hasRoot: Boolean = false) {

}
