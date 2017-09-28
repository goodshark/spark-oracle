package org.apache.spark.sql.util;

import org.dom4j.*;
import java.util.*;

public class XmlFunctionsUtils {

    // append one xml into another xml depending on xPath.
    public String appendChildXml(String sourceXml, String xPath, String waitAppendXml) throws DocumentException{
        Document document = DocumentHelper.parseText(sourceXml);
        List list = document.selectNodes(xPath);
        for(int i = 0; i< list.size(); i++){
            Document document2 = DocumentHelper.parseText(waitAppendXml);
            ((Element)list.get(i)).add(document2.getRootElement());
        }
        return document.getRootElement().asXML();
    }

    // delete child xml from source xml depending on xPath.
    public String deleteXml(String sourceXml, String xPath){

        Document document;
        try{
            document = DocumentHelper.parseText(sourceXml);
        } catch(DocumentException de) {
            throw new IllegalArgumentException("input cannot be parsed");
        }

        List<Node> list = document.selectNodes(xPath);
        for(Iterator<Node> iterator = list.iterator(); iterator.hasNext();){
            Element waitToDelete = (Element) iterator.next();
            Element parent = waitToDelete.getParent();
            if(parent != null){
                parent.remove(waitToDelete);
            }else{
                throw new IllegalArgumentException("cannot delete root node");
            }
        }
        return document.getRootElement().asXML();
    }

    // whether traversal of an XML document using a specified
    // path results in any nodes,return 0 if no nodes remain,else return 1.
    public int existsNode(String sourceXml, String xPath){

        Document document;
        try{
            document = DocumentHelper.parseText(sourceXml);
        } catch(DocumentException de) {
            throw new IllegalArgumentException("input cannot be parsed");
        }

        List<Node> list = document.selectNodes(xPath);
        if(list.size() == 0){
            return 0;
        }else{
            return 1;
        }
    }

    // returns an XMLType instance containing an XML fragment
    // from an source XML depending on specified xml path.
    public String xmlExtract(String sourceXml, String xPath){
        Document document;
        try{
            document = DocumentHelper.parseText(sourceXml);
        } catch(DocumentException de) {
            throw new IllegalArgumentException("input cannot be parsed");
        }

        StringBuilder result = new StringBuilder();
        List<Node> list = document.selectNodes(xPath);
        for(Iterator<Node> iterator = list.iterator(); iterator.hasNext();){
            result.append(iterator.next().asXML());
        }

        return result.toString();
    }

    // takes as arguments an XMLType instance and an XPath expression
    // and returns a scalar value of the resultant node.
    public String extractXmlValue(String sourceXml, String xPath){
        Document document;
        try{
            document = DocumentHelper.parseText(sourceXml);
        } catch(DocumentException de) {
            throw new IllegalArgumentException("input cannot be parsed");
        }
        List<Node> list = document.selectNodes(xPath);
        if(list.size() != 1 && ((Element)list.get(0)).isTextOnly()){
            return null;
        }
        return list.get(0).getText();
    }

    // takes as arguments an XMLType instance and an XPath expression
    // and returns a scalar value of the resultant node.
    public String insertChildXml(String sourceXml, String xPath, String child, String valueXml)
            throws DocumentException{
        Document document = DocumentHelper.parseText(sourceXml);
        List list = document.selectNodes(xPath);
        for(int i = 0; i< list.size(); i++){
            Document document2 = DocumentHelper.parseText(valueXml);
            if(!child.equals(document2.getRootElement().getName())){
                throw new IllegalArgumentException("specified child expression is illegal");
            }
            ((Element)list.get(i)).add(document2.getRootElement());
        }
        return document.getRootElement().asXML();
    }

    //insert a user-supplied value into the source XML at the node
    // indicated by the XPath expression and the position in brothers.
    public String insertChildXmlAfter(String sourceXml, String xPath, String child,
                                      String valueXml) throws DocumentException{
        StringBuilder result = new StringBuilder();
        result.append(xPath);
        result.append("/");
        result.append(child);
        return insertXmlAfter(sourceXml, result.toString(), valueXml);
    }

    //insert a user-supplied value into the source XML at the node
    // indicated by the XPath expression and the position in brothers(before).
    public String insertChildXmlBefore(String sourceXml, String xPath, String child,
                                       String valueXml) throws DocumentException{
        StringBuilder result = new StringBuilder();
        result.append(xPath);
        result.append("/");
        result.append(child);
        return insertXmlBefore(sourceXml, result.toString(), valueXml);
    }

    //insert a user-supplied value into the source XML at the node
    // indicated by the XPath expression(after).
    public String insertXmlAfter(String sourceXml, String xPath,
                                 String valueXml) throws DocumentException{
        Document document = DocumentHelper.parseText(sourceXml);
        List<Node> list = document.selectNodes(xPath);
        if(list.size() == 0){
            return document.getRootElement().asXML();
        }
        for(Iterator<Node> iterator = list.iterator(); iterator.hasNext();){
            Element before = (Element) iterator.next();
            Document document2 = DocumentHelper.parseText(valueXml);
            List<Element> childElement = before.getParent().elements();
            int index = childElement.indexOf(before)+1;
            Element current;
            Element last = document2.getRootElement();
            while(childElement.size()>index){
                current = childElement.get(index);
                childElement.set(index, last);
                last = current;
                index++;
            }
            childElement.add(last);
        }
        return document.getRootElement().asXML();
    }

    //insert a user-supplied value into the source XML at the node
    // indicated by the XPath expression(before).
    public String insertXmlBefore(String sourceXml, String xPath,
                                  String valueXml) throws DocumentException{

        Document document = DocumentHelper.parseText(sourceXml);
        List<Node> list = document.selectNodes(xPath);
        if(list.size() == 0){
            return document.getRootElement().asXML();
        }
        for(Iterator<Node> iterator = list.iterator(); iterator.hasNext();){
            Element after = (Element) iterator.next();
            Document document2 = DocumentHelper.parseText(valueXml);
            List<Element> childElement = after.getParent().elements();
            childElement.add(document2.getRootElement());
            int index = childElement.size()-1;
            Element current = childElement.get(index-1);
            Element temp = current.createCopy();
            Element next;
            while(current != after){
                childElement.set(index-1, temp);
                next = childElement.get(index);
                childElement.set(index, current);
                childElement.set(index-1, next);
                index--;
                current = childElement.get(index-1);
            }
            childElement.set(index-1, temp);
            next = childElement.get(index);
            childElement.set(index, current);
            childElement.set(index-1, next);
        }
        return document.getRootElement().asXML();
    }

    public String sysXmlGeneration(String xmlText)  throws DocumentException{
        StringBuilder result = new StringBuilder();
        result.append("<ROW>");
        result.append(xmlText);
        result.append("</ROW>");
        Document document = DocumentHelper.parseText(result.toString());
        return document.asXML();
    }

}

