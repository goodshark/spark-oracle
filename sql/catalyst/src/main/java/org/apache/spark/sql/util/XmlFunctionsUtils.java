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

    // returns an instance of xml containing input text.
    public String sysXmlGeneration(String xmlText)  throws DocumentException{
        StringBuilder result = new StringBuilder();
        result.append("<ROW>");
        result.append(xmlText);
        result.append("</ROW>");
        Document document = DocumentHelper.parseText(result.toString());
        return document.asXML();
    }

    // update a source xml, first parameter is source xml, and then are some pairs of xPath and value.
    public String updateXml(String... inputs) throws DocumentException{
        String sourceXml = inputs[0];
        Document document = DocumentHelper.parseText(sourceXml);
        for(int i = 1; i < inputs.length; i++){
            updateXml2(document, inputs[i], inputs[i+1]);
            i++;
        }
        return document.getRootElement().asXML();
    }

    // used in updateXml.
    private void updateXml2(Document document, String xPathString, String valueToUpdate){

        List<Node> list = document.selectNodes(xPathString);
        if(list.size() == 0){
            return;
        }
        for(Iterator<Node> iterator = list.iterator(); iterator.hasNext();){
            Node node = iterator.next();
            int nodeType = node.getNodeType();
            switch (nodeType) {
                case 3:
                    ((Text) node).setText(valueToUpdate);
                    break;
                case 2:
                    ((Attribute) node).setText(valueToUpdate);
                    break;
                case 1:
                    Document document2;
                    try {
                        document2 = DocumentHelper.parseText(valueToUpdate);
                    } catch (DocumentException de) {
                        throw new IllegalArgumentException("xml that updated is invalid.");
                    }
                    List<Element> childElement = ((Element) node).getParent().elements();
                    childElement.set(childElement.indexOf((Element) node), document2.getRootElement());
                    break;
                default:
            }
        }
    }

    // concatenate two or more xml.
    public String xmlConcat(String... inputs){

        StringBuilder result = new StringBuilder();
        for(int i = 0; i< inputs.length; i++){
            try{
                Document document = DocumentHelper.parseText(inputs[i]);
                result.append(document.getRootElement().asXML());
                if(i != inputs.length - 1)
                    result.append("\n");
            } catch (DocumentException de){
                throw new IllegalArgumentException("number " + (i+ 1) +
                        " of the input parameters is not a valid xml");
            }
        }
        return result.toString();
    }

    // compare expr with all search and return corresponding result that equal, if not found, return default.
    public String decode2(String... inputs){
        int length = inputs.length;
        if(length < 3){
            throw new IllegalArgumentException("there are not enough args.");
        }
        String source = inputs[0];
        HashMap<String, String> keyValues = new HashMap<>();
        int i = 1;
        while((length - i) > 1){
            keyValues.put(inputs[i], inputs[i+1]);
            i = i + 2;
        }
        String defaultValue = null;
        if((length - i) == 1){
            defaultValue = inputs[i];
        }
        if(keyValues.containsKey(source)){
            return keyValues.get(source);
        }
        return defaultValue;
    }

}

