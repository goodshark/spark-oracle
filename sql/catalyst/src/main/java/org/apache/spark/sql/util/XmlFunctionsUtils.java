package org.apache.spark.sql.util;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class XmlFunctionsUtils {

    // takes as arguments an XMLType instance and an XPath expression
    // and returns a scalar value of the resultant node.
    public String insertChildXml(String sourceXml, String xPath, String child, String valueXml){

        sourceXml = xmlFilter(sourceXml);
        XmlNode sourceXmlNode = getRoot(sourceXml);
        valueXml = xmlFilter(valueXml);
        XmlNode valueXmlNode = getRoot(valueXml);
        if(xPath.length()==0){
            throw new IllegalArgumentException("specified xPath is invalid.");
        }

        ArrayList<String> xPathParsed = parseXPath(xPath);
        int xPathLength = xPathParsed.size();
        String[] pathTitle = new String[xPathLength];
        boolean[] isAbsolutePath = new boolean[xPathLength];
        for(int i = 0; i< xPathLength; i++){
            String[] currentPathName = xPathParsed.get(i).split(" ");
            pathTitle[i] = currentPathName[0];
            isAbsolutePath[i] = Boolean.parseBoolean(currentPathName[1]);
        }
        if(!valueXmlNode.getTitle().equals(child)){
            return null;
        }

        appendChildXml2(sourceXmlNode, valueXmlNode, pathTitle, isAbsolutePath, 0);
        return sourceXmlNode.toString();
    }

    // takes as arguments an XMLType instance and an XPath expression
    // and returns a scalar value of the resultant node.
    public String extractXmlValue(String sourceXml, String xPath){
        sourceXml = xmlFilter(sourceXml);
        XmlNode sourceXmlNode = getRoot(sourceXml);
        if(xPath.length()==0){
            throw new IllegalArgumentException("specified xPath is invalid.");
        }

        ArrayList<String> xPathParsed = parseXPath(xPath);
        int xPathLength = xPathParsed.size();
        String[] pathTitle = new String[xPathLength];
        boolean[] isAbsolutePath = new boolean[xPathLength];
        for(int i = 0; i< xPathLength; i++){
            String[] currentPathName = xPathParsed.get(i).split(" ");
            pathTitle[i] = currentPathName[0];
            isAbsolutePath[i] = Boolean.parseBoolean(currentPathName[1]);
        }

        ArrayList<XmlNode> result = new ArrayList<>();
        xmlExtract2(sourceXmlNode, pathTitle, isAbsolutePath, 0, result);
        if(result.size() != 1){
            return null;
        }
        XmlNode nodeFinded = result.get(0);
        if(nodeFinded == null || !nodeFinded.getChildNode().isEmpty() || nodeFinded.getText()==null){
            return null;
        }
        return nodeFinded.getText();
    }

    // returns an XMLType instance containing an XML fragment
    // from an source XML depending on specified xml path.
    public String xmlExtract(String sourceXml, String xPath){
        sourceXml = xmlFilter(sourceXml);
        XmlNode sourceXmlNode = getRoot(sourceXml);
        if(xPath.length()==0){
            throw new IllegalArgumentException("specified xPath is invalid.");
        }

        ArrayList<String> xPathParsed = parseXPath(xPath);
        int xPathLength = xPathParsed.size();
        String[] pathTitle = new String[xPathLength];
        boolean[] isAbsolutePath = new boolean[xPathLength];
        for(int i = 0; i< xPathLength; i++){
            String[] currentPathName = xPathParsed.get(i).split(" ");
            pathTitle[i] = currentPathName[0];
            isAbsolutePath[i] = Boolean.parseBoolean(currentPathName[1]);
        }
        ArrayList<XmlNode> result = new ArrayList<>();
        xmlExtract2(sourceXmlNode, pathTitle, isAbsolutePath, 0, result);
        StringBuilder returnStr = new StringBuilder();
        for(int i = 0; i< result.size(); i++){
            returnStr.append(result.get(i));
        }
        return returnStr.toString();
    }

    // whether traversal of an XML document using a specified
    // path results in any nodes,return 0 if no nodes remain,else return 1.
    public int existsNode(String sourceXml, String xPath){

        sourceXml = xmlFilter(sourceXml);
        XmlNode sourceXmlNode = getRoot(sourceXml);
        if(xPath.length()==0){
            throw new IllegalArgumentException("specified xPath is invalid.");
        }

        ArrayList<String> xPathParsed = parseXPath(xPath);
        int xPathLength = xPathParsed.size();
        String[] pathTitle = new String[xPathLength];
        boolean[] isAbsolutePath = new boolean[xPathLength];
        for(int i = 0; i< xPathLength; i++){
            String[] currentPathName = xPathParsed.get(i).split(" ");
            pathTitle[i] = currentPathName[0];
            isAbsolutePath[i] = Boolean.parseBoolean(currentPathName[1]);
        }
        return existsNode2(sourceXmlNode, pathTitle, isAbsolutePath, 0);
    }

    // delete child xml from source xml depending on xPath.
    public String deleteXml(String sourceXml, String xPath){

        sourceXml = xmlFilter(sourceXml);
        XmlNode sourceXmlNode = getRoot(sourceXml);
        if(xPath.length()==0){
            throw new IllegalArgumentException("specified xPath is invalid.");
        }

        ArrayList<String> xPathParsed = parseXPath(xPath);
        int xPathLength = xPathParsed.size();
        String[] pathTitle = new String[xPathLength];
        boolean[] isAbsolutePath = new boolean[xPathLength];
        for(int i = 0; i< xPathLength; i++){
            String[] currentPathName = xPathParsed.get(i).split(" ");
            pathTitle[i] = currentPathName[0];
            isAbsolutePath[i] = Boolean.parseBoolean(currentPathName[1]);
        }

        if(pathTitle.length==1 && pathTitle[0].equals(xPath)){
            // not allowed to delete root node.
            return null;
        }else if(isAbsolutePath[0] == true && sourceXmlNode.getTitle().equals(pathTitle[0])){
            // if the first path title is absolute and same as root node title,
            // go into all child node with second path title.
            deleteXml2(sourceXmlNode, pathTitle, isAbsolutePath, 1);
        }else if(isAbsolutePath[0] == false){
            // if the first path title is not absolute, go into all child node with the still path title.
            deleteXml2(sourceXmlNode, pathTitle, isAbsolutePath, 0);
        }else {
            // all other cases, do not delete only child node.
        }

        return sourceXmlNode.toString();
    }

    // append one xml into another xml depending on xPath.
    public String appendChildXml(String sourceXml, String xPath, String waitAppendXml){
        sourceXml = xmlFilter(sourceXml);
        waitAppendXml = xmlFilter(waitAppendXml);
        XmlNode sourceXmlNode = getRoot(sourceXml);
        XmlNode waitAppendXmlNode = getRoot(waitAppendXml);
        if(xPath.length()==0){
            throw new IllegalArgumentException("specified xPath is invalid.");
        }

        ArrayList<String> xPathParsed = parseXPath(xPath);
        int xPathLength = xPathParsed.size();
        String[] pathTitle = new String[xPathLength];
        boolean[] isAbsolutePath = new boolean[xPathLength];
        for(int i = 0; i< xPathLength; i++){
            String[] currentPathName = xPathParsed.get(i).split(" ");
            pathTitle[i] = currentPathName[0];
            isAbsolutePath[i] = Boolean.parseBoolean(currentPathName[1]);
        }

        appendChildXml2(sourceXmlNode, waitAppendXmlNode, pathTitle, isAbsolutePath, 0);
        return sourceXmlNode.toString();
    }

    // filter the xml about its "\t\n\s\f" and comment.
    private String xmlFilter(String sourceXml){
        String[] regex = { "\t", "<\\?.*?\\?>", "<!.*?>", "<%.*?%>", "\\s{2,}" };
        for (String reg : regex){
            sourceXml = sourceXml.replaceAll(reg, "");
        }
        return sourceXml;
    }

    // return a XmlNode, which representing the structure of the xml, depending on the string format of xml.
    private XmlNode getRoot(String string){
        XmlNode root = new XmlNode();
        buildNodeTree(string, root);
        return root;
    }

    // used in appendChildXml to recur find the child XmlNode that should add extra XmlNode.
    private void appendChildXml2(XmlNode sourceXmlNode, XmlNode waitAppendXmlNode,
                                 String[] titlePath, boolean[] isAbsolutePath, int index){

        if(titlePath.length == index+1 && sourceXmlNode.getTitle().equals(titlePath[index])){
            // when the current path title is the last and same as the current node title, then add child node.
            sourceXmlNode.getChildNode().add(waitAppendXmlNode);
        }else if(titlePath.length > index+1 && sourceXmlNode.getTitle().equals(titlePath[index])){
            // when the current path title is same as the current node title but not the last,
            // go into all child of the current node with the next path title.
            int numChild = sourceXmlNode.getChildNode().size();
            for(int i = 0; i< numChild; i++){
                appendChildXml2(sourceXmlNode.getChildNode().get(i), waitAppendXmlNode, titlePath, isAbsolutePath, index+1);
            }
        }
        // when the current path title is not absolute.
        if(isAbsolutePath[index] == false){
            int numChild = sourceXmlNode.getChildNode().size();
            for(int i = 0; i< numChild; i++){
                appendChildXml2(sourceXmlNode.getChildNode().get(i), waitAppendXmlNode, titlePath, isAbsolutePath, index);
            }
        }
    }

    // used in deleteXml and deleteXml3 to traverse all child node of the current source node.
    private void deleteXml2(XmlNode sourceXmlNode, String[] pathTitle, boolean[] isAbsolutePath, int index){
        Iterator<XmlNode> childIterator = sourceXmlNode.getChildNode().iterator();
        while(childIterator.hasNext()){
            XmlNode currentChild = childIterator.next();
            deleteXml3(childIterator, currentChild, pathTitle, isAbsolutePath, index);
        }
    }

    // used in deleteXml2 to recur find the child XmlNode that should delete.
    private void deleteXml3(Iterator<XmlNode> brotherIterator, XmlNode sourceXmlNode,
                            String[] titlePath, boolean[] isAbsolutePath, int index){
        if(titlePath.length == index+1 && sourceXmlNode.getTitle().equals(titlePath[index])){
            // when the current path title is last and same as the title of the current node.
            brotherIterator.remove();
            return;
        }else if(titlePath.length > index+1 && sourceXmlNode.getTitle().equals(titlePath[index])){
            // when the current path title is same as the current node title but not the last,
            // go into all child of the current node with the next path title.
            deleteXml2(sourceXmlNode, titlePath, isAbsolutePath, index+1);
        }
        // when the current path title is not absolute, go into all child of the
        // current node with the still path title.
        if(isAbsolutePath[index] == false){
            deleteXml2(sourceXmlNode, titlePath, isAbsolutePath, index);
        }
    }

    // used in existsNode to recur find existing the child XmlNode that.
    private int existsNode2(XmlNode sourceXmlNode, String[] titlePath, boolean[] isAbsolutePath, int index){

        if(titlePath.length == index+1 && sourceXmlNode.getTitle().equals(titlePath[index])){
            // when the current path title is the last and same as the current node title, exist.
            return 1;
        }else if(titlePath.length > index+1 && sourceXmlNode.getTitle().equals(titlePath[index])){
            // when the current path title is same as the current node title but not the last,
            // go into all child of the current node with the next path title.
            int numChild = sourceXmlNode.getChildNode().size();
            for(int i = 0; i< numChild; i++){
                if(existsNode2(sourceXmlNode.getChildNode().get(i), titlePath, isAbsolutePath, index+1)== 1){
                    return 1;
                }
            }
        }
        // when the current path title is not absolute, go into all child of the current node with the still path title.
        if(isAbsolutePath[index] == false){
            int numChild = sourceXmlNode.getChildNode().size();
            for(int i = 0; i< numChild; i++){
                if(existsNode2(sourceXmlNode.getChildNode().get(i), titlePath, isAbsolutePath, index)== 1){
                    return 1;
                }
            }
        }
        return 0;
    }

    // used in xmlExtract to recur find specified child node.
    private void  xmlExtract2(XmlNode sourceXmlNode, String[] titlePath,
                              boolean[] isAbsolutePath, int index, ArrayList<XmlNode> result){

        if(titlePath.length == index+1 && sourceXmlNode.getTitle().equals(titlePath[index])){
            // when the current path title is the last and same as the current node title, return sourceXmlNode.
            result.add(sourceXmlNode);
        }else if(titlePath.length > index+1 && sourceXmlNode.getTitle().equals(titlePath[index])){
            // when the current path title is same as the current node title but not the last,
            // go into all child of the current node with the next path title.
            int numChild = sourceXmlNode.getChildNode().size();
            for(int i = 0; i< numChild; i++){
                xmlExtract2(sourceXmlNode.getChildNode().get(i), titlePath, isAbsolutePath, index+1, result);
            }
        }
        // when the current path title is not absolute, go into all child of the current node with the still path title.
        if(isAbsolutePath[index] == false){
            int numChild = sourceXmlNode.getChildNode().size();
            for(int i = 0; i< numChild; i++){
                xmlExtract2(sourceXmlNode.getChildNode().get(i), titlePath, isAbsolutePath, index, result);
            }
        }
    }

    // parse xPath and return array of (pathTitle + isAbsolutePath).
    private ArrayList<String> parseXPath(String xPath){

        String regex1 = "(((//)|(/))?[^/]+)+";
        String regex2 = "((//)|(/))?[^/]+";
        if(!xPath.matches(regex1)){
            throw new IllegalArgumentException("the input xPath is invalid");
        }
        Pattern p = Pattern.compile(regex2);
        Matcher m = p.matcher(xPath);
        ArrayList<String> returnArr = new ArrayList<>();
        String current;
        StringBuilder result = new StringBuilder();
        while(m.find()){
            current = m.group();
            result.delete(0,result.length());
            if(current.matches("//.*")){
                result.append(current.substring(2));
                result.append(' ');
                result.append(false);
                returnArr.add(result.toString());
            }else if(current.matches("/.*")){
                result.append(current.substring(1));
                result.append(' ');
                result.append(true);
                returnArr.add(result.toString());
            }else{
                result.append(current);
                result.append(' ');
                result.append(true);
                returnArr.add(result.toString());
            }
        }
        return returnArr;
    }

    // parse all text from a xml string.
    private List<String> parser(String text){
        List<String> childrenDocs = new ArrayList<String>();
        Pattern p = Pattern.compile("<.*?>(.*)</.*?>");
        Matcher m = p.matcher(text);
        if (m.matches()){
            String inner = m.group(1);
            p = Pattern.compile("<(.*?)>");
            m = p.matcher(inner);
            while (m.find()){
                String s1 = m.group(1);
                if (s1.endsWith("/")){
                    childrenDocs.add(m.group());
                } else if (!s1.startsWith("/") && !s1.endsWith("/")){
                    int start = m.end() - m.group().length();
                    int index = 1;
                    while(m.find()){
                        String s2 = m.group(1);
                        if (!s2.startsWith("/") && !s2.endsWith("/")){
                            index++;
                        } else if (s2.startsWith("/")){
                            index--;
                        }
                        if (index == 0) {
                            break;
                        }
                    }
                    int end = m.end();
                    childrenDocs.add(inner.substring(start, end));
                }
            }
        }
        return childrenDocs;
    }

    private void buildNodeTree(String str, XmlNode n){
        buildNodeTitle(str, n);
        buildNodeAttribute(str, n);
        buildNodeText(str, n);
        if (!parser(str).isEmpty()){
            for (String temp : parser(str)){
                XmlNode child = new XmlNode();
                buildNodeTitle(temp, child);
                buildNodeAttribute(temp, child);
                buildNodeText(temp, child);
                n.getChildNode().add(child);
                buildNodeTree(temp, child);
            }
        }
    }

    private void buildNodeTitle(String str, XmlNode n){
        Pattern p = Pattern.compile("<.*?>");
        Matcher m = p.matcher(str);
        if (m.find()){
            String temp = m.group();
            String s = temp.substring(1, temp.length() - 1).split(" ")[0];
            if (s.endsWith("/")){
                n.setTitle(s.substring(0, s.length() - 1));
            }else{
                n.setTitle(s.split(" ")[0]);
            }
        }
    }

    private void buildNodeAttribute(String str, XmlNode n){
        Pattern p = Pattern.compile("<.*?>");
        Matcher m = p.matcher(str);
        if (m.find()){
            String temp = m.group();
            String s = temp.substring(1, temp.length() - 1);
            p = Pattern.compile("(\\S*)=\"(.*?)\"");
            m = p.matcher(s);
            while (m.find()){
                String key = m.group(1).trim();
                String value = m.group(2).trim();
                n.getAttribute().put(key, value);
            }
            p = Pattern.compile("(\\S*)=(-?\\d+(\\.\\d+)?)");
            m = p.matcher(s);
            while (m.find()){
                String key = m.group(1).trim();
                String value = m.group(2).trim();
                n.getAttribute().put(key, value);
            }
        }
    }

    private void buildNodeText(String str, XmlNode n){
        Pattern p = Pattern.compile("<.*?>(.*)</.*?>");
        Matcher m = p.matcher(str);
        List<String> childrenDocs = parser(str);
        if (m.find()){
            String temp = m.group(1);
            for (String s : childrenDocs){
                temp = temp.replaceFirst(s, "");
            }
            n.setText(temp.trim());
        }
    }
}

class XmlNode {

    private String title;
    private String text;

    private Map<String, String> attributes = new HashMap<String, String>();
    private List<XmlNode> childNodes = new LinkedList<XmlNode>();

    public void setTitle(String title){
        this.title = title;
    }

    public String getTitle(){
        return title;
    }

    public void setText(String text){
        this.text = text;
    }

    public String getText(){
        return text;
    }

    public Map<String, String> getAttribute(){
        return attributes;
    }

    public List<XmlNode> getChildNode(){
        return childNodes;
    }

    private String attrToString() {
        if (attributes.isEmpty()) {
            return "";
        }
        Iterator<Map.Entry<String, String>> its = attributes.entrySet().iterator();
        StringBuffer buff = new StringBuffer();
        while (its.hasNext()) {
            Map.Entry<String, String> entry = its.next();
            buff.append(entry.getKey() + "=\"" + entry.getValue() + "\" ");
        }
        return " " + buff.toString().trim();
    }
    public String toString() {
        String attr = attrToString();
        if (childNodes.isEmpty() && text == null) {
            return "<" + title + attr + "/>";
        } else if (childNodes.isEmpty() && text != null) {
            return "<" + title + attr + ">" + text + "</" + title + ">";
        } else {
            StringBuilder result = new StringBuilder();
            result.append("<" + title + attr + ">");
            if (!text.isEmpty()) {
                result.append(text);
            }
            for (XmlNode n : childNodes) {
                result.append(n.toString());
            }
            result.append("</" + title + ">");
            return result.toString();
        }
    }
}
