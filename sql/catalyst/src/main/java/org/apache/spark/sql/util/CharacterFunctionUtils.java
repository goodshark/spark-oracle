package org.apache.spark.sql.util;

import java.util.ArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class CharacterFunctionUtils {

    // create a matcher depending on regex and string.
    private static Matcher createMatcher(String regex, String waitToMatch){
        Pattern p1 = Pattern.compile(regex);
        return p1.matcher(waitToMatch);
    }

    // search the occurrence number of the substring start from the position of string,
    // return the start position of result.
    public static int stringInstr(String string, String substring, int position, int occurrence){
        String regex = "^"+substring+".*";
        if(position > 0){
            int occurCount = 0;
            for(int i = position-1; i<string.length(); i++){
                if(string.substring(i).matches(regex)){
                    occurCount++;
                }
                if(occurCount == occurrence){
                    return i+1;
                }
            }
        }else if(position < 0){
            ArrayList<Integer> array = new ArrayList<>();
            for(int i = 0; i<string.length()+position; i++){
                if(string.substring(i).matches(regex)){
                    if(array.size() == occurrence){
                        array.remove(0);
                    }
                    array.add(i+1);
                }
                if(array.size() == occurrence){
                    return array.get(0);
                }
            }
        }
        return 0;    // whatever case else, return 0.
    }

    // search the count of the occurrence that the regex matched start from the position of string,
    // and matchPara can indicate how to match.
    public static int regExpCount(String string, String regex, int position, String matchPara){
        if(position> string.length()|position<= 0|regex.length()== 0){
            return 0;
        }
        String[] updateString = applyMatchParameter(string.substring(position - 1), regex, matchPara);
        if(updateString == null){
            return 0;
        }
        return regExpCount(updateString[0], updateString[1]);
    }

    // apply match parameter to update string and regex.
    private static String[] applyMatchParameter(String string, String regex, String matchPara){
        boolean caseInsensitive = false;
        char c;
        for(int i = 0; i< matchPara.length(); i++){
            c = matchPara.charAt(i);
            switch (c){
                case 'i':
                    caseInsensitive = true;
                    break;
                case 'c':
                    caseInsensitive = false;
                    break;
                case 'n':
                    StringBuilder regexReplace = new StringBuilder();
                    regexReplace.append(regex.charAt(0));
                    for(int j = 1; j< regex.length(); j++){
                        if(regex.charAt(j)=='.' && regex.charAt(j-1)!='\\'){
                            regexReplace.append("[\\s|\\S]");
                            continue;
                        }
                        regexReplace.append(regex.charAt(j));
                    }
                    regex = regexReplace.toString();
                    break;
                case 'x':
                    regex = regex.replaceAll("\\s", "");
                    break;
                case 'm':
                    // ?
                    break;
                default:
                    return null;
            }
        }
        if(caseInsensitive){
            string = string.toUpperCase();
            regex = regex.toUpperCase();
        }
        return new String[]{string, regex};
    }

    // search the count of the occurrence that the regex matched from the string.
    private static int regExpCount(String string, String regex){
        int result = 0;
        Matcher m = createMatcher(regex, string);
        while(m.find()){
            result++;
        }
        return result;
    }

    // search the occurrence number of the regex start from the position of string,
    // and matchPara can indicate how to match, and return the position of result depending on returnOpt.
    public static int regExpInstr(String string, String regex, int position, int occurrence,
                                  int returnOpt, String matchPara, int subExpr){
        if(position> string.length()|position<= 0|regex.length()== 0){
            return 0;
        }
        String[] updateString = applyMatchParameter(string.substring(position - 1), regex, matchPara);
        if(updateString == null){
            return 0;
        }

        Matcher m = createMatcher(updateString[1], updateString[0]);
        int i;
        for(i = 0; i< occurrence; i++){
            if(!m.find())
                break;
        }
        int tempPosition;
        if(i == occurrence && returnOpt == 0){
            tempPosition = m.start()+1;
        }else if (i == occurrence && returnOpt == 1){
            tempPosition = m.start() + m.group().length()+1;
        }else {
            return 0;
        }
        return tempPosition+position-1;

    }

    // removes from the left or right or both end of string all of the characters contained in set.
    public static String stringTrim(String string, String set, String option){
        String regexLTrim = "^[" + set + "]*";
        String regexRTrim = "[" + set + "]*$";
        String regexTrim = "(^[" + set + "]*)|([" + set + "]*$)";
        if(option.equals("left")){
            return string.replaceAll(regexLTrim, "");
        }else if(option.equals("right")){
            return string.replaceAll(regexRTrim, "");
        }else if(option.equals("center")){
            return string.replaceAll(regexTrim, "");
        }else{
            return string;
        }
    }

    // search the occurrence number of the regex start from the position of string,
    // and matchPara can indicate how to match, and return the substring.
    public static String regExpSubStr(String string, String regex, int position,
                                      int occurrence,String matchPara, int subExpr){
        if(position> string.length()|position<= 0|regex.length()== 0){
            return "";
        }
        String[] updateString = applyMatchParameter(string.substring(position - 1), regex, matchPara);
        if(updateString == null){
            return "";
        }

        Matcher m = createMatcher(updateString[1], updateString[0]);
        int i;
        for(i = 0; i< occurrence; i++){
            if(!m.find())
                break;
        }
        if(i == occurrence && i!=0){
            return string.substring(position-1+ m.start(), position-1+ m.end());
        }
        return "";
    }

    // search the occurrence of the regex start from the position of string and replace it,
    // and matchPara can indicate how to match, and return the string that replaced.
    public static String regExpReplace(String string, String regex, String replace,
                                       int position, int occurrence,String matchPara){
        if(occurrence < 0|position<= 0){
            return null;
        }
        if(position> string.length()|regex.length()== 0){
            return string;
        }
        String[] updateString = applyMatchParameter(string.substring(position - 1), regex, matchPara);
        if(updateString == null){
            return null;
        }

        StringBuilder result = new StringBuilder();
        result.append(string.substring(0,position-1));

        Matcher m = createMatcher(updateString[1], updateString[0]);
        int i = 0;
        int lastEnd = position - 1;
        while(m.find()){
            i++;
            if(occurrence == 0){
                result.append(string.substring(lastEnd, m.start()+position-1)+replace);
                lastEnd = m.end()+position-1;
            }else if (occurrence == i){
                break;
            }
        }
        if(i == occurrence && i!= 0){
            result.append(string.substring(lastEnd, m.start()+position-1)+replace);
            lastEnd = m.end()+position-1;
        }
        result.append(string.substring(lastEnd));
        return result.toString();
    }
}
