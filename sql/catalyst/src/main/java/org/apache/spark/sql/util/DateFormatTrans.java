package org.apache.spark.sql.util;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class DateFormatTrans {

    // convert the dateString into string date that spark can recognize depending on formatString.
    // for example: ("2017:/08..23", "YYYY:/MM..DD") -> "2017-08-23"
    public static String specifiedDateToSparkDate(String dateString, String formatString){

        dateString = dateString.toUpperCase();         // Oracle don't distinguish uppercase and lowercase,
        formatString = formatString.toUpperCase();     // so convert dateString and formatString into uppercase uniformly

        ArrayList<String> patternName = parseTimePattern(formatString);
        String[] realTime = parseRealTime(dateString, patternName);
        return realTime[0] + "-" + realTime[1] + "-" + realTime[2];
    }

    // convert the dateString into timestamp string that spark can recognize depending on formatString.
    // for example: ("2017:/08..23 11:12:13.1", "YYYY:/MM..DD HH:MI:SS.FF") -> "2017-08-23 11:12:13.1"
    public static String specifiedDateToSparkTimestamp(String dateString, String formatString){

        dateString = dateString.toUpperCase();         // Oracle don't distinguish uppercase and lowercase,
        formatString = formatString.toUpperCase();     // so convert dateString and formatString into uppercase uniformly

        ArrayList<String> patternName = parseTimePattern(formatString);
        String[] realTime = parseRealTime(dateString, patternName);
        return realTime[0]+"-"+realTime[1]+"-"+realTime[2]+" "+realTime[3]+":"+realTime[4]+":"+realTime[5]+"."+realTime[6];
    }

    // convert spark date string into string depending on formatString
    // for example: ("2017-08-24", "YYYY:/MM..DD") ->  "2017:/08..24".
    public static String sparkDateToSpecifiedDate(String dateString, String formatString){

        String year = dateString.substring(0,4);
        String month = dateString.substring(5,7);
        String day = dateString.substring(8,10);

        String upperFormatString = formatString.toUpperCase();
        StringBuilder resultString = new StringBuilder();

        // the timePattern that should ignore.
        String regex = "^HH24|^HH12|^HH|^MI|^SS|^FF|^AM|^PM|^A.M.|^P.M.";
        Pattern p = Pattern.compile(regex);
        Matcher m;

        char c;
        String currentStr;
        for(int i = 0; i< formatString.length(); i++){
            c = formatString.charAt(i);
            if(!((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c<= '9'))){
                resultString.append(c);
                continue;
            }
            currentStr = upperFormatString.substring(i);
            if(currentStr.matches("^Y,YYY.*")){
                resultString.append(year.charAt(0)+","+year.substring(1));
                i += 4;
            }else if(currentStr.matches("^YYYY.*|^RRRR.*")){
                resultString.append(year);
                i += 3;
            }else if(currentStr.matches("^RR.*")){
                resultString.append(year.substring(2));
                i += 1;
            }else if(currentStr.matches("^MM.*")){
                resultString.append(month);
                i += 1;
            }else if(currentStr.matches("^MONTH.*")){
                resultString.append(monthTrans(month, "MM", "MONTH"));
                i += 4;
            }else if(currentStr.matches("^MON.*")){
                resultString.append(monthTrans(month, "MM", "MMM"));
                i += 2;
            }else if(currentStr.matches("^DD.*")){
                resultString.append(day);
                i += 1;
            }else if((m = p.matcher(currentStr)).find()){
                i += m.group().length() - 1;
            }else{
                return null;
            }
        }
        return resultString.toString();
    }

    // convert spark timestamp string into string depending on formatString
    // for example: ("2017-08-24 10:11:12.13", "YYYY:/MM..DD hh::mi::ss.ff") ->  "2017:/08..24 10::11::12.13".
    public static String sparkTimestampToSpecifiedDate(String dateString, String formatString){

        // spark中timestamp的格式为yyyy-mm-dd hh24:mm:ss.ff
        String year = dateString.substring(0,4);
        String month = dateString.substring(5,7);
        String day = dateString.substring(8,10);
        String hour = dateString.substring(11,13);
        String minute = dateString.substring(14,16);
        String second = dateString.substring(17,19);
        String milli = "";
        if (dateString.length()>= 21){
            milli = dateString.substring(20);
        }

        String upperFormatString = formatString.toUpperCase();
        StringBuilder resultString = new StringBuilder();

        char c;
        boolean AM_PM = true;
        String currentStr;

        for(int i = 0; i< formatString.length(); i++){
            c = formatString.charAt(i);
            if(!((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c<= '9'))){
                resultString.append(c);
                continue;
            }
            currentStr = upperFormatString.substring(i);
            if(currentStr.matches("^Y,YYY.*")){
                resultString.append(year.charAt(0)+","+year.substring(1));
                i += 4;
            }else if(currentStr.matches("^YYYY.*|^RRRR.*")){
                resultString.append(year);
                i += 3;
            }else if(currentStr.matches("^RR.*")){
                resultString.append(year.substring(2));
                i += 1;
            }else if(currentStr.matches("^MM.*")){
                resultString.append(month);
                i += 1;
            }else if(currentStr.matches("^MONTH.*")){
                resultString.append(monthTrans(month, "MM", "MMMMM"));
                i += 4;
            }else if(currentStr.matches("^MON.*")){
                resultString.append(monthTrans(month, "MM", "MMM"));
                i += 2;
            }else if(currentStr.matches("^DD.*")){
                resultString.append(day);
                i += 1;
            }else if(currentStr.matches("^MI.*")){
                resultString.append(minute);
                i += 1;
            }else if(currentStr.matches("^SS.*")){
                resultString.append(second);
                i += 1;
            }else if(currentStr.matches("^FF.*")){
                resultString.append(milli);
                for(int j = 0; j< 9-milli.length();j++){
                    resultString.append('0');
                }
                i += 1;
            }else if(currentStr.matches("^HH24.*")){
                resultString.append(hour);
                i += 3;
            }else if(currentStr.matches("^HH12.*|^HH.*")){
                int hour_int = Integer.parseInt(hour);
                if(hour_int == 0){
                    resultString.append("12");
                }else if(hour_int>= 1 && hour_int<= 12){
                    resultString.append(hour);
                    AM_PM = false;
                }else if(hour_int>= 13 && hour_int<= 21){
                    resultString.append("0"+(hour_int-12));
                }else{
                    resultString.append(hour_int-12);
                }
                if(currentStr.matches("^HH12.*")){
                    i += 3;
                }else{
                    i += 1;
                }
            }else if(currentStr.matches("^P\\.M\\..*|^A\\.M\\..*")){
                if(AM_PM){
                    resultString.append("P.M.");
                }else{
                    resultString.append("A.M.");
                }
                i += 3;
            }else if(currentStr.matches("^PM.*|^AM.*")){
                if(AM_PM){
                    resultString.append("PM");
                }else{
                    resultString.append("AM");
                }
                i += 1;
            }else{
                return null;
            }
        }
        return resultString.toString();
    }

    // convert dsInterval string that oracle can recognize into dsInterval string that spark can recognize.
    // for example: "-P1DT2H3M4.5S" -> "-1 2:3:4.5"
    // for example: "-1 2:3:4.5" -> "-1 2:3:4.5"
    public static String oracleDSIntervalToSparkInterval(String oracleIntervalString){

        Pattern sqlFormatPattern = Pattern.compile("^([+|-])?(\\d+) (\\d+):(\\d+):(\\d+)(\\.(\\d+))?$");
        Matcher m1 = sqlFormatPattern.matcher(oracleIntervalString);

        Pattern dsISOFormatPattern = Pattern.compile("^([-])?P((\\d+)D)?(T((\\d+)H)?((\\d+)M)?((\\d+)(\\.(\\d+))?S)?)?$");
        Matcher m2 = dsISOFormatPattern.matcher(oracleIntervalString);

        if(m1.matches()){
            return oracleIntervalString;
        }
        if(!m2.matches()){
            throw new IllegalArgumentException("Interval string does not match day-time format of 'd h:m:s.n':");
        }
        StringBuilder result = new StringBuilder();
        if(m2.group(1)!= null){
            result.append('-');
        }
        if(m2.group(3)!= null){
            result.append(m2.group(3)+' ');
        }else{
            result.append("0 ");
        }
        if(m2.group(6)!= null){
            result.append(m2.group(6)+':');
        }else{
            result.append("00:");
        }
        if(m2.group(8)!= null){
            result.append(m2.group(8)+':');
        }else{
            result.append("00:");
        }
        if(m2.group(10)!= null){
            result.append(m2.group(10));
        }else{
            result.append("00");
        }
        if(m2.group(12)!= null){
            result.append('.'+m2.group(10));
        }
        return result.toString();
    }

    // convert ymInterval string that oracle can recognize into ymInterval string that spark can recognize.
    // for example: "-P9Y8M1DT2H3M4.5S" -> "-9-8"
    // for example: "-2-3" -> "-2-3"
    public static String oracleYMIntervalToSparkInterval(String oracleIntervalString){

        Pattern sqlFormatPattern = Pattern.compile("^([+|-])?(\\d+)-(\\d+)$");
        Matcher m1 = sqlFormatPattern.matcher(oracleIntervalString);

        Pattern ymISOFormatPattern = Pattern.compile("^([-])?P((\\d+)Y)?((\\d+)M)?((\\d+)D)?(T((\\d+)H)?((\\d+)M)?((\\d+)(\\.(\\d+))?S)?)?$");
        Matcher m2 = ymISOFormatPattern.matcher(oracleIntervalString);

        if(m1.matches()){
            return oracleIntervalString;
        }
        if(!m2.matches()){
            throw new IllegalArgumentException("Interval string does not match day-time format of 'd h:m:s.n':");
        }
        StringBuilder result = new StringBuilder();
        if(m2.group(1)!= null){
            result.append('-');
        }
        if(m2.group(3)!= null){
            result.append(m2.group(3));
            result.append('-');
        }else{
            result.append("0-");
        }
        if(m2.group(5)!= null){
            result.append(m2.group(5));
        }else{
            result.append("0");
        }
        return result.toString();
    }

    // convert month from one type to other type
    // for example: ("02", "MM", "MMM") -> "Feb",    ("Feb", "MMM", "MMMMM") -> "Feburary".
    private static String monthTrans(String monthString, String fromFormat, String toFormat){
        SimpleDateFormat sdf = new SimpleDateFormat(fromFormat, Locale.US);
        Date date;
        try{
            date = sdf.parse(monthString);
        } catch (ParseException e){
            return null;
        }
        sdf = new SimpleDateFormat(toFormat, Locale.US);
        return sdf.format(date);
    }

    // parse timePattern from a formatString and return an ArrayList<String>;
    // for example: "YYYY:/MM..DD" -> [YYYY, :/, MM, .., DD]
    private static ArrayList<String> parseTimePattern(String formatString){

        ArrayList<String> patternName = new ArrayList<>();  // record every timePattern parsed in formatString

        String regex = "^Y,YYY|^YYYY|^RRRR|^RR|^MM|^MONTH|^MON|^DD|^HH24|^HH12|^HH|^MI|^SS|^FF|^AM|^PM|^A.M.|^P.M.";
        Pattern p1 = Pattern.compile(regex);
        Matcher m1;

        int count = 0;                            // the count of continuous delimiter
        boolean lastIsDelimiter = false;          // the last character is delimiter or not
        char c;                                   // character that addressing currently

        // the for loop is used to parse timePattern from formatString
        for(int i = 0; i < formatString.length(); i++ ){
            c = formatString.charAt(i);         // read the current character
            if(c=='-' || c=='/' || c==',' || c=='.' || c==' ' || c==':' || c==';'){     // if current character is delimiter
                count++;
                lastIsDelimiter = true;
                continue;
            }
            if((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z')){     // if current character is alphabet
                if(lastIsDelimiter){                                  // if last character is delimiter ,last timePattern is Delimiter,
                    patternName.add("Delimiter" + "-"+ count);       // and add to pattern_name with it's continuous count.
                    count = 0;
                    lastIsDelimiter = false;
                }
                m1 = p1.matcher(formatString.substring(i));
                if(!m1.find() || patternName.contains(m1.group()+"-0")){        // if current start string not belong to any timePattern or the timePattern occurs repeated, error.
                    return null;
                }
                patternName.add(m1.group()+"-0");            // add the timePattern to pattern_name
                i += m1.group().length() - 1;
                continue;
            }
            return null;
        }
        return patternName;
    }

    // parse real time from a dateString depending on timePattern that parsed from formatString
    // for example: ("2017:/08..23", [YYYY, :/, MM, .., DD]) -> "2017-08-23"
    private static String[] parseRealTime(String dateString, ArrayList<String> patternName){

        // return the string date that spark can recognize:result_year+"-"+result_month+"-"+result_day
        String[] resultArray = new String[7];

        int index = 0;                                // point to the index of currently addressed timePattern from the arrayList that parsed before
        int count = 0;                                // the count of continuous delimiter
        boolean lastIsDelimiter = false;              // the last character is delimiter or not
        char c;
        String[] patternHere;                         // record timePattern and its count

        // according to timePattern parsed from formatString, match every time from dateString in turn.
        for(int i= 0; i< dateString.length(); i++){
            c = dateString.charAt(i);
            if(c=='-' | c=='/' | c==','  | c=='.' | c==' ' | c==':' | c==';'){     // 当前字符是分界符的情况
                count++;
                lastIsDelimiter = true;
                continue;
            }
            if(!((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c<= '9'))){
                return null;
            }
            patternHere = patternName.get(index).split("-");
            if (lastIsDelimiter) {                                                  // 如果上一个字符是分隔符，那么当前待处理的时间模式应该是分隔符，且此处连续分隔符的个数要大于待处理分隔符的个数
                if(!patternHere[0].equals("Delimiter")||count> Integer.parseInt(patternHere[1])){
                    return null;
                }
                count = 0;
                lastIsDelimiter = false;
                index++;
                patternHere = patternName.get(index).split("-");
            }
            if(patternHere[0].equals("Y,YYY")){                     // 如果当前待处理的时间模式是Y,YYY，则从dateString的第i个字符开始匹配相应的内容
                if(!dateString.substring(i,i+5).matches("[0-9],[0-9]{3}")){  // 如果成功匹配，获取年份内容并处理i和index，进而匹配下一个时间模式
                    return null;
                }
                resultArray[0] = dateString.substring(i,i+5).replace(",","");
                i += 4;
                index++;
            }else if(patternHere[0].equals("YYYY") | patternHere[0].equals("RRRR")){
                if(!dateString.substring(i,i+4).matches("[0-9]{4}")){
                    return null;
                }
                resultArray[0] = dateString.substring(i,i+4);
                i += 3;
                index++;
            }else if(patternHere[0].equals("RR")){
                if(dateString.length() <= (i+1) && dateString.substring(i,i+1).matches("[0-9]")){      // 年份在字符串的末尾，且是单个字符
                    resultArray[0] = "201"+dateString.charAt(i);
                    index++;
                }else if(dateString.length() > (i+1) && dateString.substring(i,i+2).matches("[0-9]{2}")){
                    int year2 = Integer.parseInt(dateString.substring(i,i+2));
                    if(year2>=0 &&year2<= 49){
                        resultArray[0] = "20"+dateString.substring(i,i+2);
                    }else{
                        resultArray[0] = "19"+dateString.substring(i,i+2);
                    }
                    i += 1;
                    index++;
                }else{
                    return null;
                }
            }else if(patternHere[0].equals("MM")){
                if(dateString.substring(i).matches("[1-9]")|dateString.substring(i).matches("^[1-9][^0-9].*")){
                    resultArray[1] = "0"+dateString.charAt(i);
                }else if(dateString.length() > (i+1) && dateString.substring(i,i+2).matches("00|01|02|03|04|05|06|07|08|09|10|11|12")){
                    resultArray[1] = dateString.substring(i,i+2);
                    i += 1;
                }else{
                    return null;
                }
                index++;
            }else if(patternHere[0].equals("MON")){
                resultArray[1] = monthTrans(dateString.substring(i, i+3), "MMM", "MM");
                i += 2;
                index++;
            }else if(patternHere[0].equals("MONTH")){
                Pattern p = Pattern.compile("(^JANUARY)|(^FEBRUARY)|(^MARCH)|(^APRIL" +
                        ")|(^MAY)|(^JUNE)|(^JULY)|(^AUGUST" +
                        ")|(^SEPTEMBER)|(^OCTOBER)|(^NOVEMBER)|(^DECEMBER)");
                Matcher m  = p.matcher(dateString.substring(i));
                if(!m.find()){
                    return null;
                }
                resultArray[1] = monthTrans(m.group(), "MMMMM", "MM");
                i += m.group().length()-1;
                index++;
            }else if(patternHere[0].equals("DD")){
                if(dateString.substring(i).matches("[1-9]")|dateString.substring(i).matches("^[1-9][^0-9].*")){
                    resultArray[2] = "0"+dateString.charAt(i);
                }else if(dateString.length() > (i+1) && dateString.substring(i,i+2).matches("[0-9]{2}")){
                    int the_day = Integer.parseInt(dateString.substring(i,i+2));
                    if(the_day<=0 && the_day>=32){
                        return null;
                    }
                    resultArray[2] = dateString.substring(i,i+2);
                    i += 1;
                }else{
                    return null;
                }
                index++;
            }else if(patternHere[0].equals("MI")){
                if(dateString.substring(i).matches("[1-9]")|dateString.substring(i).matches("^[1-9][^0-9].*")){
                    resultArray[4] = "0"+dateString.charAt(i);
                }else if(dateString.length() > (i+1) && dateString.substring(i,i+2).matches("[0-9]{2}")){
                    int the_minute = Integer.parseInt(dateString.substring(i,i+2));
                    if(the_minute<0 && the_minute>=60){
                        return null;
                    }
                    resultArray[4] = dateString.substring(i,i+2);
                    i += 1;
                }else{
                    return null;
                }
                index++;
            }else if (patternHere[0].equals("SS")){
                if(dateString.substring(i).matches("[1-9]")|dateString.substring(i).matches("^[1-9][^0-9].*")){
                    resultArray[5] = "0"+dateString.charAt(i);
                }else if(dateString.length() > (i+1) && dateString.substring(i,i+2).matches("[0-9]{2}")){
                    int the_second = Integer.parseInt(dateString.substring(i,i+2));
                    if(the_second<0 && the_second>=60){
                        return null;
                    }
                    resultArray[5] = dateString.substring(i,i+2);
                    i += 1;
                }else{
                    return null;
                }
                index++;
            }else if(patternHere[0].equals("FF")){
                Pattern p = Pattern.compile("^[0-9]{0,9}");
                Matcher m  = p.matcher(dateString.substring(i));
                m.find();
                String cutHere = m.group();
                int length = cutHere.length();
                if (length >= 6){
                    resultArray[6] = cutHere.substring(0,6);
                }else if(length >= 0 && length < 6) {
                    StringBuilder add0 = new StringBuilder();
                    for(int j= 0; j< 6 - length; j++){
                        add0.append('0');
                    }
                    resultArray[6] = cutHere + add0;
                }
                i += length-1;
                index++;
            }else if(patternHere[0].equals("HH24")){
                if(dateString.substring(i).matches("[0-9]")|dateString.substring(i).matches("^[0-9][^0-9].*")){
                    resultArray[3] = "0"+dateString.charAt(i);
                }else if (dateString.length() > (i+1) && dateString.substring(i,i+2).matches("[0-9]{2}")){
                    String cut_here = dateString.substring(i,i+2);
                    if(Integer.parseInt(cut_here)< 0 && Integer.parseInt(cut_here)> 23){   // 当HH和HH12的情况下小时数要介于1到12之间
                        return null;
                    }
                    resultArray[3] = cut_here;
                    i += 1;
                } else {
                    return null;// 该处不是有效数字形式的小时，应报错
                }
                index++;
            }else if(patternHere[0].equals("HH12")||patternHere[0].equals("HH")){
                boolean isPM = (patternName.contains("PM-0")|patternName.contains("P.M.-0"));
                if(dateString.substring(i).matches("[1-9]")|dateString.substring(i).matches("^[1-9][^0-9].*")){
                    if(isPM){
                        resultArray[3] = ""+(Integer.parseInt(dateString.substring(i,i+1))+12);
                    }else{
                        resultArray[3] = "0"+dateString.charAt(i);
                    }
                }else if(dateString.length() > (i+1) && dateString.substring(i,i+2).matches("[0-9]{2}")){
                    String cut_here = dateString.substring(i,i+2);
                    if(Integer.parseInt(cut_here)>= 1 && Integer.parseInt(cut_here)<= 12){   // 当HH和HH12的情况下小时数要介于1到12之间
                        if(isPM){
                            if(Integer.parseInt(cut_here)==12){
                                resultArray[3] = cut_here;
                            } else {
                                resultArray[3] = (Integer.parseInt(cut_here) + 12)+"";
                            }
                        } else {
                            if(Integer.parseInt(cut_here)==12){
                                resultArray[3] = "00" ;
                            } else {
                                resultArray[3] = cut_here;
                            }
                        }
                        i += 1;
                    } else {
                        return null;    // 小时数无效应报错
                    }
                }else{
                    return null;
                }
                index++;
            }else if(patternHere[0].equals("AM")|patternHere[0].equals("A.M.")|
                    patternHere[0].equals("PM")|patternHere[0].equals("P.M.")){
                int length = patternHere[0].length();
                if(!dateString.substring(i,i+length).equals(patternHere[0])){
                    return null;
                }
                i+=length-1;
                index++;
            }else{
                return null;
            }
        }

        Calendar cal = Calendar.getInstance();       // 当未给定某一种时间时，要给出默认值，默认值为：（当前年-当前月-该月一号）
        int month = cal.get(Calendar.MONTH) + 1;    // month为当前月份
        int year = cal.get(Calendar.YEAR);          // year为当前年份
        if((resultArray[0]==null) &&(resultArray[1]==null) &&(resultArray[2]==null) &&(resultArray[3]==null) &&
                (resultArray[4]==null) &&(resultArray[5]==null) &&(resultArray[6]==null)){       // 如果所有时间都未匹配，则报错
            return null;
        }                            // 如果匹配了至少一个时间，则其他未匹配的时间都给出默认值
        if(resultArray[2]==null){
            resultArray[2] = "01";
        }
        if(resultArray[0]==null){
            resultArray[0] = ""+year;
        }
        if(resultArray[1]==null){
            if(month< 10){
                resultArray[1] = "0" + month;
            } else {
                resultArray[1] = "" + month;
            }
        }
        if(resultArray[3]==null){
            resultArray[3] = "00";
        }
        if(resultArray[4]==null){
            resultArray[4] = "00";
        }
        if(resultArray[5]==null){
            resultArray[5] = "00";
        }
        if(resultArray[6]==null){
            resultArray[6] = "000000";
        }
        // return real time of every timePattern, which packaged by an string array.
        return resultArray;
    }

}
