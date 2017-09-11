package org.apache.spark.sql.util;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class DateFormatTrans {

    private static String rung = "-";
    private static String blank = " ";
    private static String colon = ":";
    private static String point = ".";
    private static String nullStr = "";
    private static String[] arrayPatternRegex = {"^Y,YYY.*", "^YYYY.*", "^RRRR.*", "^RR.*",
            "^MM.*", "^MONTH.*", "^MON.*", "^DD.*", "^MI.*", "^SS.*", "^FF.*",
            "^HH24.*", "^HH12.*|^HH.*", "^P\\.M\\..*|^A\\.M\\..*", "^PM.*|^AM.*"};

    // convert the dateString into string date that spark can recognize depending on formatString.
    // for example: ("2017:/08..23", "YYYY:/MM..DD") -> "2017-08-23"
    public static String specifiedDateToSparkDate(String dateString, String formatString){

        dateString = dateString.toUpperCase();
        formatString = formatString.toUpperCase();

        ArrayList<String> patternName = parseTimePattern(formatString);
        String[] realTime = parseRealTime(dateString, patternName);
        return concatString(realTime[0], rung, realTime[1], rung, realTime[2]);
    }

    // convert the dateString into timestamp string that spark can recognize depending on formatString.
    // for example: ("2017:/08..23 11:12:13.1", "YYYY:/MM..DD HH:MI:SS.FF") -> "2017-08-23 11:12:13.1"
    public static String specifiedDateToSparkTimestamp(String dateString, String formatString){

        dateString = dateString.toUpperCase();
        formatString = formatString.toUpperCase();

        ArrayList<String> patternName = parseTimePattern(formatString);
        String[] realTime = parseRealTime(dateString, patternName);
        return concatString(realTime[0], rung, realTime[1], rung, realTime[2], blank, realTime[3], colon,
                realTime[4], colon, realTime[5], point, realTime[6]);
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
        Matcher m;

        char c;
        String currentStr;
        for(int i = 0; i< formatString.length(); i++){
            c = formatString.charAt(i);
            if(isDelimiter(c)){
                resultString.append(c);
                continue;
            }
            currentStr = upperFormatString.substring(i);
            if(currentStr.matches(arrayPatternRegex[0])){
                resultString.append(year.charAt(0)+","+year.substring(1));
                i += 4;
            }else if(currentStr.matches(arrayPatternRegex[1])){
                resultString.append(year);
                i += 3;
            }else if(currentStr.matches(arrayPatternRegex[2])){
                resultString.append(year);
                i += 3;
            }else if(currentStr.matches(arrayPatternRegex[3])){
                resultString.append(year.substring(2));
                i += 1;
            }else if(currentStr.matches(arrayPatternRegex[4])){
                resultString.append(month);
                i += 1;
            }else if(currentStr.matches(arrayPatternRegex[5])){
                resultString.append(monthTrans(month, "MM", "MONTH"));
                i += 4;
            }else if(currentStr.matches(arrayPatternRegex[6])){
                resultString.append(monthTrans(month, "MM", "MMM"));
                i += 2;
            }else if(currentStr.matches(arrayPatternRegex[7])){
                resultString.append(day);
                i += 1;
            }else if((m = createMatcher(regex, currentStr)).find()){
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

        // Timestamp format in spark is yyyy-mm-dd hh24:mm:ss.ff
        String year = dateString.substring(0,4);
        String month = dateString.substring(5,7);
        String day = dateString.substring(8,10);
        String hour = dateString.substring(11,13);
        String minute = dateString.substring(14,16);
        String second = dateString.substring(17,19);
        String milli = nullStr;
        if (dateString.length()>= 21){
            milli = dateString.substring(20);
        }

        String upperFormatString = formatString.toUpperCase();
        StringBuilder resultString = new StringBuilder();

        char c;
        boolean AM_PM = true;
        String currentStr;

        for(int i = 0; i< formatString.length(); i++){
            c = upperFormatString.charAt(i);
            if(isDelimiter(c)){
                resultString.append(c);
                continue;
            }
            currentStr = upperFormatString.substring(i);
            if(currentStr.matches(arrayPatternRegex[0])){
                resultString.append(year.charAt(0)+","+year.substring(1));
                i += 4;
            }else if(currentStr.matches(arrayPatternRegex[1])){
                resultString.append(year);
                i += 3;
            }else if(currentStr.matches(arrayPatternRegex[2])){
                resultString.append(year);
                i += 3;
            }else if(currentStr.matches(arrayPatternRegex[3])){
                resultString.append(year.substring(2));
                i += 1;
            }else if(currentStr.matches(arrayPatternRegex[4])){
                resultString.append(month);
                i += 1;
            }else if(currentStr.matches(arrayPatternRegex[5])){
                resultString.append(monthTrans(month, "MM", "MMMMM"));
                i += 4;
            }else if(currentStr.matches(arrayPatternRegex[6])){
                resultString.append(monthTrans(month, "MM", "MMM"));
                i += 2;
            }else if(currentStr.matches(arrayPatternRegex[7])){
                resultString.append(day);
                i += 1;
            }else if(currentStr.matches(arrayPatternRegex[8])){
                resultString.append(minute);
                i += 1;
            }else if(currentStr.matches(arrayPatternRegex[9])){
                resultString.append(second);
                i += 1;
            }else if(currentStr.matches(arrayPatternRegex[10])){
                resultString.append(milli);
                for(int j = 0; j< 9-milli.length();j++){
                    resultString.append('0');
                }
                i += 1;
            }else if(currentStr.matches(arrayPatternRegex[11])){
                resultString.append(hour);
                i += 3;
            }else if(currentStr.matches(arrayPatternRegex[12])){
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
            }else if(currentStr.matches(arrayPatternRegex[13])){
                if(AM_PM){
                    resultString.append("P.M.");
                }else{
                    resultString.append("A.M.");
                }
                i += 3;
            }else if(currentStr.matches(arrayPatternRegex[14])){
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

        String dsSqlFormatPattern = "^([+|-])?(\\d+) (\\d+):(\\d+):(\\d+)(\\.(\\d+))?$";
        String dsISOFormatPattern = "^([-])?P((\\d+)D)?(T((\\d+)H)?((\\d+)M)?((\\d+)(\\.(\\d+))?S)?)?$";

        Matcher m1 = createMatcher(dsSqlFormatPattern, oracleIntervalString);
        Matcher m2 = createMatcher(dsISOFormatPattern, oracleIntervalString);

        if(m1.matches()){
            return oracleIntervalString;
        }
        if(!m2.matches()){
            throw new IllegalArgumentException("Interval string does not match day-time format of 'd h:m:s.n':");
        }
        StringBuilder result = new StringBuilder();
        result.append(whichToChoose(m2.group(1), rung, nullStr));
        result.append(whichToChoose(m2.group(3), m2.group(3) + blank, "0 "));
        result.append(whichToChoose(m2.group(6), m2.group(6) + colon , "00:"));
        result.append(whichToChoose(m2.group(8), m2.group(8) + colon, "00:"));
        result.append(whichToChoose(m2.group(10), m2.group(10), "00"));
        result.append(whichToChoose(m2.group(12), point + m2.group(12), nullStr));
        return result.toString();
    }

    // convert ymInterval string that oracle can recognize into ymInterval string that spark can recognize.
    // for example: "-P9Y8M1DT2H3M4.5S" -> "-9-8"
    // for example: "-2-3" -> "-2-3"
    public static String oracleYMIntervalToSparkInterval(String oracleIntervalString){

        String ymSqlFormatPattern = "^([+|-])?(\\d+)-(\\d+)$";
        String ymISOFormatPattern = "^([-])?P((\\d+)Y)?((\\d+)M)?((\\d+)D)?(T((\\d+)H)?((\\d+)M)?((\\d+)(\\.(\\d+))?S)?)?$";

        Matcher m1 = createMatcher(ymSqlFormatPattern, oracleIntervalString);
        Matcher m2 = createMatcher(ymISOFormatPattern, oracleIntervalString);

        if(m1.matches()){
            return oracleIntervalString;
        }
        if(!m2.matches()){
            throw new IllegalArgumentException("Interval string does not match day-time format of 'd h:m:s.n':");
        }
        StringBuilder result = new StringBuilder();
        result.append(whichToChoose(m2.group(1), rung, nullStr));
        result.append(whichToChoose(m2.group(3), m2.group(3) + rung, "0-"));
        result.append(whichToChoose(m2.group(5), m2.group(5), "0"));
        return result.toString();
    }

    // create a matcher depending on regex and string.
    private static Matcher createMatcher(String regex, String waitToMatch){
        Pattern p1 = Pattern.compile(regex);
        return p1.matcher(waitToMatch);
    }

    // a method that if A not null, return B, null, return C.
    private static String whichToChoose(String A, String B, String C){
        if(A!= null){
            return B;
        }
        return C;
    }

    // using StringBuilder to concatenate String.
    private static String concatString(String... args){
        StringBuilder result = new StringBuilder();
        for(String arg: args){
            result.append(arg);
        }
        return result.toString();
    }

    // determine the input char is delimiter or not.
    private static boolean isDelimiter(char c){
        if(!((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c<= '9'))){
            return true;
        }
        return false;
    }

    //determine the input char is oracleDelimiter or not.
    private static boolean isOracleDelimiter(char c){
        if(c=='-' || c=='/' || c==',' || c=='.' || c==' ' || c==':' || c==';'){
            return true;
        }
        return false;
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

        String TimestampPattern = "^Y,YYY|^YYYY|^RRRR|^RR|^MM|^MONTH|^MON|^DD|^HH24|^HH12|^HH|^MI|^SS|^FF|^AM|^PM|^A.M.|^P.M.";
        Matcher m;

        int count = 0;                            // the count of continuous delimiter
        boolean lastIsDelimiter = false;          // the last character is delimiter or not
        char c;                                   // character that addressing currently

        // the for loop is used to parse timePattern from formatString
        for(int i = 0; i < formatString.length(); i++ ){
            c = formatString.charAt(i);         // read the current character
            if(isOracleDelimiter(c)){     // if current character is delimiter
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
                m = createMatcher(TimestampPattern, formatString.substring(i));
                if(!m.find() || patternName.contains(m.group())){        // if current start string not belong to any timePattern or the timePattern occurs repeated, error.
                    return null;
                }
                patternName.add(m.group());            // add the timePattern to pattern_name.
                i += m.group().length() - 1;
                continue;
            }
            return null;
        }
        return patternName;
    }

    // parse real time from a dateString depending on timePattern that parsed from formatString
    // for example: ("2017:/08..23", [YYYY, :/, MM, .., DD]) -> "2017-08-23"
    private static String[] parseRealTime(String dateString, ArrayList<String> patternName){

        // return the string date that spark can recognize:[year,month,day,hour,minute,second,milli]
        String[] resultArray = new String[7];

        int index = 0;                                // point to the index of currently addressed timePattern from the arrayList that parsed before
        int count = 0;                                // the count of continuous delimiter
        boolean lastIsDelimiter = false;              // the last character is delimiter or not
        char c;
        String patternHere;                         // record timePattern and its count

        // according to timePattern parsed from formatString, match every time from dateString in turn.
        for(int i= 0; i< dateString.length(); i++){
            c = dateString.charAt(i);
            if(isOracleDelimiter(c)){     // if c is oracle delimiter.
                count++;
                lastIsDelimiter = true;
                continue;
            }
            if(isDelimiter(c)){             // if c is delimiter but not oracle delimiter.
                return null;
            }
            patternHere = patternName.get(index);
            if (lastIsDelimiter) {                // ignore the current pattern if it is delimiter and the real count <= the pattern count, if not,error.
                if(!patternHere.matches("^Delimiter-[0-9]+")||count> Integer.parseInt(patternHere.split("-")[1])){
                    return null;
                }
                count = 0;
                lastIsDelimiter = false;
                patternHere = patternName.get(++index);
            }
            if(patternHere.equals("Y,YYY")){
                if(!dateString.substring(i,i+5).matches("[0-9],[0-9]{3}")){
                    return null;
                }
                resultArray[0] = dateString.substring(i,i+5).replace(",","");
                i += 4;
                index++;
            }else if(patternHere.equals("YYYY") | patternHere.equals("RRRR")){
                if(!dateString.substring(i,i+4).matches("[0-9]{4}")){
                    return null;
                }
                resultArray[0] = dateString.substring(i,i+4);
                i += 3;
                index++;
            }else if(patternHere.equals("RR")){
                String[] result = realTimeWhenRR(dateString, i);
                if(result == null){
                    return null;
                }
                resultArray[0] = result[0];
                i += Integer.parseInt(result[1]);
                index++;
            }else if(patternHere.equals("MM")){
                if(dateString.substring(i).matches("[1-9]")|dateString.substring(i).matches("^[1-9][^0-9].*")){
                    resultArray[1] = "0"+dateString.charAt(i);
                }else if(dateString.length() > (i+1) && dateString.substring(i,i+2).matches("00|01|02|03|04|05|06|07|08|09|10|11|12")){
                    resultArray[1] = dateString.substring(i,i+2);
                    i += 1;
                }else{
                    return null;
                }
                index++;
            }else if(patternHere.equals("MON")){
                resultArray[1] = monthTrans(dateString.substring(i, i+3), "MMM", "MM");
                i += 2;
                index++;
            }else if(patternHere.equals("MONTH")){
                String monthRegex = "(^JANUARY)|(^FEBRUARY)|(^MARCH)|(^APRIL" +
                        ")|(^MAY)|(^JUNE)|(^JULY)|(^AUGUST" +
                        ")|(^SEPTEMBER)|(^OCTOBER)|(^NOVEMBER)|(^DECEMBER)";
                Matcher m  = createMatcher(monthRegex, dateString.substring(i));
                if(!m.find()){
                    return null;
                }
                resultArray[1] = monthTrans(m.group(), "MMMMM", "MM");
                i += m.group().length()-1;
                index++;
            }else if(patternHere.equals("MI")){
                String[] result = realTimeWhenMI_SS_DD_HH24(dateString, i, 0, 60);
                if(result == null){
                    return null;
                }
                resultArray[4] = result[0];
                i += Integer.valueOf(result[1]);
                index++;
            }else if(patternHere.equals("SS")){
                String[] result = realTimeWhenMI_SS_DD_HH24(dateString, i, 0, 60);
                if(result == null){
                    return null;
                }
                resultArray[5] = result[0];
                i += Integer.valueOf(result[1]);
                index++;
            }else if(patternHere.equals("DD")){
                String[] result = realTimeWhenMI_SS_DD_HH24(dateString, i, 1, 32);
                if(result == null){
                    return null;
                }
                resultArray[2] = result[0];
                i += Integer.valueOf(result[1]);
                index++;
            }else if(patternHere.equals("HH24")){
                String[] result = realTimeWhenMI_SS_DD_HH24(dateString, i, 0, 24);
                if(result == null){
                    return null;
                }
                resultArray[3] = result[0];
                i += Integer.valueOf(result[1]);
                index++;
            }else if(patternHere.equals("FF")){
                String[] result = realTimeWhenFF(dateString, i);
                resultArray[6] = result[0];
                i += Integer.parseInt(result[1]);
                index++;
            }else if(patternHere.equals("HH12")||patternHere.equals("HH")){
                boolean isPM = (patternName.contains("PM-0")|patternName.contains("P.M.-0"));
                String[] result = realTimeWhenHH12OrHH(dateString, isPM, i);
                if(result == null){
                    return null;
                }
                resultArray[3] = result[0];
                i += Integer.parseInt(result[1]);
                index++;
            }else if(patternHere.equals("AM")|patternHere.equals("A.M.")|
                    patternHere.equals("PM")|patternHere.equals("P.M.")){
                int length = patternHere.length();
                if(!dateString.substring(i,i+length).equals(patternHere)){
                    return null;
                }
                i+=length-1;
                index++;
            }else{
                return null;
            }
        }

        resultArray = addDefaultTime(resultArray);
        if(resultArray == null){
            return null;
        }
        return resultArray;
    }

    // extract real time when time pattern is HH12 or HH.
    private static String[] realTimeWhenHH12OrHH(String dateString, boolean isPM, int i){

        String[] returnArray = new String[2];
        if(dateString.substring(i).matches("[1-9]")|dateString.substring(i).matches("^[1-9][^0-9].*")){
            if(isPM){
                returnArray[0] = nullStr + (Integer.parseInt(dateString.substring(i,i+1))+12);
            }else{
                returnArray[0] = "0"+dateString.charAt(i);
            }
            returnArray[1] = String.valueOf(0);
            return returnArray;
        }else if(dateString.length() > (i+1) && dateString.substring(i,i+2).matches("[0-9]{2}")){
            String cut_here = dateString.substring(i,i+2);
            if(Integer.parseInt(cut_here)>= 1 && Integer.parseInt(cut_here)<= 12){   // if the hour between 1 and 12 when HH12 or HH
                if(isPM){
                    if(Integer.parseInt(cut_here)==12){
                        returnArray[0] = cut_here;
                    } else {
                        returnArray[0] = (Integer.parseInt(cut_here) + 12)+nullStr;
                    }
                } else {
                    if(Integer.parseInt(cut_here)==12){
                        returnArray[0] = "00" ;
                    } else {
                        returnArray[0] = cut_here;
                    }
                }
                returnArray[1] = String.valueOf(1);
                return returnArray;
            } else {
                return null;
            }
        }else{
            return null;
        }
    }

    // extract real time when time pattern is FF
    private static String[] realTimeWhenFF(String dateString, int i){
        String[] returnArray = new String[2];
        Matcher m  = createMatcher("^[0-9]{0,9}", dateString.substring(i));
        m.find();
        String cutHere = m.group();
        int length = cutHere.length();
        if (length >= 6){
            returnArray[0] = cutHere.substring(0,6);
        }else if(length >= 0 && length < 6) {
            StringBuilder add0 = new StringBuilder();
            for(int j= 0; j< 6 - length; j++){
                add0.append('0');
            }
            returnArray[0] = cutHere + add0;
        }
        returnArray[1] = String.valueOf(length-1);
        return returnArray;
    }

    // extract real time when time pattern is MI or SS or DD or HH24.
    private static String[] realTimeWhenMI_SS_DD_HH24(String dateString, int i, int lower, int upper){
        String[] returnArray = new String[2];
        if(dateString.substring(i).matches("[0-9]")|dateString.substring(i).matches("^[0-9][^0-9].*")){
            returnArray[0] = "0"+dateString.charAt(i);
            returnArray[1] = String.valueOf(0);
        }else if(dateString.length() > (i+1) && dateString.substring(i,i+2).matches("[0-9]{2}")){
            int num = Integer.parseInt(dateString.substring(i,i+2));
            if(num<lower && num>=upper){
                return null;
            }
            returnArray[0] = dateString.substring(i,i+2);
            returnArray[1] = String.valueOf(1);
        }else{
            return null;
        }
        return returnArray;
    }

    //extract real time when time pattern is RR.
    private static String[] realTimeWhenRR(String dateString, int i){
        String[] returnArray = new String[2];
        if(dateString.length() <= (i+1) && dateString.substring(i,i+1).matches("[0-9]")){      // 年份在字符串的末尾，且是单个字符
            returnArray[0] = "201"+dateString.charAt(i);
            returnArray[1] = String.valueOf(0);
        }else if(dateString.length() > (i+1) && dateString.substring(i,i+2).matches("[0-9]{2}")){
            int year2 = Integer.parseInt(dateString.substring(i,i+2));
            if(year2>=0 &&year2<= 49){
                returnArray[0] = "20"+dateString.substring(i,i+2);
            }else{
                returnArray[0] = "19"+dateString.substring(i,i+2);
            }
            returnArray[1] = String.valueOf(1);
        }else{
            return null;
        }
        return returnArray;
    }

    // add default real time of time pattern that not specified.
    private static String[] addDefaultTime(String[] resultArray){
        Calendar cal = Calendar.getInstance();       // 当未给定某一种时间时，要给出默认值，默认值为：（当前年-当前月-该月一号）
        int month = cal.get(Calendar.MONTH) + 1;    // month为当前月份
        int year = cal.get(Calendar.YEAR);          // year为当前年份
        if((resultArray[0]==null) &&(resultArray[1]==null) &&(resultArray[2]==null) &&(resultArray[3]==null) &&
                (resultArray[4]==null) &&(resultArray[5]==null) &&(resultArray[6]==null)){       // 如果所有时间都未匹配，则报错
            return null;
        }                            // 如果匹配了至少一个时间，则其他未匹配的时间都给出默认值
        if(resultArray[0]==null){
            resultArray[0] = nullStr + year;
        }
        if(resultArray[1]==null){
            if(month< 10){
                resultArray[1] = "0" + month;
            } else {
                resultArray[1] = nullStr + month;
            }
        }
        if(resultArray[2]==null){
            resultArray[2] = "01";
        }
        for(int i = 3; i<= 5; i++){
            if(resultArray[i]== null){
                resultArray[i] = "00";
            }
        }
        if(resultArray[6]==null){
            resultArray[6] = "000000";
        }
        return resultArray;
    }
}
