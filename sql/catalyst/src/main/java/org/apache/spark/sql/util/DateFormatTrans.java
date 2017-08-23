package org.apache.spark.sql.util;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public final class DateFormatTrans {


    public static String specifiedDateToSparkDate(String dateString, String formatString){

        // 返回值为spark能够识别的日期格式： result_year+"-"+result_month+"-"+result_day
        String result_year = null;
        String result_month = null;
        String result_day = null;

        dateString = dateString.toUpperCase();         // Oracle不区分大小写，因此将输入字符串都转换成大写，方便匹配
        formatString = formatString.toUpperCase();

        ArrayList<String> pattern_name = new ArrayList<String>();  // 用于依次存放formatString中的每个时间模式

        String regex = "^Y,YYY|^YYYY|^RRRR|^RR|^MM|^MONTH|^MON|^DD|^HH24|^HH12|^HH|^MI|^SS|^AM|^PM|^A.M.|^P.M.";
        Pattern p_1 = Pattern.compile(regex);
        Matcher m_1;

        int count = 0;                            // 用于记录连续分界符的个数
        boolean lastIsDelimiter = false;          // 用于记录上一个字符是否是分界符
        char c;                                   // 用于记录当前处理的字符

        // 该for循环从formatString中解析出包含的时间模式
        for(int i = 0; i < formatString.length(); i++ ){
            c = formatString.charAt(i);         // 读取当前字符
            if(c=='-' | c=='/' | c==','  | c=='.' | c==' ' | c==':' | c==';'){     // 如果当前字符是分界符的情况
                count++;
                lastIsDelimiter = true;
                continue;
            }
            if((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z')){           // 当前字符是字母的时候
                if(lastIsDelimiter){                                              // 如果上一个字符是分界符，记录上一个时间模式为
                    pattern_name.add("Delimiter" + "-"+ count);                   // 分界符，count记录这个时间模式中分界符的个数
                    count = 0;
                    lastIsDelimiter = false;
                }
                m_1 = p_1.matcher(formatString.substring(i));
                if(!m_1.find()){                                // 如果当前字母不属于任何待匹配的时间模式，则报错
                    return null;
                }
                if(pattern_name.contains(m_1.group()+"-0")){    // 如果该时间模式没有重复出现，则记录，否则报错
                    return null;
                }
                pattern_name.add(m_1.group()+"-0");
                i += m_1.group().length() - 1;
                continue;
            }
            return null;
        }

        int index = 0;                                // 用于标记之前解析出的待处理的时间模式的索引值
        count = 0;                                    // 记录dateString中连续分隔符的个数
        lastIsDelimiter = false;                      // 记录上一个字符是否是分隔符

        // 根据从formatString解析出的时间模式从dateString中依次匹配
        for(int i= 0; i< dateString.length(); i++){
            c = dateString.charAt(i);
            if(c=='-' | c=='/' | c==','  | c=='.' | c==' ' | c==':' | c==';'){     // 当前字符是分界符的情况
                count++;
                lastIsDelimiter = true;
            }else if((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c<= '9')){    // 当前字符是字母的时候
                if (lastIsDelimiter) {                                                  // 如果上一个字符是分隔符，那么当前待处理的时间模式应该是分隔符，且此处连续分隔符的个数要大于待处理分隔符的个数
                    String[] pattern_here = pattern_name.get(index).split("-");
                    if(pattern_here[0].equals("Delimiter") && count<= Integer.parseInt(pattern_here[1])){
                        count = 0;
                        lastIsDelimiter = false;
                        index++;
                    }else{
                        return null;
                    }
                }

                String[] pattern_here = pattern_name.get(index).split("-");
                if(pattern_here[0].equals("Y,YYY")){                     // 如果当前待处理的时间模式是Y,YYY，则从dateString的第i个字符开始匹配相应的内容
                    if(dateString.substring(i,i+5).matches("[0-9],[0-9]{3}")){  // 如果成功匹配，获取年份内容并处理i和index，进而匹配下一个时间模式
                        result_year = dateString.substring(i,i+5).replace(",","");
                        i += 4;
                        index++;
                    }else{                                 // 如果匹配失败，则报错
                        return null;
                    }
                }else if(pattern_here[0].equals("YYYY") | pattern_here[0].equals("RRRR")){
                    if(dateString.substring(i,i+4).matches("[0-9]{4}")){
                        result_year = dateString.substring(i,i+4);
                        i += 3;
                        index++;
                    }else{
                        return null;
                    }
                }else if(pattern_here[0].equals("RR")){
                    if(dateString.length()<= (i+1)){            // 年份在字符串的末尾，且是单个字符
                        if(dateString.substring(i,i+1).matches("[0-9]")){
                            result_year = "201"+dateString.charAt(i);
                            index++;
                        } else {
                            return null;
                        }
                    } else if(dateString.substring(i,i+2).matches("[0-9]{2}")){
                        int year_2 = Integer.parseInt(dateString.substring(i,i+2));
                        if(year_2>=0 &&year_2<= 49){
                            result_year = "20"+dateString.substring(i,i+2);
                            i += 1;
                            index++;
                        }else{
                            result_year = "19"+dateString.substring(i,i+2);
                            i += 1;
                            index++;
                        }
                    }else{
                        return null;
                    }
                }else if(pattern_here[0].equals("MM")){
                    if(dateString.substring(i).matches("[1-9]")|dateString.substring(i).matches("^[1-9][^0-9].*")){
                        result_month = "0"+dateString.charAt(i);
                        index++;
                    } else if(dateString.substring(i,i+2).matches("00|01|02|03|04|05|06|07|08|09|10|11|12")){
                        result_month = dateString.substring(i,i+2);
                        i += 1;
                        index++;
                    }else{
                        return null;
                    }
                }else if(pattern_here[0].equals("MON")){
                    SimpleDateFormat sdf = new SimpleDateFormat("MMM", Locale.US);
                    Date date;
                    try{
                        date = sdf.parse(dateString.substring(i,i+3));
                    } catch (ParseException e){
                        return null;
                    }
                    sdf = new SimpleDateFormat("MM", Locale.US);
                    result_month = sdf.format(date);
                    i += 2;
                    index++;
                }else if(pattern_here[0].equals("MONTH")){
                    Pattern p = Pattern.compile("(^JANUARY)|(^FEBRUARY)|(^MARCH)|(^APRIL" +
                            ")|(^MAY)|(^JUNE)|(^JULY)|(^AUGUST" +
                            ")|(^SEPTEMBER)|(^OCTOBER)|(^NOVEMBER)|(^DECEMBER)");
                    Matcher m  = p.matcher(dateString.substring(i));
                    String cutHere = "";
                    if(m.find()){
                        cutHere += m.group();
                    }
                    int length = cutHere.length();
                    if (length > 0){
                        SimpleDateFormat sdf = new SimpleDateFormat("MMMMM", Locale.US);
                        Date date;
                        try{
                            date = sdf.parse(cutHere);
                        } catch (ParseException e){
                            return null;
                        }
                        sdf = new SimpleDateFormat("MM", Locale.US);
                        result_month = sdf.format(date);
                        i += length-1;
                        index++;
                    } else {
                        return null;
                    }
                }else if(pattern_here[0].equals("DD")){
                    if(dateString.substring(i).matches("[1-9]")|dateString.substring(i).matches("^[1-9][^0-9].*")){
                        result_day = "0"+dateString.charAt(i);
                        index++;
                    }else if(dateString.substring(i,i+2).matches("[0-9]{2}")){
                        int the_day = Integer.parseInt(dateString.substring(i,i+2));
                        if(the_day>0 && the_day<32){
                            result_day = dateString.substring(i,i+2);
                            i += 1;
                            index++;
                        }else{
                            return null;
                        }
                    }else{
                        return null;
                    }
                }else if(pattern_here[0].equals("MI")){
                    if(dateString.substring(i).matches("[1-9]")|dateString.substring(i).matches("^[1-9][^0-9].*")){
                        index++;
                    }else if(dateString.substring(i,i+2).matches("[0-9]{2}")){
                        int the_minute = Integer.parseInt(dateString.substring(i,i+2));
                        if(the_minute>=0 && the_minute<60){
                            i += 1;
                            index++;
                        }else{
                            return null;
                        }
                    }else{
                        return null;
                    }
                }else if (pattern_here[0].equals("SS")){
                    if(dateString.substring(i).matches("[1-9]")|dateString.substring(i).matches("^[1-9][^0-9].*")){
                        index++;
                    } else if(dateString.substring(i,i+2).matches("[0-9]{2}")){
                        int the_second = Integer.parseInt(dateString.substring(i,i+2));
                        if(the_second>=0 && the_second<60){
                            i += 1;
                            index++;
                        }else{
                            return null;
                        }
                    }else{
                        return null;
                    }
                }else if(pattern_here[0].equals("HH24")){
                    if(dateString.substring(i).matches("[0-9]")|dateString.substring(i).matches("^[0-9][^0-9].*")){
                        index++;
                    }else if (dateString.substring(i,i+2).matches("[0-9]{2}")){
                        String cut_here = dateString.substring(i,i+2);
                        if(Integer.parseInt(cut_here)>= 0 && Integer.parseInt(cut_here)<= 23){   // 当HH和HH12的情况下小时数要介于1到12之间
                            i += 1;
                            index++;
                        } else {
                            return null;    // 小时数无效应报错
                        }
                    } else {
                        return null;// 该处不是有效数字形式的小时，应报错
                    }
                }else if(pattern_here[0].equals("HH12")|pattern_here[0].equals("HH")){
                    boolean isPM = (pattern_name.contains("PM-0")|pattern_name.contains("P.M.-0"));
                    if(dateString.substring(i).matches("[1-9]")|dateString.substring(i).matches("^[1-9][^0-9].*")){
                        if(isPM){
                            index++;
                        } else {
                            index++;
                        }
                    }else if(dateString.substring(i,i+2).matches("[0-9]{2}")){
                        String cut_here = dateString.substring(i,i+2);
                        if(Integer.parseInt(cut_here)>= 1 && Integer.parseInt(cut_here)<= 12){   // 当HH和HH12的情况下小时数要介于1到12之间
                            if(isPM){
                                if(Integer.parseInt(cut_here)==12){
                                    i += 1;
                                    index++;
                                } else {
                                    i += 1;
                                    index++;
                                }
                            } else {
                                if(Integer.parseInt(cut_here)==12){
                                    i += 1;
                                    index++;
                                } else {
                                    i += 1;
                                    index++;
                                }
                            }
                        } else {
                            return null;    // 小时数无效应报错
                        }
                    }else{
                        return null;
                    }
                }else if(pattern_here[0].equals("AM")|pattern_here[0].equals("A.M.")|
                        pattern_here[0].equals("PM")|pattern_here[0].equals("P.M.")){
                    int length = pattern_here[0].length();
                    if(dateString.substring(i,i+length).equals(pattern_here[0])){
                        i+=length-1;
                        index++;
                    }else {
                        return null;
                    }
                }else{
                    return null;
                }
            }else {
                return null;
            }
        }

        Calendar cal = Calendar.getInstance();       // 当未给定某一种时间时，要给出默认值，默认值为：（当前年-当前月-该月一号）
        int month = cal.get(Calendar.MONTH) + 1;    // month为当前月份
        int year = cal.get(Calendar.YEAR);          // year为当前年份
        if((result_day==null) && (result_month==null) &&(result_year==null)){       // 如果所有时间都未匹配，则报错
            return null;
        }else{                              // 如果匹配了至少一个时间，则其他未匹配的时间都给出默认值
            if(result_day==null){
                result_day = "01";
            }
            if(result_year==null){
                result_year = ""+year;
            }
            if(result_month==null){
                if(month< 10){
                    result_month = "0" + month;
                } else {
                    result_month = "" + month;
                }
            }

        }
        // 返回sparksql能够识别的timestamp时间格式
        return result_year+"-"+result_month+"-"+result_day;
    }


    public static String sparkDateToSpecifiedDate(String dateString, String formatString){

        // spark中date的格式为yyyy-mm-dd
        String year = dateString.substring(0,4);
        String month = dateString.substring(5,7);
        String day = dateString.substring(8,10);

        String upperFormatString = formatString.toUpperCase();

        StringBuilder resultString = new StringBuilder();

        char c;

        for(int i = 0; i< formatString.length(); i++){
            c = formatString.charAt(i);
            if(!((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c<= '9'))){
                resultString.append(c);
            }else if(upperFormatString.substring(i).matches("^Y,YYY.*")){
                resultString.append(year.charAt(0));
                resultString.append(",");
                resultString.append(year.substring(1));
                i += 4;
            }else if(upperFormatString.substring(i).matches("^YYYY.*|^RRRR.*")){
                resultString.append(year);
                i += 3;
            }else if(upperFormatString.substring(i).matches("^RR.*")){
                resultString.append(year.substring(2));
                i += 1;
            }else if(upperFormatString.substring(i).matches("^MM.*")){
                resultString.append(month);
                i += 1;
            }else if(upperFormatString.substring(i).matches("^MONTH.*")){

                SimpleDateFormat sdf = new SimpleDateFormat("MM", Locale.US);
                Date date;
                try{
                    date = sdf.parse(month);
                } catch (ParseException e){
                    return null;
                }
                sdf = new SimpleDateFormat("MMMMM", Locale.US);
                resultString.append(sdf.format(date));
                i += 4;
            }else if(upperFormatString.substring(i).matches("^MON.*")){
                SimpleDateFormat sdf = new SimpleDateFormat("MM", Locale.US);
                Date date;
                try{
                    date = sdf.parse(month);
                } catch (ParseException e){
                    return null;
                }
                sdf = new SimpleDateFormat("MMM", Locale.US);
                resultString.append(sdf.format(date));
                i += 2;
            }else if(upperFormatString.substring(i).matches("^DD.*")){
                resultString.append(day);
                i += 1;
            }else if(upperFormatString.substring(i).matches("^MI.*")){
                i += 1;
            }else if(upperFormatString.substring(i).matches("^SS.*")){
                i += 1;
            }else if(upperFormatString.substring(i).matches("^FF.*")){
                i += 1;
            }else if(upperFormatString.substring(i).matches("^HH24.*")){
                i += 3;
            }else if(upperFormatString.substring(i).matches("^HH12.*|^HH.*")){
                if(upperFormatString.substring(i).matches("^HH12.*")){
                    i += 3;
                }else{
                    i += 1;
                }
            }else if(upperFormatString.substring(i).matches("^P\\.M\\..*|^A\\.M\\..*")){
                i += 3;
            }else if(upperFormatString.substring(i).matches("^PM.*|^AM.*")){
                i += 1;
            }else{
                return null;
            }
        }
        return resultString.toString();

    }


    public static String specifiedDateToSparkTimestamp(String dateString, String formatString){

        // 返回值为 result_year+"-"+result_month+"-"+result_day+" "+result_hour+":"+result_minute+":"+result_second+"."+result_milli;
        String result_year = null;
        String result_month = null;
        String result_day = null;
        String result_hour = null;
        String result_minute = null;
        String result_second = null;
        String result_milli = null;

        dateString = dateString.toUpperCase();         // Oracle不区分大小写，因此将输入字符串都转换成大写，方便匹配
        formatString = formatString.toUpperCase();

        ArrayList<String> pattern_name = new ArrayList<String>();  // 用于依次存放formatString中的每个时间模式

        String regex = "^Y,YYY|^YYYY|^RRRR|^RR|^MM|^MONTH|^MON|^DD|^HH24|^HH12|^HH|^MI|^SS|^FF|^AM|^PM|^A.M.|^P.M.";
        Pattern p_1 = Pattern.compile(regex);
        Matcher m_1;

        int count = 0;                            // 用于记录连续分界符的个数
        boolean lastIsDelimiter = false;          // 用于记录上一个字符是否是分界符
        char c;                                   // 用于记录当前处理的字符

        // 该for循环从formatString中解析出包含的时间模式
        for(int i = 0; i < formatString.length(); i++ ){
            c = formatString.charAt(i);         // 读取当前字符
            if(c=='-' | c=='/' | c==','  | c=='.' | c==' ' | c==':' | c==';'){     // 如果当前字符是分界符的情况
                count++;
                lastIsDelimiter = true;
            }else if((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z')){           // 当前字符是字母的时候
                if(lastIsDelimiter){                                              // 如果上一个字符是分界符，记录上一个时间模式为
                    pattern_name.add("Delimiter" + "-"+ count);                   // 分界符，count记录这个时间模式中分界符的个数
                    count = 0;
                    lastIsDelimiter = false;
                }
                m_1  = p_1.matcher(formatString.substring(i));
                String cutHere = "";
                if(m_1.find()){
                    cutHere += m_1.group();
                } else {
                    return null;
                }
                if(pattern_name.contains(cutHere + "-0")){                   // 同时如果该时间模式没有重复出现，则记录，否则报错
                    return null;
                }else {
                    pattern_name.add(cutHere + "-0");
                }
                i += cutHere.length() - 1;
            }else{
                return null;
            }
        }

        int index = 0;                                // 用于标记之前解析出的待处理的时间模式的索引值
        count = 0;                                    // 记录dateString中连续分隔符的个数
        lastIsDelimiter = false;                      // 记录上一个字符是否是分隔符

        // 根据从formatString解析出的时间模式从dateString中依次匹配
        for(int i= 0; i< dateString.length(); i++){
            c = dateString.charAt(i);
            if(c=='-' | c=='/' | c==','  | c=='.' | c==' ' | c==':' | c==';'){     // 当前字符是分界符的情况
                count++;
                lastIsDelimiter = true;
            }else if((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c<= '9')){    // 当前字符是字母的时候
                if (lastIsDelimiter) {                                                  // 如果上一个字符是分隔符，那么当前待处理的时间模式应该是分隔符，且此处连续分隔符的个数要大于待处理分隔符的个数
                    String[] pattern_here = pattern_name.get(index).split("-");
                    if(pattern_here[0].equals("Delimiter") && count<= Integer.parseInt(pattern_here[1])){
                        count = 0;
                        lastIsDelimiter = false;
                        index++;
                    }else{
                        return null;
                    }
                }

                String[] pattern_here = pattern_name.get(index).split("-");
                if(pattern_here[0].equals("Y,YYY")){                     // 如果当前待处理的时间模式是Y,YYY，则从dateString的第i个字符开始匹配相应的内容
                    if(dateString.substring(i,i+5).matches("[0-9],[0-9]{3}")){  // 如果成功匹配，获取年份内容并处理i和index，进而匹配下一个时间模式
                        result_year = dateString.substring(i,i+5).replace(",","");
                        i += 4;
                        index++;
                    }else{                                 // 如果匹配失败，则报错
                        return null;
                    }
                }else if(pattern_here[0].equals("YYYY") | pattern_here[0].equals("RRRR")){
                    if(dateString.substring(i,i+4).matches("[0-9]{4}")){
                        result_year = dateString.substring(i,i+4);
                        i += 3;
                        index++;
                    }else{
                        return null;
                    }
                }else if(pattern_here[0].equals("RR")){
                    if(dateString.length()<= (i+1)){            // 年份在字符串的末尾，且是单个字符
                        if(dateString.substring(i,i+1).matches("[0-9]")){
                            result_year = "201"+dateString.charAt(i);
                            index++;
                        } else {
                            return null;
                        }
                    } else if(dateString.substring(i,i+2).matches("[0-9]{2}")){
                        int year_2 = Integer.parseInt(dateString.substring(i,i+2));
                        if(year_2>=0 &&year_2<= 49){
                            result_year = "20"+dateString.substring(i,i+2);
                            i += 1;
                            index++;
                        }else{
                            result_year = "19"+dateString.substring(i,i+2);
                            i += 1;
                            index++;
                        }
                    }else{
                        return null;
                    }
                }else if(pattern_here[0].equals("MM")){
                    if(dateString.substring(i).matches("[1-9]")|dateString.substring(i).matches("^[1-9][^0-9].*")){
                        result_month = "0"+dateString.charAt(i);
                        index++;
                    } else if(dateString.substring(i,i+2).matches("00|01|02|03|04|05|06|07|08|09|10|11|12")){
                        result_month = dateString.substring(i,i+2);
                        i += 1;
                        index++;
                    }else{
                        return null;
                    }
                }else if(pattern_here[0].equals("MON")){
                    SimpleDateFormat sdf = new SimpleDateFormat("MMM", Locale.US);
                    Date date;
                    try{
                        date = sdf.parse(dateString.substring(i,i+3));
                    } catch (ParseException e){
                        return null;
                    }
                    sdf = new SimpleDateFormat("MM", Locale.US);
                    result_month = sdf.format(date);
                    i += 2;
                    index++;
                }else if(pattern_here[0].equals("MONTH")){
                    Pattern p = Pattern.compile("(^JANUARY)|(^FEBRUARY)|(^MARCH)|(^APRIL" +
                            ")|(^MAY)|(^JUNE)|(^JULY)|(^AUGUST" +
                            ")|(^SEPTEMBER)|(^OCTOBER)|(^NOVEMBER)|(^DECEMBER)");
                    Matcher m  = p.matcher(dateString.substring(i));
                    String cutHere = "";
                    if(m.find()){
                        cutHere += m.group();
                    }
                    int length = cutHere.length();
                    if (length > 0){
                        SimpleDateFormat sdf = new SimpleDateFormat("MMMMM", Locale.US);
                        Date date;
                        try{
                            date = sdf.parse(cutHere);
                        } catch (ParseException e){
                            return null;
                        }
                        sdf = new SimpleDateFormat("MM", Locale.US);
                        result_month = sdf.format(date);
                        i += length-1;
                        index++;
                    } else {
                        return null;
                    }
                }else if(pattern_here[0].equals("DD")){
                    if(dateString.substring(i).matches("[1-9]")|dateString.substring(i).matches("^[1-9][^0-9].*")){
                        result_day = "0"+dateString.charAt(i);
                        index++;
                    }else if(dateString.substring(i,i+2).matches("[0-9]{2}")){
                        int the_day = Integer.parseInt(dateString.substring(i,i+2));
                        if(the_day>0 && the_day<32){
                            result_day = dateString.substring(i,i+2);
                            i += 1;
                            index++;
                        }else{
                            return null;
                        }
                    }else{
                        return null;
                    }
                }else if(pattern_here[0].equals("MI")){
                    if(dateString.substring(i).matches("[1-9]")|dateString.substring(i).matches("^[1-9][^0-9].*")){
                        result_minute = "0"+dateString.charAt(i);
                        index++;
                    }else if(dateString.substring(i,i+2).matches("[0-9]{2}")){
                        int the_minute = Integer.parseInt(dateString.substring(i,i+2));
                        if(the_minute>=0 && the_minute<60){
                            result_minute = dateString.substring(i,i+2);
                            i += 1;
                            index++;
                        }else{
                            return null;
                        }
                    }else{
                        return null;
                    }
                }else if (pattern_here[0].equals("SS")){
                    if(dateString.substring(i).matches("[1-9]")|dateString.substring(i).matches("^[1-9][^0-9].*")){
                        result_second = "0"+dateString.charAt(i);
                        index++;
                    } else if(dateString.substring(i,i+2).matches("[0-9]{2}")){
                        int the_second = Integer.parseInt(dateString.substring(i,i+2));
                        if(the_second>=0 && the_second<60){
                            result_second = dateString.substring(i,i+2);
                            i += 1;
                            index++;
                        }else{
                            return null;
                        }
                    }else{
                        return null;
                    }
                }else if(pattern_here[0].equals("FF")){
                    Pattern p = Pattern.compile("^[0-9]{0,9}");
                    Matcher m  = p.matcher(dateString.substring(i));
                    String cutHere = "";
                    if(m.find()){
                        cutHere += m.group();
                    }
                    int length = cutHere.length();
                    if (length >= 6){
                        result_milli = cutHere.substring(0,6);
                        i += length-1;
                        index++;
                    } else if (length >= 0 && length < 6) {
                        StringBuilder add0 = new StringBuilder();
                        for(int j= 0; j< 6 - length; j++){
                            add0.append('0');
                        }
                        result_milli = cutHere + add0;
                        i += length-1;
                        index++;
                    }
                }else if(pattern_here[0].equals("HH24")){
                    if(dateString.substring(i).matches("[0-9]")|dateString.substring(i).matches("^[0-9][^0-9].*")){
                        result_hour = "0"+dateString.charAt(i);
                        index++;
                    }else if (dateString.substring(i,i+2).matches("[0-9]{2}")){
                        String cut_here = dateString.substring(i,i+2);
                        if(Integer.parseInt(cut_here)>= 0 && Integer.parseInt(cut_here)<= 23){   // 当HH和HH12的情况下小时数要介于1到12之间
                            result_hour = cut_here;
                            i += 1;
                            index++;
                        } else {
                            return null;    // 小时数无效应报错
                        }
                    } else {
                        return null;// 该处不是有效数字形式的小时，应报错
                    }
                }else if(pattern_here[0].equals("HH12")|pattern_here[0].equals("HH")){
                    boolean isPM = (pattern_name.contains("PM-0")|pattern_name.contains("P.M.-0"));
                    if(dateString.substring(i).matches("[1-9]")|dateString.substring(i).matches("^[1-9][^0-9].*")){
                        if(isPM){
                            result_hour = ""+(Integer.parseInt(dateString.substring(i,i+1))+12);
                            index++;
                        } else {
                            result_hour = "0"+dateString.charAt(i);
                            index++;
                        }
                    }else if(dateString.substring(i,i+2).matches("[0-9]{2}")){
                        String cut_here = dateString.substring(i,i+2);
                        if(Integer.parseInt(cut_here)>= 1 && Integer.parseInt(cut_here)<= 12){   // 当HH和HH12的情况下小时数要介于1到12之间
                            if(isPM){
                                if(Integer.parseInt(cut_here)==12){
                                    result_hour = cut_here;
                                    i += 1;
                                    index++;
                                } else {
                                    result_hour = (Integer.parseInt(cut_here) + 12)+"";
                                    i += 1;
                                    index++;
                                }
                            } else {
                                if(Integer.parseInt(cut_here)==12){
                                    result_hour = "00" ;
                                    i += 1;
                                    index++;
                                } else {
                                    result_hour = cut_here;
                                    i += 1;
                                    index++;
                                }
                            }
                        } else {
                            return null;    // 小时数无效应报错
                        }
                    }else{
                        return null;
                    }
                }else if(pattern_here[0].equals("AM")|pattern_here[0].equals("A.M.")|
                        pattern_here[0].equals("PM")|pattern_here[0].equals("P.M.")){
                    int length = pattern_here[0].length();
                    if(dateString.substring(i,i+length).equals(pattern_here[0])){
                        i+=length-1;
                        index++;
                    }else {
                        return null;
                    }
                }else{
                    return null;
                }
            }else {
                return null;
            }
        }

        Calendar cal = Calendar.getInstance();       // 当未给定某一种时间时，要给出默认值，默认值为：（当前年-当前月-该月一号）
        int month = cal.get(Calendar.MONTH) + 1;    // month为当前月份
        int year = cal.get(Calendar.YEAR);          // year为当前年份
        if((result_day==null) &&(result_hour==null) &&(result_milli==null) &&(result_minute==null) &&
                (result_month==null) &&(result_second==null) &&(result_year==null)){       // 如果所有时间都未匹配，则报错
            return null;
        }else{                              // 如果匹配了至少一个时间，则其他未匹配的时间都给出默认值
            if(result_day==null){
                result_day = "01";
            }
            if(result_year==null){
                result_year = ""+year;
            }
            if(result_month==null){
                if(month< 10){
                    result_month = "0" + month;
                } else {
                    result_month = "" + month;
                }
            }
            if(result_hour==null){
                result_hour = "00";
            }
            if(result_minute==null){
                result_minute = "00";
            }
            if(result_second==null){
                result_second = "00";
            }
            if(result_milli==null){
                result_milli = "000000";
            }
        }

        // 返回sparksql能够识别的timestamp时间格式
        return result_year+"-"+result_month+"-"+result_day+" "+result_hour+":"+result_minute+":"+result_second+"."+result_milli;
    }


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

        for(int i = 0; i< formatString.length(); i++){
            c = formatString.charAt(i);
            if(!((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c<= '9'))){
                resultString.append(c);
            }else if(upperFormatString.substring(i).matches("^Y,YYY.*")){
                resultString.append(year.charAt(0));
                resultString.append(",");
                resultString.append(year.substring(1));
                i += 4;
            }else if(upperFormatString.substring(i).matches("^YYYY.*|^RRRR.*")){
                resultString.append(year);
                i += 3;
            }else if(upperFormatString.substring(i).matches("^RR.*")){
                resultString.append(year.substring(2));
                i += 1;
            }else if(upperFormatString.substring(i).matches("^MM.*")){
                resultString.append(month);
                i += 1;
            }else if(upperFormatString.substring(i).matches("^MONTH.*")){

                SimpleDateFormat sdf = new SimpleDateFormat("MM", Locale.US);
                Date date;
                try{
                    date = sdf.parse(month);
                } catch (ParseException e){
                    return null;
                }
                sdf = new SimpleDateFormat("MMMMM", Locale.US);
                resultString.append(sdf.format(date));
                i += 4;
            }else if(upperFormatString.substring(i).matches("^MON.*")){
                SimpleDateFormat sdf = new SimpleDateFormat("MM", Locale.US);
                Date date;
                try{
                    date = sdf.parse(month);
                } catch (ParseException e){
                    return null;
                }
                sdf = new SimpleDateFormat("MMM", Locale.US);
                resultString.append(sdf.format(date));
                i += 2;
            }else if(upperFormatString.substring(i).matches("^DD.*")){
                resultString.append(day);
                i += 1;
            }else if(upperFormatString.substring(i).matches("^MI.*")){
                resultString.append(minute);
                i += 1;
            }else if(upperFormatString.substring(i).matches("^SS.*")){
                resultString.append(second);
                i += 1;
            }else if(upperFormatString.substring(i).matches("^FF.*")){
                resultString.append(milli);
                for(int j = 0; j< 9-milli.length();j++){
                    resultString.append('0');
                }
                i += 1;
            }else if(upperFormatString.substring(i).matches("^HH24.*")){
                resultString.append(hour);
                i += 3;
            }else if(upperFormatString.substring(i).matches("^HH12.*|^HH.*")){
                int hour_int = Integer.parseInt(hour);
                if(hour_int == 0){
                    resultString.append("12");
                }else if(hour_int>= 1 && hour_int<= 12){
                    resultString.append(hour);
                    AM_PM = false;
                }else if(hour_int>= 13 && hour_int<= 21){
                    resultString.append("0");
                    resultString.append((hour_int-12));
                }else{
                    resultString.append(hour_int-12);
                }
                if(upperFormatString.substring(i).matches("^HH12.*")){
                    i += 3;
                }else{
                    i += 1;
                }
            }else if(upperFormatString.substring(i).matches("^P\\.M\\..*|^A\\.M\\..*")){
                if(AM_PM){
                    resultString.append("P.M.");
                }else{
                    resultString.append("A.M.");
                }
                i += 3;
            }else if(upperFormatString.substring(i).matches("^PM.*|^AM.*")){
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


    public static String oracleDSIntervalToSparkInterval(String oracleIntervalString){

        Pattern sqlFormatPattern = Pattern.compile("^([+|-])?(\\d+) (\\d+):(\\d+):(\\d+)(\\.(\\d+))?$");
        Matcher m_1 = sqlFormatPattern.matcher(oracleIntervalString);

        Pattern dsISOFormatPattern = Pattern.compile("^([-])?P((\\d+)D)?(T((\\d+)H)?((\\d+)M)?((\\d+)(\\.(\\d+))?S)?)?$");
        Matcher m_2 = dsISOFormatPattern.matcher(oracleIntervalString);

        if (!m_1.matches()) {
            if(!m_2.matches()){
                throw new IllegalArgumentException(
                        "Interval string does not match day-time format of 'd h:m:s.n':");
            }else{
                StringBuilder result = new StringBuilder();
                if(m_2.group(1)!= null){
                    result.append('-');
                }
                if(m_2.group(3)!= null){
                    result.append(m_2.group(3));
                    result.append(' ');
                }else{
                    result.append("0 ");
                }
                if(m_2.group(6)!= null){
                    result.append(m_2.group(6));
                    result.append(':');
                }else{
                    result.append("00:");
                }
                if(m_2.group(8)!= null){
                    result.append(m_2.group(8));
                    result.append(':');
                }else{
                    result.append("00:");
                }
                if(m_2.group(10)!= null){
                    result.append(m_2.group(10));
                }else{
                    result.append("00");
                }
                if(m_2.group(12)!= null){
                    result.append('.');
                    result.append(m_2.group(10));
                }
                return result.toString();
            }
        } else {
            return oracleIntervalString;
        }
    }


    public static String oracleYMIntervalToSparkInterval(String oracleIntervalString){

        Pattern sqlFormatPattern = Pattern.compile("^([+|-])?(\\d+)-(\\d+)$");
        Matcher m_1 = sqlFormatPattern.matcher(oracleIntervalString);

        Pattern ymISOFormatPattern = Pattern.compile("^([-])?P((\\d+)Y)?((\\d+)M)?((\\d+)D)?(T((\\d+)H)?((\\d+)M)?((\\d+)(\\.(\\d+))?S)?)?$");
        Matcher m_2 = ymISOFormatPattern.matcher(oracleIntervalString);

        if (!m_1.matches()) {
            if(!m_2.matches()){
                throw new IllegalArgumentException(
                        "Interval string does not match day-time format of 'd h:m:s.n':");
            }else{
                StringBuilder result = new StringBuilder();
                if(m_2.group(1)!= null){
                    result.append('-');
                }
                if(m_2.group(3)!= null){
                    result.append(m_2.group(3));
                    result.append('-');
                }else{
                    result.append("0-");
                }
                if(m_2.group(5)!= null){
                    result.append(m_2.group(5));
                }else{
                    result.append("0");
                }
                return result.toString();
            }
        } else {
            return oracleIntervalString;
        }
    }


}
