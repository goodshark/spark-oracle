package org.apache.spark.sql.util;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class DateTimeFunctions {

    // create a matcher depending on regex and string.
    private Matcher createMatcher(String regex, String waitToMatch) {
        Pattern p1 = Pattern.compile(regex);
        return p1.matcher(waitToMatch);
    }

    // return value of the specified unit in spark Date.
    public int unitValueOfDateString(String sparkDateString, String unit){
        // the format of Date in spark is YYYY-MM-DD.
        if(unit.equals("year")){
            return Integer.parseInt(sparkDateString.substring(0,4));
        }else if(unit.equals("month")){
            return Integer.parseInt(sparkDateString.substring(5,7));
        }else if(unit.equals("day")){
            return Integer.parseInt(sparkDateString.substring(8,10));
        }
        return -1;
    }

    // return value of the specified unit in spark Timestamp.
    public int unitValueOfTimestampString(String sparkTimestampString, String unit){
        // the format of Timestamp in spark is YYYY-MM-DD HH:MI:SS.FF.
        if(unit.equals("year")){
            return Integer.parseInt(sparkTimestampString.substring(0,4));
        }else if(unit.equals("month")){
            return Integer.parseInt(sparkTimestampString.substring(5,7));
        }else if(unit.equals("day")){
            return Integer.parseInt(sparkTimestampString.substring(8,10));
        }else if(unit.equals("hour")){
            return Integer.parseInt(sparkTimestampString.substring(11,13));
        }else if(unit.equals("minute")){
            return Integer.parseInt(sparkTimestampString.substring(14,16));
        }else if(unit.equals("second")){
            return Integer.parseInt(sparkTimestampString.substring(17,19));
        }
        return -1;
    }

    // return value of the specified unit in spark CalenderInterval.
    public int unitValueOfIntervalString(String sparkIntervalString, String unit){
        // the format of CalenderInterval in spark is "interval **** year ** month ** day ** hour ..........".
        StringBuilder regex = new StringBuilder();
        regex.append("[0-9]* ");
        regex.append(unit);
        regex.append('s');
        Matcher m = createMatcher(regex.toString(), sparkIntervalString);
        if(!m.find()){
            return -1;
        }
        if(!unit.equals("day")){
            return Integer.parseInt(m.group().split(" ")[0]);
        }

        StringBuilder regex2 = new StringBuilder();
        regex2.append("[0-9]* ");
        regex2.append("week");
        regex2.append('s');
        Matcher m2 = createMatcher(regex2.toString(), sparkIntervalString);
        if(!unit.equals("day")||!m2.find()){
            return Integer.parseInt(m.group().split(" ")[0]);
        }
        return Integer.parseInt(m.group().split(" ")[0]) +
                Integer.parseInt(m2.group().split(" ")[0]) * 7;
    }

    // round date depending on the specified unit, only allowed year、month、day.
    public String dateAfterRounded(String sparkDate, String unit){
        int year = Integer.parseInt(sparkDate.substring(0,4));
        int month = Integer.parseInt(sparkDate.substring(5,7));
        int day = Integer.parseInt(sparkDate.substring(8,10));
        StringBuilder result = new StringBuilder();
        if(unit.equals("year")|unit.equals("YYYY")){
            if(month>6){
                result.append(year+1);
                result.append("-01-01");
                return result.toString();
            }else{
                result.append(year);
                result.append("-01-01");
                return result.toString();
            }
        }else if(unit.equals("month")|unit.equals("mon")|unit.equals("mm")){
            if(day>15&& month == 12){
                result.append(year+1);
                result.append("-01-01");
                return result.toString();
            }else if(day>15){
                result.append(year);
                result.append('-');
                result.append(month+1);
                result.append("-01");
                return result.toString();
            }else{
                return sparkDate.substring(0, 8)+"01";
            }
        }else if(unit.equals("day")|unit.equals("dy")|unit.equals("d")){
            return roundDay(year, month, day);
        }
        return sparkDate;
    }

    // round date depending on the day unit. Actually, round day means round day in its week.
    private String roundDay(int year, int month, int day){
        SimpleDateFormat sdf1 = new SimpleDateFormat("yyyy-MM-dd", Locale.US);
        SimpleDateFormat sdf2 = new SimpleDateFormat("EEE", Locale.US);
        String[] weekEEE = {"Sun", "Mon","Tue", "Wed", "Thu", "Fri", "Sat"};
        Calendar myCalender = Calendar.getInstance();
        myCalender.set(year,month,day);
        Date date;
        try{
            date = sdf1.parse(year + "-" + month + "-" + day);
            String weekGet = sdf2.format(date);
            for(int i = 0; i< 7; i++){
                if(weekEEE[i].equals(weekGet)&& i<= 3){
                    myCalender.add(Calendar.DAY_OF_MONTH,-i);
                }else if(weekEEE[i].equals(weekGet)&& i> 3){
                    myCalender.add(Calendar.DAY_OF_MONTH,7-i);
                }
            }
            StringBuilder result = new StringBuilder();
            result.append(myCalender.get(Calendar.YEAR));
            result.append("-");
            result.append(myCalender.get(Calendar.MONTH));
            result.append("-");
            result.append(myCalender.get(Calendar.DAY_OF_MONTH));
            return result.toString();
        } catch (ParseException e){
            return null;
        }
    }
}
