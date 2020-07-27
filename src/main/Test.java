package main;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.util.Calendar;
import java.util.Date;

public class Test {

	public static void main(String[] args) {

//	    System.out.println(CalTime("2019-09-17 19:57", "2019-09-17 19:59"));
//		System.out.println(isWeekend("2018-03-04"));
//		System.out.println(201909021959L/10000);
		
		System.out.println("0002".split("\\|").length);
		
	}
	public static void getNextDay(String day){
		
	}
	
	public static String isWeekend(String bDate){
		try {
	        DateFormat format1 = new SimpleDateFormat("yyyy-MM-dd");
	        Date bdate = format1.parse(bDate);
	        Calendar cal = Calendar.getInstance();
	        cal.setTime(bdate);
	        if(cal.get(Calendar.DAY_OF_WEEK) == Calendar.SATURDAY || cal.get(Calendar.DAY_OF_WEEK) == Calendar.SUNDAY){
	            return "OK";
	        } else{
	            return "NO";
	        }
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.out.println("��������");
		}	
		return "error";
 }

    // ��������ʱ������Ϊ���ӡ�
    private static long CalTime(String time1, String time2) {
        DateFormat df = new SimpleDateFormat("yyyy-MM-dd hh:mm");
        long minutes = 0L;
        try {
            Date d1 = df.parse(time1);
            Date d2 = df.parse(time2);
            long diff = d2.getTime() - d1.getTime();// �����õ��Ĳ�ֵ��΢�뼶��
            minutes = diff / (1000 * 60);
        } catch (ParseException e) {
            System.out.println("��Ǹ��ʱ�����ڽ�������");
        }
        return minutes;
    }


}
