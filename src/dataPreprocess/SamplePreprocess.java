package dataPreprocess;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

public class SamplePreprocess {
	//2019年9,11,12三个月中节假日以及节假日的前一天and后一天
	public static final HashSet<String> holiday = new HashSet<String>() {{
		add("2019-09-12");
		add("2019-09-13");
		add("2019-09-16");
		add("2019-09-25");
		add("2019-09-31");
	}}; 
	//排除0:00-6:00和22:00-24:00共8个小时的数据
	public static final HashSet<String> otherHour = new HashSet<String>() {{
		add("00");add("01");add("02");add("03");
		add("04");add("05");add("22");add("23");
	}}; 
	//只要早晚高峰时期数据（为保证灵活性，分别往前往后各扩一个小时），共8个小时
	public static final HashSet<String> peakHour = new HashSet<String>() {{
		add("06");add("07");add("08");add("09");
		add("16");add("17");add("18");add("19");
	}};

	
	
	public static void main(String[] args) {
		String orderedTjamOriPath = "E:\\G-1149\\trafficCongestion\\训练数据\\时间顺序文件";
		String orderedByLinkPath = "E:\\G-1149\\trafficCongestion\\训练数据\\时间顺序文件(按照link分类)";
		String peakSamplePath = "E:\\G-1149\\trafficCongestion\\训练数据\\peakSample.csv";
		String filledSamplePath = "E:\\G-1149\\trafficCongestion\\训练数据\\filledSample.csv";
		String tempPath = "E:\\G-1149\\trafficCongestion\\训练数据\\temp.csv";
		
		//mergeDataByLink(orderedTjamOriPath, orderedByLinkPath);
		//getPeakSample(orderedByLinkPath, peakSamplePath);
		//fillData(peakSamplePath, filledSamplePath);
		checkData(filledSamplePath, tempPath);
		
		//System.out.println(subTimeByMin(formatTime("201909260959"), formatTime("201909261600")));
	}
	
	//看看缺失数据有多少
	public static void checkData(String inPath, String outPath){
		try {
			InputStreamReader inStream = new InputStreamReader(new FileInputStream(inPath), "UTF-8");
			BufferedReader reader = new BufferedReader(inStream);
			OutputStreamWriter writerStream = new OutputStreamWriter(new FileOutputStream(outPath), "utf-8");
			BufferedWriter writer = new BufferedWriter(writerStream);
			
			int qualified = 0;
			int unqua = 0;
			int sum = 480;
			int count = 1;
			String preLine = reader.readLine();
			String preLink = preLine.split(",")[0];
			Long preTime = Long.parseLong(preLine.split(",")[1]);

			String line = "";
			String[] lineArray;
			String rec = preLine + "\n";
			while ((line = reader.readLine()) != null) {
				lineArray = line.split(",");
				String linkID = lineArray[0].trim();
				String status = lineArray[2].trim();
				long time = Long.parseLong(lineArray[1].trim());
				rec += line+"\n";
				
				if(linkID.equals(preLink) && (time/10000)==(preTime/10000)){
					count++;
				}else {
					if(count==sum){
						qualified++;
					}
					else{
						unqua++;
						writer.write(rec);		
					}
					//System.out.println(count);
					rec = "";
					count = 1;
				}
				
				preLine = line;
				preLink = linkID;
				preTime = time;
			}
			reader.close();
			writer.flush();
			writer.close();
			System.out.println(qualified);
			System.out.println(unqua);
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		System.out.println("******************数据处理完毕*************");
	}
	
	/**
	 * 将已经排好序的原始tjam文件（以月为单位），按照link重新组织
	 * 踢掉时间范围在0:00-6:00的数据记录
	 * 踢掉周六周日的数据记录
	 * @param inPath
	 * @param outPath
	 */
	public static void mergeDataByLink(String inPath, String outPath){	
		File file = new File(inPath);
		List<String> list = Arrays.asList(file.list());	
		try {
			
			for (int i = 0; i < list.size(); i++) {
				//依次处理每一个文件
				HashMap<String, ArrayList<String>> tjamMap = new HashMap<>();
				
				String path = inPath + "/" + list.get(i);
				InputStreamReader inStream = new InputStreamReader(new FileInputStream(path), "UTF-8");
				BufferedReader reader = new BufferedReader(inStream);

				String line = "";
				String[] lineArray;
					
				while ((line = reader.readLine()) != null) {
					lineArray = line.split(",");
					String linkID = lineArray[2].trim();
					String status = lineArray[7].trim();
					String time = lineArray[14].trim();
					
					String day = formatTime(time).substring(0,10);

					//排除0:00-6:00和22:00-24:00共8个小时的数据
					String hour = time.substring(8,10);
					if(otherHour.contains(hour))continue;
					//剔除周六周日的记录
					if(isWeekend(day)) continue;
					//剔除节假日的数据记录
					if(holiday.contains(day))continue;

					if(tjamMap.containsKey(linkID)){
						ArrayList<String> temp = tjamMap.get(linkID);
						temp.add(time + "," + status);
						tjamMap.put(linkID, temp);
					}else {
						ArrayList<String> temp = new ArrayList<>();
						temp.add(time + "," + status);
						tjamMap.put(linkID, temp);
					}
				}
				
				String resPath = outPath + "/" + list.get(i);
				OutputStreamWriter writerStream = new OutputStreamWriter(new FileOutputStream(resPath), "utf-8");
				BufferedWriter writer = new BufferedWriter(writerStream);
				
				int max = 0;
				String preLine = "";
				for(String key: tjamMap.keySet()){
					ArrayList<String> temp = tjamMap.get(key);
					long preKey = Long.parseLong(temp.get(0).split(",")[0]);
					
					for(int j = 0; j < temp.size(); j++){
						long cur = Long.parseLong(temp.get(j).split(",")[0]);
						if((max < cur-preKey) && (cur/100==preKey/100)){
							max = (int) (cur-preKey);
							preLine = key + "," + temp.get(j);
						}
//						if((cur-preKey) > 10 && (cur/100==preKey/100)){
//							System.out.println(preLine);
//							System.out.println(key + "," + temp.get(j));
//							System.out.println(cur + " " + preKey);
//						}
						writer.write(key + "," + temp.get(j) + "\n");
						//preLine = key + "," + temp.get(j);
						preKey = cur;
					}
				}
				System.out.println(max);
				System.out.println(preLine);
				System.out.println(resPath + " write finish!!");
				writer.flush();
				writer.close();
				reader.close();
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		System.out.println("******************数据处理完毕*************");
	}
	
	/**
	 * 将早晚高峰时期数据提取出来，降低数据特征
	 * @param inPath
	 * @param outPath
	 */
	public static void getPeakSample(String inPath, String outPath){
		File file = new File(inPath);
		List<String> list = Arrays.asList(file.list());	
		try {
			OutputStreamWriter writerStream = new OutputStreamWriter(new FileOutputStream(outPath), "utf-8");
			BufferedWriter writer = new BufferedWriter(writerStream);
			
			for (int i = 0; i < list.size(); i++) {
				//依次处理每一个文件
				
				String path = inPath + "/" + list.get(i);
				InputStreamReader inStream = new InputStreamReader(new FileInputStream(path), "UTF-8");
				BufferedReader reader = new BufferedReader(inStream);

				int max = 0;
				long preKey = 201909020603L;
				String preLink = "85387316";
				String preLine = "";
				
				String line = "";
				String[] lineArray;
				
				while ((line = reader.readLine()) != null) {
					lineArray = line.split(",");
					String linkID = lineArray[0].trim();
					String status = lineArray[2].trim();
					String time = lineArray[1].trim();
					
					String day = formatTime(time).substring(0,10);
					
					//只筛选高峰时期的数据
					String hour = time.substring(8,10);
					if(!peakHour.contains(hour))continue;
					
					long cur = Long.parseLong(time);
					if((max < cur-preKey) && (cur/100==preKey/100) && (linkID.equals(preLink))){
						max = (int) (cur-preKey);
						preLine = line;
					}
					preLink = linkID;
					preKey = cur;
					
					writer.write(line + "\n");
				}
				System.out.println(max);
				System.out.println(preLine);
				reader.close();
			}
			writer.flush();
			writer.close();
			System.out.println(outPath + " write finish!!");
		} catch (Exception e) {
			e.printStackTrace();
		}
		System.out.println("******************数据处理完毕*************");
	}
	
	/**
	 * 有些时段没有拥堵数据，对数据进行填充，供后期训练使用
	 * @param inPath
	 * @param outPath
	 */
	public static void fillData(String inPath, String outPath){
		try {
			InputStreamReader inStream = new InputStreamReader(new FileInputStream(inPath), "UTF-8");
			BufferedReader reader = new BufferedReader(inStream);
			OutputStreamWriter writerStream = new OutputStreamWriter(new FileOutputStream(outPath), "utf-8");
			BufferedWriter writer = new BufferedWriter(writerStream);
			
			//1.丢失数据十分钟及以内，两端拥堵等级相同，填充端点拥堵等级
			//2.丢失数据三分钟以内，两端拥堵等级不同，填充与相邻端点相同拥堵等级
			int firstRule = 15;
			int secondRule = 5;
			int diffMax = 360;//当差值超过360的时候，代表时间到了晚高峰时期
			String preLine = reader.readLine();
			String preLink = preLine.split(",")[0];
			String preTime = preLine.split(",")[1];
			String preStat = preLine.split(",")[2];
			
			writer.write(preLine + "\n");
			
			String line = "";
			String[] lineArray;
			while ((line = reader.readLine()) != null) {
				lineArray = line.split(",");
				String linkID = lineArray[0].trim();
				String status = lineArray[2].trim();
				String time = lineArray[1].trim();
				
				if(linkID.equals(preLink)){
					long miss = subTimeByMin(formatTime(preTime), formatTime(time));
					
					//rule1
					if(miss <= firstRule && status.equals(preStat)){
						long t = Long.parseLong(preTime) + 1;
						for(int i = 1; i < miss; i++){
							writer.write(linkID +"," + t + "," + status + "\n");
							t++;
						}
					}
					
					//rule2
					if(miss <= secondRule && !status.equals(preStat)){
						long t = Long.parseLong(preTime) + 1;
						String curStat = "";
						for(int i = 1; i < miss; i++){
							curStat = (i > miss/2) ? status:preStat;
							writer.write(linkID +"," + t + "," + curStat + "\n");
							t++;
						}
					}
				}
				
				preLine = line;
				preLink = linkID;
				preStat = status;
				preTime = time;
				writer.write(line + "\n");
			}
			reader.close();
			writer.flush();
			writer.close();
			System.out.println(outPath + " write finish!!");
		} catch (Exception e) {
			e.printStackTrace();
		}
		System.out.println("******************数据处理完毕*************");
	}
	
	//将时间格式化:201909141752 -> yyyy-MM-dd hh:mm
	public static String formatTime(String time){
		String t = time.substring(0,4)+"-"+time.substring(4,6)+"-"+time.substring(6,8)+" "
				+time.substring(8,10)+":"+time.substring(10);
		return t;
	}
	
    /**
     * 计算两个时间差，返回为分钟。
     * @param time1 起始时间
     * @param time2 结束时间
     * @return 分钟
     */
    private static long subTimeByMin(String time1, String time2) {
        DateFormat df = new SimpleDateFormat("yyyy-MM-dd hh:mm");
        long minutes = 0L;
        try {
            Date d1 = df.parse(time1);
            Date d2 = df.parse(time2);
            long diff = d2.getTime() - d1.getTime();// 这样得到的差值是微秒级别
            minutes = diff / (1000 * 60);
        } catch (ParseException e) {
            System.out.println("抱歉，时间日期解析出错。");
        }
        return minutes;
    }
    
    /**
     * 判断给定的日期是否是周末
     * @param bDate yyyy-MM-dd
     * @return 是周六或者周日返回true，否则false
     */
	public static boolean isWeekend(String bDate){
		try {
	        DateFormat format1 = new SimpleDateFormat("yyyy-MM-dd");
	        Date bdate = format1.parse(bDate);
	        Calendar cal = Calendar.getInstance();
	        cal.setTime(bdate);
	        if(cal.get(Calendar.DAY_OF_WEEK) == Calendar.SATURDAY || cal.get(Calendar.DAY_OF_WEEK) == Calendar.SUNDAY){
	            return true;
	        }
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.out.println("解析错误");
		}	
		return false;
	}

}
