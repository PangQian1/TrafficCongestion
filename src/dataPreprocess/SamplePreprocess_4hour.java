package dataPreprocess;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.text.DateFormat;
import java.text.DecimalFormat;
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

import javax.print.DocFlavor.STRING;

import org.omg.PortableServer.ServantActivator;
import org.w3c.dom.ls.LSInput;

public class SamplePreprocess_4hour {
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
		add("07");add("08");;add("17");add("18");
	}};

	
	
	public static void main(String[] args) {
		String orderedTjamOriPath = "E:\\G-1149\\trafficCongestion\\训练数据\\时间顺序文件";
		String orderedByLinkPath = "E:\\G-1149\\trafficCongestion\\训练数据\\时间顺序文件(按照link分类)";
		String peakSamplePath = "E:\\G-1149\\trafficCongestion\\训练数据\\4小时文件\\peakSample.csv";
		String filledInterSamplePath = "E:\\G-1149\\trafficCongestion\\训练数据\\4小时文件\\filledInterSample.csv";
		String filledEdgeSamplePath = "E:\\G-1149\\trafficCongestion\\训练数据\\4小时文件\\filledEdgeSample.csv";
		String resSamplePath = "E:\\G-1149\\trafficCongestion\\训练数据\\4小时文件\\resSample.csv";
		String tempPath = "E:\\G-1149\\trafficCongestion\\训练数据\\4小时文件\\temp.csv";
		
		String linkPeerPath = "E:\\G-1149\\trafficCongestion\\训练数据\\4小时文件\\trainData\\linkPeer.csv";
		String sample_2_Path = "E:\\G-1149\\trafficCongestion\\训练数据\\4小时文件\\trainData\\Sample_2.csv";
		String labelPath = "E:\\G-1149\\trafficCongestion\\训练数据\\4小时文件\\trainData\\label.csv";
		String sample10_2_Path = "E:\\G-1149\\trafficCongestion\\训练数据\\4小时文件\\trainData\\Sample10_2.csv";
		String sample10_1_Path = "E:\\G-1149\\trafficCongestion\\训练数据\\4小时文件\\trainData\\Sample10_1.csv";
		String sample15_2_Path = "E:\\G-1149\\trafficCongestion\\训练数据\\4小时文件\\trainData\\Sample15_2.csv";
		String sample15_1_Path = "E:\\G-1149\\trafficCongestion\\训练数据\\4小时文件\\trainData\\Sample15_1.csv";
		
		
		//mergeDataByLink(orderedTjamOriPath, orderedByLinkPath);
		//getPeakSample(orderedByLinkPath, peakSamplePath);
		//fillInternelData(peakSamplePath, filledInterSamplePath);
		//fillEdgeData(filledInterSamplePath, filledEdgeSamplePath);
		//fillSameCycleData(filledEdgeSamplePath, resSamplePath);
		//checkData(resSamplePath, tempPath);
		generateTrainableSample(resSamplePath, linkPeerPath, sample_2_Path, labelPath);
		//MergeByMin(sample_2_Path, sample10_1_Path, sample10_2_Path, 10);
		
		//System.out.println(subTimeByMin(formatTime("201909260959"), formatTime("201909261600")));
		//System.out.println(getNextMin("201909230659"));
		//System.out.println(getPreWeek("201909230659"));
	}
	
	/**
	 * 本来一天的数据是480个数值，为了降低复杂度，将一定时间粒度的数据合并，取算术平均值
	 * @param inPath 原始文件
	 * @param outPath_1  合并数据后的文件(一行对应一个样本，用来训练模型)
 	 * @param outPath_2  合并数据后的文件(两行对应一个样本，用来人工分析)
	 * @param grainSize 时间粒度
	 */
	public static void MergeByMin(String inPath, String outPath_1, String outPath_2, int grainSize){
		if(60 % grainSize != 0){
			System.out.println("传入的时间粒度不正确");
			return;
		}
		 DecimalFormat df = new DecimalFormat("#.00");
		try {
			InputStreamReader inStream = new InputStreamReader(new FileInputStream(inPath), "UTF-8");
			BufferedReader reader = new BufferedReader(inStream);
			OutputStreamWriter writerStream_2 = new OutputStreamWriter(new FileOutputStream(outPath_2), "utf-8");
			BufferedWriter writer_2 = new BufferedWriter(writerStream_2);
			OutputStreamWriter writerStream_1 = new OutputStreamWriter(new FileOutputStream(outPath_1), "utf-8");
			BufferedWriter writer_1 = new BufferedWriter(writerStream_1);

			String line = "";
			String[] lineArr;
			int lineNum = 0;
			while ((line = reader.readLine()) != null) {
				lineNum++;
				lineArr = line.split(",");
				int count = 0;
				double sum = 0;
				for(int i = 0; i < lineArr.length; i++){
					count++;
					sum += Double.parseDouble(lineArr[i]);
					if(count/grainSize == 1){
						String res = df.format(sum/grainSize);
						if(i == lineArr.length-1){
							writer_2.write(res + "\n");
							if(lineNum % 2 == 0) writer_1.write(res + "\n");
							else writer_1.write(res + ",");
						}
						else{
							writer_1.write(res + ",");
							writer_2.write(res + ",");
						}
						count = 0;
						sum = 0;
					}
				}
			}
			
			reader.close();
			writer_1.flush();
			writer_1.close();
			writer_2.flush();
			writer_2.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
		System.out.println("******************数据读取完毕*************");
	}

	public static void generateTrainableSample(String resSamplePath, String linkPeerPath, String samplePath, String labelPath){
		try {
			OutputStreamWriter sampleWriterStream = new OutputStreamWriter(new FileOutputStream(samplePath), "utf-8");
			BufferedWriter sampleWriter = new BufferedWriter(sampleWriterStream);
			OutputStreamWriter labelWriterStream = new OutputStreamWriter(new FileOutputStream(labelPath), "utf-8");
			BufferedWriter labelWriter = new BufferedWriter(labelWriterStream);
			
			//<linkID,<day,statusList>>
			HashMap<String, HashMap<String,ArrayList<String>>> map = getSampleMap(resSamplePath);
			ArrayList<String> linkPeerList = getLinkPeerList(linkPeerPath);//前40正样本，后31负样本
			
			int statusNum = 240;
			for(int i = 0; i < linkPeerList.size(); i++){
				String[] peer = linkPeerList.get(i).split(",");
				if(!map.containsKey(peer[0]) || !map.containsKey(peer[1])){
					System.out.println(peer[0] + "," + peer[1]);
					continue;
				}
				HashMap<String, ArrayList<String>> dayMap_1 = map.get(peer[0]);
				HashMap<String, ArrayList<String>> dayMap_2 = map.get(peer[1]);
				for(String day: dayMap_1.keySet()){
					if(dayMap_1.get(day).size()==statusNum && dayMap_2.get(day).size()==statusNum){
						String s1 = "";
						String s2 = "";
						for(int j = 0; j < statusNum-1; j++){
							s1 += dayMap_1.get(day).get(j) + ",";
							s2 += dayMap_2.get(day).get(j) + ",";
						}
						s1 += dayMap_1.get(day).get(statusNum-1);
						s2 += dayMap_2.get(day).get(statusNum-1);
						//sampleWriter.write(peer[0]+","+day+"," + s1 + "\n");
						//sampleWriter.write(peer[1]+","+day+"," + s2 + "\n");
						sampleWriter.write(s1 + "\n");
						sampleWriter.write(s2 + "\n");
						
						if(i < 40){
							labelWriter.write("1\n");
						}else{
							labelWriter.write("0\n");
						}
					}
				}
			}
			
			sampleWriter.flush();
			sampleWriter.close();
			labelWriter.flush();
			labelWriter.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
		System.out.println("******************数据处理完毕*************");
	}
	
	
	public static ArrayList<String> getLinkPeerList(String path){
		ArrayList<String> linkPeerList = new ArrayList<>();
		try {
			InputStreamReader inStream = new InputStreamReader(new FileInputStream(path), "UTF-8");
			BufferedReader reader = new BufferedReader(inStream);

			String line = "";
			while ((line = reader.readLine()) != null) {
				linkPeerList.add(line.trim());
			}
			
			reader.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
		System.out.println("******************" + path + "读取完毕*************");
		return linkPeerList;
	}
	
	
	public static HashMap<String, HashMap<String,ArrayList<String>>> getSampleMap(String path){
		HashMap<String, HashMap<String,ArrayList<String>>> map = new HashMap<>();//<linkID,<day,statusList>>
		try {
			InputStreamReader inStream = new InputStreamReader(new FileInputStream(path), "UTF-8");
			BufferedReader reader = new BufferedReader(inStream);

			String line = "";
			String[] lineArray;
			while ((line = reader.readLine()) != null) {
				lineArray = line.split(",");
				String linkID = lineArray[0].trim();
				String day = lineArray[1].trim().substring(0,8);
				String status = lineArray[2].trim();
				
				if(map.containsKey(linkID)){
					HashMap<String, ArrayList<String>> dayMap = map.get(linkID);
					if(dayMap.containsKey(day)){
						ArrayList<String> list = dayMap.get(day);
						list.add(status);
						dayMap.put(day, list);
					}else{
						ArrayList<String> list = new ArrayList<>();
						list.add(status);
						dayMap.put(day, list);
					}
					map.put(linkID, dayMap);
				}else{
					HashMap<String, ArrayList<String>> dayMap = new HashMap<>();
					ArrayList<String> list = new ArrayList<>();
					list.add(status);
					dayMap.put(day, list);
					map.put(linkID, dayMap);
				}
			}
			
			reader.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
		System.out.println("linkID的数目：" + map.size());
		int dayNum = 0;
		for(String key:map.keySet()){
			if(dayNum<map.get(key).size())dayNum=map.get(key).size();
		}
		System.out.println("天数最大数目：" + dayNum);
		System.out.println("******************" + path + "读取完毕*************");
		return map;
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
			int sum = 240;
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
			
				if(linkID.equals(preLink) && (time/10000)==(preTime/10000)){
					count++;
					rec += line+"\n";
				}else {
					if(count==sum){
						qualified++;
					}
					else if(count<sum){
						unqua++;
						writer.write(rec);		
					}else{
						System.out.println(line);
					}
					rec = line+"\n";
					count = 1;
				}
				
				preLine = line;
				preLink = linkID;
				preTime = time;
			}
			
			if(count==sum){
				qualified++;
			}
			else{
				unqua++;
				writer.write(rec);		
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
	 * 踢掉时间范围在22:00-24:00以及0:00-6:00的数据记录
	 * 踢掉周六周日的数据记录
	 * 剔掉节假日的数据记录
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
				
				String line = "";
				String[] lineArray;
				
				while ((line = reader.readLine()) != null) {
					lineArray = line.split(",");
					String time = lineArray[1].trim();

					//只筛选高峰时期的数据
					String hour = time.substring(8,10);
					if(!peakHour.contains(hour))continue;
					
					writer.write(line + "\n");
				}
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
	public static void fillInternelData(String inPath, String outPath){
		try {
			InputStreamReader inStream = new InputStreamReader(new FileInputStream(inPath), "UTF-8");
			BufferedReader reader = new BufferedReader(inStream);
			OutputStreamWriter writerStream = new OutputStreamWriter(new FileOutputStream(outPath), "utf-8");
			BufferedWriter writer = new BufferedWriter(writerStream);
			
			//1.丢失数据15分钟及以内，两端拥堵等级相同，填充端点拥堵等级
			//2.丢失数据5分钟以内，两端拥堵等级不同，填充与相邻端点相同拥堵等级
			int firstRule = 16;
			int secondRule = 6;
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
						String t = getNextMin(preTime);
						for(int i = 1; i < miss; i++){
							writer.write(linkID +"," + t + "," + status + "\n");
							t = getNextMin(t);
						}
					}
					
					//rule2
					if(miss <= secondRule && !status.equals(preStat)){
						String t = getNextMin(preTime);
						String curStat = "";
						for(int i = 1; i < miss; i++){
							curStat = (i > miss/2) ? status:preStat;
							writer.write(linkID +"," + t + "," + curStat + "\n");
							t = getNextMin(t);
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
	
	public static void fillEdgeData(String inPath, String outPath){
		try {
			InputStreamReader inStream = new InputStreamReader(new FileInputStream(inPath), "UTF-8");
			BufferedReader reader = new BufferedReader(inStream);
			OutputStreamWriter writerStream = new OutputStreamWriter(new FileOutputStream(outPath), "utf-8");
			BufferedWriter writer = new BufferedWriter(writerStream);
			
			//丢失数据在时间段开头或结尾，五分钟之内，填充端点拥堵等级
			int rule = 5;

			String preLine = "";
			String preLink = "";
			String preTime = "";
			String preStat = "";
			String preHour = "";
			
			String line = "";
			String[] lineArray;
			while ((line = reader.readLine()) != null) {
				lineArray = line.split(",");
				String linkID = lineArray[0].trim();
				String status = lineArray[2].trim();
				String time = lineArray[1].trim();
				String hour = time.substring(8,10);
				
				//1、文件两端缺失数据
				//情况1.1：文件开始，时间段开始缺数据
				if(preLine.equals("")){
					String begin = time.substring(0,8) + "0700";
					long miss = subTimeByMin(formatTime(begin), formatTime(time));
					if(miss > 0 && miss <= rule){
						String t = begin;
						for(int i = 0; i < miss; i++){
							writer.write(linkID +"," + t + "," + status + "\n");
							t = getNextMin(t);
						}
					}
				}
			
				//情况1.2:文件结尾，时间段结尾缺数据
				//手动补充吧!!!!!!!!!!!
				
				//2、文件中间缺失数据
				//情况2.1：文件中间，时间段结尾缺数据
				//注意！！！！一定要先补充时间段结尾数据，才能补充时间段开始数据
				if(!preLine.equals("") && !preHour.equals(hour) &&
						(preHour.equals("08") || preHour.equals("18"))){
					String end = preTime.substring(0,10) + "59";
					long miss = subTimeByMin(formatTime(preTime), formatTime(end));
					if(miss > 0 && miss <= rule){
						String t = getNextMin(preTime);
						for(int i = 0; i < miss; i++){
							writer.write(preLink +"," + t + "," + preStat + "\n");
							t = getNextMin(t);
						}
					}
				}
				
				//情况2.2：文件中间，时间段开始，缺数据
				if(!preLine.equals("") && !preHour.equals(hour) &&
						(hour.equals("07") || hour.equals("17"))){
					String begin = time.substring(0,10) + "00";
					long miss = subTimeByMin(formatTime(begin), formatTime(time));
					if(miss > 0 && miss <= rule){
						String t = begin;
						for(int i = 0; i < miss; i++){
							writer.write(linkID +"," + t + "," + status + "\n");
							t = getNextMin(t);
						}
					}
				}
		
				preLine = line;
				preLink = linkID;
				preStat = status;
				preTime = time;
				preHour  = preTime.substring(8,10);
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
	
	//这个方法a和b的值不对，有待解读
	public static void fillSameCycleData(String inPath, String outPath){
		HashMap<String, ArrayList<String>> map = getFillMap(inPath);
		try {
			InputStreamReader inStream = new InputStreamReader(new FileInputStream(inPath), "UTF-8");
			BufferedReader reader = new BufferedReader(inStream);
			OutputStreamWriter writerStream = new OutputStreamWriter(new FileOutputStream(outPath), "utf-8");
			BufferedWriter writer = new BufferedWriter(writerStream);

			int upLimit = 360;//10:00-16:00共6个小时
			String preLine = reader.readLine();
			String preLink = preLine.split(",")[0];
			String preTime = preLine.split(",")[1];
			String preStat = preLine.split(",")[2];
			
			writer.write(preLine + "\n");
			
			int a=0,b = 0;
			
			String line = "";
			String[] lineArray;
			while ((line = reader.readLine()) != null) {
				lineArray = line.split(",");
				String linkID = lineArray[0].trim();
				String status = lineArray[2].trim();
				String time = lineArray[1].trim();
				
				long miss = subTimeByMin(formatTime(preTime), formatTime(time));
				if(miss > 0 && miss < upLimit && linkID.equals(preLink)){
					String preKey = linkID + "," + getPreWeek(time).substring(0,8); //linkID+day作为key
					String nextKey = linkID + "," + getNextWeek(time).substring(0,8);
					boolean flag = true;
					if(map.containsKey(preKey)){
						String begin = linkID + "," + getPreWeek(preTime) + "," + preStat;
						String end = linkID + "," + getPreWeek(time) + "," + status;
						ArrayList<String> list = map.get(preKey);
						int begin_index = list.indexOf(begin);
						int end_index = list.indexOf(end);
						if(begin_index!=-1 && end_index!=-1 && (end_index-begin_index==miss)){
a++;
							flag = false;
							for(int i = begin_index+1; i < end_index; i++){
								String[] arr = list.get(i).split(",");
								writer.write(linkID+","+getNextWeek(arr[1])+","+arr[2] + "\n");
							}
						}
					}
					if(map.containsKey(nextKey) && flag){
						String begin = linkID + "," + getNextWeek(preTime) + "," + preStat;
						String end = linkID + "," + getNextWeek(time) + "," + status;
						ArrayList<String> list = map.get(nextKey);
						int begin_index = list.indexOf(begin);
						int end_index = list.indexOf(end);
						if(begin_index!=-1 && end_index!=-1 && (end_index-begin_index==miss)){
b++;
							for(int i = begin_index+1; i < end_index; i++){
								String[] arr = list.get(i).split(",");
								writer.write(linkID+","+getPreWeek(arr[1])+","+arr[2] + "\n");
							}
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
			System.out.println(a + " " + b);
			System.out.println(outPath + " write finish!!");
		} catch (Exception e) {
			e.printStackTrace();
		}
		System.out.println("******************数据处理完毕*************");
	}
	
	public static HashMap<String, ArrayList<String>> getFillMap(String path){
		HashMap<String, ArrayList<String>> map = new HashMap<>();
		try {
			InputStreamReader inStream = new InputStreamReader(new FileInputStream(path), "UTF-8");
			BufferedReader reader = new BufferedReader(inStream);
			
			String line = "";
			String[] lineArray;
			while ((line = reader.readLine()) != null) {
				lineArray = line.split(",");
				String linkID = lineArray[0].trim();
				String time = lineArray[1].trim();
				
				String key = linkID+","+time.substring(0,8);
				if(map.containsKey(key)){
					ArrayList<String> temp = map.get(key);
					temp.add(line);
					map.put(key, temp);
				}else{
					ArrayList<String> temp = new ArrayList<>();
					temp.add(line);
					map.put(key, temp);
				}
			}
			
			reader.close();
			System.out.println(path + " read finish!!");
		} catch (Exception e) {
			e.printStackTrace();
		}
		return map;
	}
	
	//获取指定时间的下一分钟 time格式:201909141752
	public static String getNextMin(String time){
		return getAppointTime(Calendar.MINUTE, 1, time);
	}
	
	//获取一周后的指定时间 time格式:201909141752
	public static String getNextWeek(String time){
		return getAppointTime(Calendar.DAY_OF_WEEK, 7, time);
	}
	
	//获取一周前的指定时间 time格式:201909141752
	public static String getPreWeek(String time){
		return getAppointTime(Calendar.DAY_OF_WEEK, -7, time);
	}
	
	/**
	 * 根据给定的Calender,
	 * @param cal 给定的Calender
	 * @param value 距离给定时间的距离
	 * @param time 给定的时间
	 * @return
	 */
	public static String getAppointTime(int cal, int value, String time){
		int year = Integer.parseInt(time.substring(0,4));
		//mon要减1，因为设置完成后会自动加1，不是很懂原理
		int mon = Integer.parseInt(time.substring(4,6)) - 1;
		int day = Integer.parseInt(time.substring(6,8));
		int hour = Integer.parseInt(time.substring(8,10));
		int min = Integer.parseInt(time.substring(10,12));
		
        Calendar nowAfter = Calendar.getInstance();
        nowAfter.set(year, mon, day, hour, min);
        nowAfter.add(cal, value);
        SimpleDateFormat sdf= new SimpleDateFormat("yyyy-MM-dd HH:mm");
        String nextMin = sdf.format(nowAfter.getTimeInMillis()) ;
        nextMin = nextMin.replace("-", "").replace(" ", "").replace(":", "");
        return nextMin;
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
