package dataPreprocess;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * 组织好样本数据，并且提取出来
 * 针对时间段 仅针对早晚高峰时期共4个小时
 * @author 98259
 *
 */

public class GetAppointLinkSample_PeakHour {
	public static final HashSet<String> peakHour = new HashSet<String>() {{
		add("07");add("08");;add("17");add("18");
	}};
	
	public static String oriPath = "E:\\G-1149\\trafficCongestion\\res";//是一个文件夹目录(以一刻钟数据为单位)
	public static String linkSamplePath = "C:\\Users\\98259\\Desktop\\6.9学习相关文档\\样本数据\\linkPeer.csv";
	public static String samplePath = "C:\\Users\\98259\\Desktop\\6.9学习相关文档\\样本数据\\fiftMin\\samplePeakHour.csv";
	public static String ori_14_Path = "E:\\G-1149\\trafficCongestion\\res\\weekDay\\14.csv";
	public static String oriMapPath = "E:/G-1149/trafficCongestion/网格化/resMap.csv";
	public static String linkStatus_14_Path = "E:/G-1149/trafficCongestion/网格化/linkStatus_14.csv";
	
	public static void main(String[] args) {
		//getSample(oriPath, linkSamplePath, samplePath);
		getLinkStatus(ori_14_Path, linkStatus_14_Path, oriMapPath);
	}
	
	public static void getSample(String oriPath, String linkSamplePath, String samplePath){
		//<linkID, 7:00-9:00,17:00-19:00拥堵数据(按顺序来，以逗号隔开)>
		Map<String, String> linkDataMap = new HashMap<String, String>();
		LinkedList<String> linkList = new LinkedList<>();
		 
		try {
			OutputStreamWriter writerStream = new OutputStreamWriter(new FileOutputStream(samplePath), "utf-8");
			BufferedWriter writer = new BufferedWriter(writerStream);

			InputStreamReader inStream = new InputStreamReader(new FileInputStream(linkSamplePath), "UTF-8");
			BufferedReader reader = new BufferedReader(inStream);
			String line = "";
			while((line = reader.readLine()) != null){
				String[] lineArray = line.split(",");
				linkList.add(lineArray[0]);
				linkList.add(lineArray[1]);
				linkDataMap.put(lineArray[0], "");
				linkDataMap.put(lineArray[1], "");
			}
			
			File file = new File(oriPath);
			List<String> list = Arrays.asList(file.list());	
			for (int i = 0; i < list.size(); i++) {
				//依次处理每一个文件
if(i == 2) break;//不处理周末文件
				
				String pathIn=oriPath+"/"+list.get(i);
				
				InputStreamReader inStream2 = new InputStreamReader(new FileInputStream(pathIn), "UTF-8");
				BufferedReader reader2 = new BufferedReader(inStream2);

				String line2 = reader2.readLine();
				String[] lineArray2;
					
				System.out.println("开始处理" + pathIn);
				while ((line2 = reader2.readLine()) != null) {
					lineArray2 = line2.split(",");
					
					String hour = lineArray2[0].substring(8, 10);
					if(!peakHour.contains(hour)) continue;
					
					String linkID = lineArray2[1];
					String status = lineArray2[7];//拥堵等级
					String statusList = linkDataMap.get(linkID);
					statusList += ("," + status);
					linkDataMap.put(linkID, statusList);

				}
				
				for(int j = 0; j < linkList.size(); j++){
					String link = linkList.get(j);
					String statusList = linkDataMap.get(link);
					writer.write(link + ":" + list.get(i) + statusList + "\n");
				}
				
				for(String key: linkDataMap.keySet()){
					String statusList = linkDataMap.get(key);
					statusList = "";
					linkDataMap.put(key, statusList);
				}
				reader2.close();
				System.out.println(pathIn + "读文件结束");
			}

			reader.close();	
			writer.flush();
			writer.close();
			
			System.out.println(samplePath + "写文件结束");
			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * 根据之前处理得到的tjam文件，计算6-13（周四）这一天的早晚高峰4小时（15min）的拥堵值序列，利用resMap.csv文件筛掉无用link
	 * @param inPath 按时间顺序排列的6-13tjam文件，status字段已经按照15min为单位取了算术平均
	 * @param outPath linkStatus_13.csv
	 * @param originMapPath resMap.csv文件，用来对无用link做过滤
	 */
	public static void getLinkStatus(String inPath, String outPath, String originMapPath){
		//<linkID, 7:00-9:00,17:00-19:00拥堵数据(按顺序来，以逗号隔开)>
		HashMap<String, String> linkDataMap = new HashMap<String, String>();
		//有很多link是无用的，因此用oriSet做一轮筛选，但还有很多可以继续筛掉的link
		HashSet<String> oriSet = getOriSet(originMapPath);
		 
		try {
			InputStreamReader inStream = new InputStreamReader(new FileInputStream(inPath), "UTF-8");
			BufferedReader reader = new BufferedReader(inStream);
			String line = reader.readLine();//第一行是标题行
			while((line = reader.readLine()) != null){
				String[] lineArray = line.split(",");
				String linkID = lineArray[1];
				String hour = lineArray[0].substring(8,10);
				if(!peakHour.contains(hour) || !oriSet.contains(linkID)) continue;
				
				if(linkDataMap.containsKey(linkID)){
					String val = linkDataMap.get(linkID);
					val += ("," + lineArray[7]);
					linkDataMap.put(linkID, val);
				}else{
					linkDataMap.put(linkID, lineArray[7]);
				}	
			}
			
			reader.close();	
			writeData(linkDataMap, outPath);
			
			System.out.println(outPath + "写文件结束");
			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public static void writeData(HashMap<String, String> map, String path){
		try {
			OutputStreamWriter writerStream = new OutputStreamWriter(new FileOutputStream(path), "utf-8");
			BufferedWriter writer = new BufferedWriter(writerStream);
			
			for(String key: map.keySet()){
				writer.write(key + "," + map.get(key));
				writer.write("\n");
			}
			
			writer.flush();
			writer.close();
		} catch (Exception e) {
			e.printStackTrace();
		}	
	}

	public static HashSet< String> getOriSet(String path){
		HashSet<String> oriSet = new HashSet<>();
		try {
			InputStreamReader inStream = new InputStreamReader(new FileInputStream(path), "UTF-8");
			BufferedReader reader = new BufferedReader(inStream);
	
			String line = "";
			String[] lineArr;
			while ((line = reader.readLine()) != null) {
				lineArr = line.split(",");
				oriSet.add(lineArr[0]);
			}
			
			reader.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return oriSet;
	}


}
