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

public class GetAppointLinkSample {
	public static String oriPath = "E:\\G-1149\\trafficCongestion\\res";//是一个文件夹目录(以一刻钟数据为单位)
	public static String linkSamplePath = "C:\\Users\\98259\\Desktop\\6.9学习相关文档\\样本数据\\linkPeer.csv";
	public static String samplePath = "C:\\Users\\98259\\Desktop\\6.9学习相关文档\\样本数据\\fiftMin\\sample.csv";

	public static void getSample(String oriPath, String linkSamplePath, String samplePath){
		//<linkID, 6:00-22:00拥堵数据(按顺序来，以逗号隔开)>
		Map<String, String> linkDataMap = new HashMap<String, String>();
		LinkedList<String> linkList = new LinkedList<>();
		//排除0:00-6:00和22:00-24:00共8个小时的数据
		HashSet<String> otherHour = new HashSet<>();
		otherHour.add("00");otherHour.add("01");otherHour.add("02");otherHour.add("03");
		otherHour.add("04");otherHour.add("05");otherHour.add("22");otherHour.add("23");
		 
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
					if(otherHour.contains(hour)) continue;
					
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
	
	public static void main(String[] args) {
		getSample(oriPath, linkSamplePath, samplePath);
	}

}
