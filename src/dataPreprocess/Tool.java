package dataPreprocess;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
/**
 * 
 * @author PQ
 * 合并原始数据，每15分钟合并为一条数据
 * status字段取值：简单的算术平均
 */
public class Tool {

	private static String originDataPath = "I:\\programData\\trafficCongetion\\TJAM";
	private static String mergeDataBy15MinPath = "I:\\programData\\trafficCongetion\\res";
	
	/**
	 * 合并原始数据，每15分钟合并为一条数据
	 * @param inPath
	 * @param outPath
	 */
	public static void mergeDataBy15Min(String inPath, String outPath) {
		//<时间段，<由各类信息组成的key，(拥堵等级，旅行时间)>>
		Map<String, Map<String, ArrayList<String>>> linkDataMap = new HashMap<String, Map<String, ArrayList<String>>>();
		File file = new File(inPath);
		List<String> list = Arrays.asList(file.list());	
		
		try {
			
			for (int i = 0; i < list.size(); i++) {
				//依次处理每一个文件夹
				//每个文件夹一个文件，即每天对应一个文件
				String resPath = outPath + "/" + list.get(i) + ".csv";
				OutputStreamWriter writerStream = new OutputStreamWriter(new FileOutputStream(resPath), "utf-8");
				BufferedWriter writer = new BufferedWriter(writerStream);
				writer.write("Time,ObjectID,DirectionFlag,RegionID,RoadLength,RoadClass,LinkType,Status,TravelTime" + "\n");
				
				String path = inPath + "/" + list.get(i);
				File fileIn=new File(path);
				List<String> listIn=Arrays.asList(fileIn.list());
				
				for(int j = 0;j < listIn.size(); j++){
					//依次处理每一个文件
					String pathIn=path+"/"+listIn.get(j);
					
					InputStreamReader inStream = new InputStreamReader(new FileInputStream(pathIn), "UTF-8");
					BufferedReader reader = new BufferedReader(inStream);

					String line = "";
					String[] lineArray;
						
					while ((line = reader.readLine()) != null) {
			
						lineArray = line.split(",");
						if(lineArray.length == 29) {							
						
							String time = lineArray[0].trim().split("_")[0];//记录时间
							String directionFlag = lineArray[2].trim();//方向标记
							String regionID = lineArray[5].trim();//区域ID
							String objectID = lineArray[6].trim();//linkID**
							String roadLength = lineArray[7].trim();//道路长度
							String roadClass = lineArray[8].trim();//道路等级，比如乡镇街道之类
							String linkType = lineArray[9].trim();//linkType，比如匝道之类					
							
							String status = lineArray[24].trim();//拥堵等级*
							String travelTime = lineArray[25].trim();//旅行时间*

							int min = Integer.parseInt(lineArray[0].trim().split("_")[0].substring(10));
							String timePeriod = (min/15)+1 + "";
							
							String key = time.substring(0, 10)+"0"+timePeriod + ","+objectID+","+directionFlag+","+regionID
									+","+roadLength+","+roadClass+","+linkType;

							if(linkDataMap.containsKey(timePeriod)) {
								Map<String, ArrayList<String>> dataMap = linkDataMap.get(timePeriod);
								
								if(dataMap.containsKey(key)){
									ArrayList<String> dataList = dataMap.get(key);
									dataList.add(status + "," + travelTime);
									dataMap.put(key, dataList);
								}else{
									ArrayList<String> dataList = new ArrayList<>();
									dataList.add(status + "," + travelTime);
									dataMap.put(key, dataList);
								}
								
								linkDataMap.put(timePeriod, dataMap);	
							}else {
								Map<String, ArrayList<String>> dataMap = new HashMap<>();
								ArrayList<String> dataList = new ArrayList<>();
								
								dataList.add(status + "," + travelTime);
								
								dataMap.put(key, dataList);
								linkDataMap.put(timePeriod, dataMap);
							}
	
						}
					}					
					reader.close();	
					System.out.println(pathIn + " read finish!");
					
					writeData(linkDataMap, writer);
					linkDataMap = new HashMap<String, Map<String, ArrayList<String>>>();
				}
				System.out.println(resPath + " write finish!!");
				writer.flush();
				writer.close();
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		System.out.println("******************数据处理完毕*************");
	}
	
	
	public static void writeData(Map<String, Map<String, ArrayList<String>>> linkDataMap, BufferedWriter writer){
		DecimalFormat df = new DecimalFormat("0.##"); 
		try {
		
			for (String timePeriod : linkDataMap.keySet()) {
				Map<String, ArrayList<String>> dataMap = linkDataMap.get(timePeriod);
				for(String key : dataMap.keySet()){
					
					ArrayList<String> dataList = dataMap.get(key);
					String status_ave = "";
					String travelTime_ave = "";
					int status_sum = 0;
					int travelTime_sum = 0;
					int num = 0;			
					for(String dt : dataList) {
						status_sum += Integer.parseInt(dt.split(",")[0]);
						travelTime_sum += Integer.parseInt(dt.split(",")[1]);
						num++;
					}
					
					status_ave = df.format((double)status_sum/num);
					travelTime_ave = df.format((double)travelTime_sum/num);
									
					String rec = key+","+status_ave+","+travelTime_ave;	
					writer.write(rec + "\n");			
				}
			}
			
			writer.flush();
			//writer.close();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}


	}
	
	public static void main(String[] args) {
		mergeDataBy15Min(originDataPath, mergeDataBy15MinPath);
	}

}
