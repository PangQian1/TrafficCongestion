package dataPreprocess;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.sql.SQLNonTransientConnectionException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.sound.midi.Receiver;

import tidal.TopologyWithCongestion;
/**
 * 
 * @author PQ
 * �ϲ�ԭʼ���ݣ�ÿ15���Ӻϲ�Ϊһ������
 * �Ѵ��ڶ�������linkID�޳���
 * ����status�ֶ�ȡֵ������15min�ڣ���ÿ���ӵ�status�ֶ���ֵ��1��ͣ���������ƽ���������ӵ�µȼ�Ϊ1��2��3�����ݣ���������������������
 */
public class Tool3 {

	private static String originDataPath = "I:\\programData\\trafficCongetion\\TJAM1";
	private static String mergeDataBy15MinPath = "I:\\programData\\trafficCongetion\\res(����߷���෽��linkID)_Status3";
	private static String bjTopolog_withoutNullPath = "I:\\programData\\trafficCongetion\\bjTopolog(withoutNull).csv";
	private static String conflictLinkIDPath = "I:\\programData\\trafficCongetion\\��ϫ��·�о�\\�෽��linkID(�������ļ��غ�linkID).csv";
	
	public static void main(String[] args) {
		mergeDataBy15Min(originDataPath, mergeDataBy15MinPath);
	}
	
	/**
	 * �ϲ�ԭʼ���ݣ�ÿ15���Ӻϲ�Ϊһ������
	 * @param inPath
	 * @param outPath
	 */
	public static void mergeDataBy15Min(String inPath, String outPath) {
		//<timePeriod,<linkID�Լ�һϵ������ƴ�ӵ��ַ�����ӵ��״̬+����ʱ��List>>
		Map<String, Map<String, ArrayList<String>>> linkDataMap = new HashMap<String, Map<String, ArrayList<String>>>();
		Map<String, String> detectConflictMap = new HashMap<String, String>();
		Map<String, String> conflictLinkIDMap = new HashMap<>();
		
		
		File file = new File(inPath);
		List<String> list = Arrays.asList(file.list());	
		
		try {
			
			for (int i = 0; i < list.size(); i++) {
				//���δ���ÿһ���ļ���
				//ÿ���ļ���һ���ļ�����ÿ���Ӧһ���ļ�
				String resPath = outPath + "/" + list.get(i) + ".csv";
				OutputStreamWriter writerStream = new OutputStreamWriter(new FileOutputStream(resPath), "utf-8");
				BufferedWriter writer = new BufferedWriter(writerStream);
				writer.write("Time,ObjectID,DirectionFlag,RegionID,RoadLength,RoadClass,LinkType,Status,TravelTime" + "\n");
				
				String path = inPath + "/" + list.get(i);
				File fileIn=new File(path);
				List<String> listIn=Arrays.asList(fileIn.list());
				
				for(int j = 0;j < listIn.size(); j++){
					//���δ���ÿһ���ļ�
					String pathIn=path+"/"+listIn.get(j);
					
					InputStreamReader inStream = new InputStreamReader(new FileInputStream(pathIn), "UTF-8");
					BufferedReader reader = new BufferedReader(inStream);

					String line = "";
					String[] lineArray;
						
					while ((line = reader.readLine()) != null) {
			
						lineArray = line.split(",");
						if(lineArray.length == 29) {							
						
							String time = lineArray[0].trim().split("_")[0];//��¼ʱ��
							String directionFlag = lineArray[2].trim();//������
							String regionID = lineArray[5].trim();//����ID
							String objectID = lineArray[6].trim();//linkID**
							String roadLength = lineArray[7].trim();//��·����
							String roadClass = lineArray[8].trim();//��·�ȼ�����������ֵ�֮��
							String linkType = lineArray[9].trim();//linkType�������ѵ�֮��					
							
							int status = reCalStatus(lineArray[24].trim());//ӵ�µȼ�*
							String travelTime = lineArray[25].trim();//����ʱ��*

							int min = Integer.parseInt(lineArray[0].trim().split("_")[0].substring(10));
							String timePeriod = (min/15)+1 + "";
							
							String key = time.substring(0, 10)+"0"+timePeriod + ","+objectID+","+directionFlag+","+regionID
									+","+roadLength+","+roadClass+","+linkType;
							
							//���ͬһlinkIDͬһ���ӵ���ͬ��������ݼ�¼
							String time_linkID = time.substring(0, 12) + objectID;
							if(detectConflictMap.containsKey(time_linkID)) {
								conflictLinkIDMap.put(objectID, "");
							}else {
								detectConflictMap.put(time_linkID, "");
							}

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
					
					writeData(linkDataMap, conflictLinkIDMap, writer);
					linkDataMap = new HashMap<String, Map<String, ArrayList<String>>>();
					detectConflictMap = new HashMap<String, String>();
				}
				System.out.println(resPath + " write finish!!");
				writer.flush();
				writer.close();
			}
			
			/*			int count = 0;
						Map<String, String> topologyMap = TopologyWithCongestion.getTopologyMap(bjTopolog_withoutNullPath);
						BufferedWriter writer2=new BufferedWriter(new FileWriter(conflictLinkIDPath));
						for(String linkID: conflictLinkIDMap.keySet()) {
							if(topologyMap.containsKey(linkID)) {
								writer2.write(linkID + "\n");
								count++;
							}
						}
						System.out.println(count);
						writer2.flush();
						writer2.close();*/
		} catch (Exception e) {
			e.printStackTrace();
		}
		System.out.println("******************���ݴ������*************");
	}
	
	
	public static void writeData(Map<String, Map<String, ArrayList<String>>> linkDataMap, Map<String, String> conflictMap, BufferedWriter writer){
		DecimalFormat df = new DecimalFormat("0.##"); 
		try {
			
			for (String timePeriod : linkDataMap.keySet()) {
				Map<String, ArrayList<String>> dataMap = linkDataMap.get(timePeriod);
				for(String key : dataMap.keySet()){
					
					//����Ƿ���ڳ�ͻlinkID��������ڣ�ֱ�Ӻ��ԣ��������linkID
					String[] info = key.split(",");
					if(conflictMap.containsKey(info[1])) {
						//System.out.println(info[1]);
						continue;
					}
					
					ArrayList<String> dataList = dataMap.get(key);
					String travelTime_ave = "";
					String status_coe = "";
					int status_num = 0;
					int travelTime_sum = 0;
					int num = 0;			
					for(String dt : dataList) {
						status_num += Integer.parseInt(dt.split(",")[0]);
						travelTime_sum += Integer.parseInt(dt.split(",")[1]);
						num++;
					}
					
					travelTime_ave = df.format((double)travelTime_sum/num);
					status_coe = df.format((double)status_num/num);
					
					String rec;

					rec = key+","+status_coe+","+travelTime_ave;	
					
					
					writer.write(rec + "\n");			
				}
			}
			
			writer.flush();
			//writer.close();�˴�����close��������Ϊ��û��д��
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}


	}
	
	public static int reCalStatus(String status) {
		int sta = Integer.parseInt(status);
		if(sta == 1 || sta == 2 || sta == 3) {
			sta -= 1;
		}else {
			System.out.println(sta);
			sta = 0;
		}
		
		return sta;
	}
}
