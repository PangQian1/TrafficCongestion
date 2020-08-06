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
 * ��֯���������ݣ�������ȡ����
 * ���ʱ��� ���������߷�ʱ�ڹ�4��Сʱ
 * @author 98259
 *
 */

public class GetAppointLinkSample_PeakHour {
	public static final HashSet<String> peakHour = new HashSet<String>() {{
		add("07");add("08");;add("17");add("18");
	}};
	
	public static String oriPath = "E:\\G-1149\\trafficCongestion\\res";//��һ���ļ���Ŀ¼(��һ��������Ϊ��λ)
	public static String linkSamplePath = "C:\\Users\\98259\\Desktop\\6.9ѧϰ����ĵ�\\��������\\linkPeer.csv";
	public static String samplePath = "C:\\Users\\98259\\Desktop\\6.9ѧϰ����ĵ�\\��������\\fiftMin\\samplePeakHour.csv";
	public static String ori_14_Path = "E:\\G-1149\\trafficCongestion\\res\\weekDay\\14.csv";
	public static String oriMapPath = "E:/G-1149/trafficCongestion/����/resMap.csv";
	public static String linkStatus_14_Path = "E:/G-1149/trafficCongestion/����/linkStatus_14.csv";
	
	public static void main(String[] args) {
		//getSample(oriPath, linkSamplePath, samplePath);
		getLinkStatus(ori_14_Path, linkStatus_14_Path, oriMapPath);
	}
	
	public static void getSample(String oriPath, String linkSamplePath, String samplePath){
		//<linkID, 7:00-9:00,17:00-19:00ӵ������(��˳�������Զ��Ÿ���)>
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
				//���δ���ÿһ���ļ�
if(i == 2) break;//��������ĩ�ļ�
				
				String pathIn=oriPath+"/"+list.get(i);
				
				InputStreamReader inStream2 = new InputStreamReader(new FileInputStream(pathIn), "UTF-8");
				BufferedReader reader2 = new BufferedReader(inStream2);

				String line2 = reader2.readLine();
				String[] lineArray2;
					
				System.out.println("��ʼ����" + pathIn);
				while ((line2 = reader2.readLine()) != null) {
					lineArray2 = line2.split(",");
					
					String hour = lineArray2[0].substring(8, 10);
					if(!peakHour.contains(hour)) continue;
					
					String linkID = lineArray2[1];
					String status = lineArray2[7];//ӵ�µȼ�
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
				System.out.println(pathIn + "���ļ�����");
			}

			reader.close();	
			writer.flush();
			writer.close();
			
			System.out.println(samplePath + "д�ļ�����");
			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * ����֮ǰ����õ���tjam�ļ�������6-13�����ģ���һ�������߷�4Сʱ��15min����ӵ��ֵ���У�����resMap.csv�ļ�ɸ������link
	 * @param inPath ��ʱ��˳�����е�6-13tjam�ļ���status�ֶ��Ѿ�����15minΪ��λȡ������ƽ��
	 * @param outPath linkStatus_13.csv
	 * @param originMapPath resMap.csv�ļ�������������link������
	 */
	public static void getLinkStatus(String inPath, String outPath, String originMapPath){
		//<linkID, 7:00-9:00,17:00-19:00ӵ������(��˳�������Զ��Ÿ���)>
		HashMap<String, String> linkDataMap = new HashMap<String, String>();
		//�кܶ�link�����õģ������oriSet��һ��ɸѡ�������кܶ���Լ���ɸ����link
		HashSet<String> oriSet = getOriSet(originMapPath);
		 
		try {
			InputStreamReader inStream = new InputStreamReader(new FileInputStream(inPath), "UTF-8");
			BufferedReader reader = new BufferedReader(inStream);
			String line = reader.readLine();//��һ���Ǳ�����
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
			
			System.out.println(outPath + "д�ļ�����");
			
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
