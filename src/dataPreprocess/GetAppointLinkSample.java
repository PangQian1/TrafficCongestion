package dataPreprocess;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
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
 * ���ʱ��� 6:00-22:00
 * @author 98259
 *
 */

public class GetAppointLinkSample {
	//ֻҪ����߷�ʱ�����ݣ�Ϊ��֤����ԣ��ֱ���ǰ�������һ��Сʱ������8��Сʱ
	public static final HashSet<String> peakHour = new HashSet<String>() {{
		add("06");add("07");add("08");add("09");add("16");add("17");add("18");add("19");
	}};
	
	public static String oriPath = "E:\\G-1149\\trafficCongestion\\res\\weekDay";//��һ���ļ���Ŀ¼(��һ��������Ϊ��λ)
	public static String linkSamplePath = "C:\\Users\\98259\\Desktop\\6.9ѧϰ����ĵ�\\��������\\linkPeer.csv";
	public static String samplePath = "C:\\Users\\98259\\Desktop\\6.9ѧϰ����ĵ�\\��������\\fiftMin\\sample.csv";
	
	public static void main(String[] args) {
		//getSample(oriPath, linkSamplePath, samplePath);
		
		String testSamplePath_2 = "E:\\G-1149\\trafficCongestion\\ѵ������\\��������\\test_2.csv";
		String testSamplePath_1 = "E:\\G-1149\\trafficCongestion\\ѵ������\\��������\\test_1.csv";
		String testLinkSamplePath = "E:\\G-1149\\trafficCongestion\\��ϫ��·�о�\\����·��\\��ʯ��·\\linkID��·.csv";
		//get8HourSample(oriPath, testLinkSamplePath, testSamplePath_2);
		MergeData16_2(testSamplePath_2, testSamplePath_1);
		
	}
	
	public static void get8HourSample(String oriPath, String linkSamplePath, String samplePath){
		//<linkID, ����߷�ӵ������(��˳�������Զ��Ÿ���)>
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
	
	public static void getSample(String oriPath, String linkSamplePath, String samplePath){
		//<linkID, 6:00-22:00ӵ������(��˳�������Զ��Ÿ���)>
		Map<String, String> linkDataMap = new HashMap<String, String>();
		LinkedList<String> linkList = new LinkedList<>();
		//�ų�0:00-6:00��22:00-24:00��8��Сʱ������
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
				//���δ���ÿһ���ļ�
				
				String pathIn=oriPath+"/"+list.get(i);
				
				InputStreamReader inStream2 = new InputStreamReader(new FileInputStream(pathIn), "UTF-8");
				BufferedReader reader2 = new BufferedReader(inStream2);

				String line2 = reader2.readLine();
				String[] lineArray2;
					
				System.out.println("��ʼ����" + pathIn);
				while ((line2 = reader2.readLine()) != null) {
					lineArray2 = line2.split(",");
					
					String hour = lineArray2[0].substring(8, 10);
					if(otherHour.contains(hour)) continue;
					
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
	 * ��������ÿ���кϲ�Ϊһ�У���ÿһ�����ݶ�Ӧһ��������
	 * ��֯��ʽ 16*2	
	 * @param inPath
	 * @param outPath
	 */
		public static void MergeData16_2(String inPath, String outPath){
			try {
				BufferedReader reader = new BufferedReader(new FileReader(inPath));
				OutputStreamWriter writerStream = new OutputStreamWriter(new FileOutputStream(outPath), "utf-8");
				BufferedWriter writer = new BufferedWriter(writerStream);
			
				String line = "";
				
				while((line = reader.readLine()) != null){
					line += ("," + reader.readLine());
					writer.write(line + "\n");
				}
				
				reader.close();
				writer.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

}
