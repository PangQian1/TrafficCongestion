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
	public static String oriPath = "E:\\G-1149\\trafficCongestion\\res";//��һ���ļ���Ŀ¼(��һ��������Ϊ��λ)
	public static String linkSamplePath = "C:\\Users\\98259\\Desktop\\6.9ѧϰ����ĵ�\\��������\\linkPeer.csv";
	public static String samplePath = "C:\\Users\\98259\\Desktop\\6.9ѧϰ����ĵ�\\��������\\fiftMin\\sample.csv";

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
	
	public static void main(String[] args) {
		getSample(oriPath, linkSamplePath, samplePath);
	}

}
