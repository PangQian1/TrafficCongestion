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
import java.util.List;
import java.util.Map;

public class SamplePreprocess {
	/**
	 * ���Ѿ��ź����ԭʼtjam�ļ�������Ϊ��λ��������link������֯
	 * @param inPath
	 * @param outPath
	 */
	public static void mergeDataByLink(String inPath, String outPath){
		File file = new File(inPath);
		List<String> list = Arrays.asList(file.list());	
		try {
			
			for (int i = 0; i < list.size(); i++) {
				//���δ���ÿһ���ļ�
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
				
				for(String key: tjamMap.keySet()){
					ArrayList<String> temp = tjamMap.get(key);
					for(int j = 0; j < temp.size(); j++){
						writer.write(key + "," + temp.get(j) + "\n");
					}
				}
				
				System.out.println(resPath + " write finish!!");
				writer.flush();
				writer.close();
				reader.close();
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		System.out.println("******************���ݴ������*************");
	}
	

	public static void main(String[] args) {
		String orderedTjamOriPath = "E:\\G-1149\\trafficCongestion\\ѵ������\\ʱ��˳���ļ�";
		String orderedByLinkPath = "E:\\G-1149\\trafficCongestion\\ѵ������\\ʱ��˳���ļ�(����link����)";
		
		mergeDataByLink(orderedTjamOriPath, orderedByLinkPath);
	}

}
