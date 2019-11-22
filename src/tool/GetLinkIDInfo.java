package tool;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.HashMap;
import java.util.Map;

import javax.naming.InitialContext;

public class GetLinkIDInfo {
	
	public static void main(String[] args) {
//		getLinkIDInfo("I:\\programData\\trafficCongetion\\��ϫ��·�о�\\fusionRes(ȥ������ƥ��)_Status3.csv", 
//				"I:\\programData\\trafficCongetion\\��ϫ��·�о�\\��ʯ����-��Ȫ��\\linkID��·.csv",
//				"I:\\programData\\trafficCongetion\\��ϫ��·�о�\\��ʯ����-��Ȫ��\\��ʯ����-��Ȫ��_Status3.csv");
		
		//removeFrequence();
		
		getLinkIDInfoSum("I:\\programData\\trafficCongetion\\��ϫ��·�о�\\fusionRes(ȥ������ƥ��)_Status3v2.csv", 
				"I:\\programData\\trafficCongetion\\��ϫ��·�о�\\ѧԺ��-������\\linkID��·.csv",
				"I:\\programData\\trafficCongetion\\��ϫ��·�о�\\ѧԺ��-������\\ѧԺ��-������_Status3_sum.csv");
	}
	
	public static Map<String, String> getFusionMap(String fusionResPath){
		Map<String, String> fusionMap = new HashMap<>();//linkID,line
		
		try {
			InputStreamReader inStream = new InputStreamReader(new FileInputStream(fusionResPath), "utf-8");
			BufferedReader reader = new BufferedReader(inStream);

			String line;
			String[] lineArray;

			while ((line = reader.readLine()) != null) {
				
				lineArray = line.split(",");
				//if(lineArray.length == 16) {
					String linkID = lineArray[0].trim();
					fusionMap.put(linkID, line);
				//}else {
					//System.out.println(line);
				//}
			}
			reader.close();	
			System.out.println(fusionResPath + " read finish!");

		} catch (Exception e) {
			e.printStackTrace();
		}
		
		return fusionMap;
	}

	
	public static void getLinkIDInfo(String fusionResPath,String linkIDPath, String linkIDInfoPath){
		Map<String, String> fusionMap = getFusionMap(fusionResPath);
		
		try {
			InputStreamReader inStream = new InputStreamReader(new FileInputStream(linkIDPath), "UTF-8");
			BufferedReader reader = new BufferedReader(inStream);
			
			OutputStreamWriter writerStream = new OutputStreamWriter(new FileOutputStream(linkIDInfoPath), "utf-8");
			BufferedWriter writer = new BufferedWriter(writerStream);
			writer.write(fusionMap.get("id") + "\n");
			
			System.out.println(fusionMap.size());

			String line;

			while ((line = reader.readLine()) != null) {
				line = line.trim();
				if(fusionMap.containsKey(line)){
					writer.write(fusionMap.get(line) + "\n");
				}else{
					System.out.println(line);
				}
			}
			reader.close();	
			writer.flush();
			writer.close();
			System.out.println(linkIDPath + " read finish!");
			System.out.println(linkIDInfoPath + " write finish!");

		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * �÷�������fusionRes(ȥ������ƥ��)_Status3v2.csv�ļ�������ÿ�������߷��ֳ������ֶΣ�v1�Ƿ���һ���ֶ��У��ÿո������
	 * �÷������ɵĽ���ļ��У�status�ֶ�Ϊ��߷壨��߷壩��һϵ��ӵ�µȼ�����8�����ĺ�
	 * @param fusionResPath
	 * @param linkIDPath
	 * @param linkIDInfoSumPath
	 */
	public static void getLinkIDInfoSum(String fusionResPath,String linkIDPath, String linkIDInfoSumPath){
		Map<String, String> fusionMap = getFusionMap(fusionResPath);
		
		try {
			InputStreamReader inStream = new InputStreamReader(new FileInputStream(linkIDPath), "UTF-8");
			BufferedReader reader = new BufferedReader(inStream);
			
			OutputStreamWriter writerStream = new OutputStreamWriter(new FileOutputStream(linkIDInfoSumPath), "utf-8");
			BufferedWriter writer = new BufferedWriter(writerStream);
			writer.write(fusionMap.get("id") + "\n");
			
			System.out.println(fusionMap.size());

			String line;

			while ((line = reader.readLine()) != null) {
				line = line.trim();
				if(fusionMap.containsKey(line)){
					writer.write(calStatusSum(fusionMap.get(line)) + "\n");
				}else{
					System.out.println(line);
				}
			}
			reader.close();	
			writer.flush();
			writer.close();

			System.out.println(linkIDPath + " read finish!");
			System.out.println(linkIDInfoSumPath + " write finish!");

		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	//��������߷��������ֶα�ʾ�󣬲���a������£�a��ʾ��Ӧ��15min��û�����ݣ���a���棩������Сʱ��status�ֶε���ֵ��
	public static String calStatusSum(String line) {
		String res = "";
		String[] lineArray = line.split(",");
		int size = lineArray.length;
		for(int i = 13; i < size; i++) {
			String curStatusLine = lineArray[i];
			if(!curStatusLine.equals("") && curStatusLine != null && curStatusLine.indexOf("a") == -1) {
				String[] statusList = curStatusLine.split(":");
				double sum = 0;
				for(int j = 0; j < statusList.length; j++) {
					sum += Double.parseDouble(statusList[j]);
				}
				lineArray[i] = sum + "";
			}
		}
		
		if(size > 13) {
			for(int i = 0; i < size; i++) {
				if(i == size-1) {
					res += lineArray[i];
				}else {
					res += lineArray[i] + ",";
				}
			}
		}else {
			res = line;
		}
		
		return res;
	}
	
	//ȥ��б�ܼ����ߵ���������Ŀ��������б��ǰӵ�µȼ�Ϊ3��������Ŀ
	public static void removeFrequence(){
		try {
			InputStreamReader inStream = new InputStreamReader
					(new FileInputStream("I:\\programData\\trafficCongetion\\��ϫ��·�о�\\ѧԺ��-������\\ѧԺ��-������.csv"), "UTF-8");
			BufferedReader reader = new BufferedReader(inStream);

			String line;
			String[] lineArray;
			
			OutputStreamWriter writerStream = new OutputStreamWriter
					(new FileOutputStream("I:\\programData\\trafficCongetion\\��ϫ��·�о�\\ѧԺ��-������\\ѧԺ��-������(�޴���).csv"), "utf-8");
			BufferedWriter writer = new BufferedWriter(writerStream);
			writer.write(reader.readLine() + "\n");

			while ((line = reader.readLine()) != null) {
				
				lineArray = line.split(",");
				if(lineArray.length == 16) {
					if(!lineArray[3].equals("")) lineArray[3] = removeBackSlash(lineArray[3]);
					if(!lineArray[4].equals("")) lineArray[4] = removeBackSlash(lineArray[4]);
					if(!lineArray[5].equals("")) lineArray[5] = removeBackSlash(lineArray[5]);
				}
				
				String res = "";
				for(int i = 0; i < lineArray.length; i++){
					if(i == lineArray.length-1){
						res += lineArray[i];
					}else{
						res += lineArray[i] +",";
					}
				}
				
				writer.write(res + "\n");
			}
					
			reader.close();	
			writer.flush();
			writer.close();

		} catch (Exception e) {
			e.printStackTrace();
		}
		
	}
	
	public static String removeBackSlash(String record){
		String[] recArray = record.split("  ");
		String[] morningArr = recArray[0].split(":");
		String[] afternoonArr = recArray[1].split(":");
		
		for(int i = 0; i <  morningArr.length; i++){
			int indexMor = morningArr[i].indexOf("\\");
			morningArr[i] = morningArr[i].substring(0, indexMor);
			int indexAft = afternoonArr[i].indexOf("\\");
			afternoonArr[i] = afternoonArr[i].substring(0, indexAft);
		}
		
		String res = "";
		for(int i = 0; i < morningArr.length; i++){
			if(i == morningArr.length-1){
				res += morningArr[i] + "  ";
			}else{
				res += morningArr[i] + ":";
			}
		}
		
		for(int i = 0; i < afternoonArr.length; i++){
			if(i == afternoonArr.length-1){
				res += afternoonArr[i];
			}else{
				res += afternoonArr[i] + ":";
			}
		}
		
		return res;
	}
	
}
