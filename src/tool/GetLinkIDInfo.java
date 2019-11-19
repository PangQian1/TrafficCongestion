package tool;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.HashMap;
import java.util.Map;

public class GetLinkIDInfo {
	
	public static void main(String[] args) {
		getLinkIDInfo("I:\\programData\\trafficCongetion\\潮汐道路研究\\fusionRes(去除方向匹配)_Status3.csv", 
				"I:\\programData\\trafficCongetion\\潮汐道路研究\\杏石口桥-香泉桥\\linkID线路.csv",
				"I:\\programData\\trafficCongetion\\潮汐道路研究\\杏石口桥-香泉桥\\杏石口桥-香泉桥_Status3.csv");
		
		//removeFrequence();
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
	
	public static void removeFrequence(){
		try {
			InputStreamReader inStream = new InputStreamReader
					(new FileInputStream("I:\\programData\\trafficCongetion\\潮汐道路研究\\学院桥-六道口\\学院桥-六道口.csv"), "UTF-8");
			BufferedReader reader = new BufferedReader(inStream);

			String line;
			String[] lineArray;
			
			OutputStreamWriter writerStream = new OutputStreamWriter
					(new FileOutputStream("I:\\programData\\trafficCongetion\\潮汐道路研究\\学院桥-六道口\\学院桥-六道口(无次数).csv"), "utf-8");
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
