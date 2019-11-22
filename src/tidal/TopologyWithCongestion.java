package tidal;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.sound.sampled.Line;

import org.omg.PortableInterceptor.IORInterceptor;

public class TopologyWithCongestion {
	
	public static void main(String[] args) {
		topologyWithCongestion("I:\\programData\\trafficCongetion\\res(早晚高峰除多方向linkID)_Status3", 
				"I:\\programData\\trafficCongetion\\bjTopolog(withoutNull).csv",
				"I:\\programData\\trafficCongetion\\潮汐道路研究\\fusionRes(去除方向匹配)_Status3.csv");
	}
	
	public static int switchDirToMap(String direction){
		int dir = Integer.parseInt(direction);
		if(dir == 0) return 2;
		else if(dir == 1) return 3;
		else return 1;
	}
	
	public static int getArrayIndex(String time){
		int lastChar = Integer.parseInt(time.substring(3));
		int t = Integer.parseInt(time);
		if(t < 800){
			return (lastChar-1);
		}else if(t < 900){
			return (lastChar+3);
		}else if(t < 1800){
			return (lastChar+7);
		}else if(t < 1900){
			return (lastChar+11);
		}
		return -1;
	}
	
	public static Map<String, ArrayList<String>> getCongestionMap(String congestionFileByDate){
		Map<String, ArrayList<String>> conMap = new HashMap<>();//linkID:dir,statusList
		
		try {
			InputStreamReader inStream = new InputStreamReader(new FileInputStream(congestionFileByDate), "UTF-8");
			BufferedReader reader = new BufferedReader(inStream);

			String line = reader.readLine();
			String[] lineArray;

			while ((line = reader.readLine()) != null) {
				
				lineArray = line.split(",");
				if(lineArray.length == 9) {
					String time = lineArray[0];
					String linkID = lineArray[1];
					int dir = switchDirToMap(lineArray[2]);
					String status = lineArray[7];
					
					if(dir != 1){//不要双向的
						//改动1
						//String key = linkID + ":" + dir;
						String key = linkID;
						int index = getArrayIndex(time.substring(8));
						if(conMap.containsKey(key)){
							ArrayList<String> congestionList = conMap.get(key);
							congestionList.set(index, status);
							conMap.put(key, congestionList);
						}else{
							ArrayList<String> congestionList = new ArrayList<>();
							for(int i = 0; i < 16; i++){
								congestionList.add("a");
							}
							congestionList.set(index, status);
							conMap.put(key, congestionList);
						}
					}				
				}
			}
			reader.close();	
			System.out.println(congestionFileByDate + " read finish!");

		} catch (Exception e) {
			e.printStackTrace();
		}
		
		return conMap;
	}
	
	public static Map<String, String> getTopologyMap(String topologyPath){
		Map<String, String> topologyMap = new HashMap<>();//linkID:dir,line
		
		try {
			InputStreamReader inStream = new InputStreamReader(new FileInputStream(topologyPath), "GBK");
			BufferedReader reader = new BufferedReader(inStream);

			String line;
			String[] lineArray;

			while ((line = reader.readLine()) != null) {
				
				lineArray = line.split(",");
				if(lineArray.length == 13) {
					String linkID = lineArray[0];
					String dir = lineArray[12];
					//改动2
					//topologyMap.put(linkID+":"+dir, line);
					topologyMap.put(linkID, line);
				}
			}
			reader.close();	
			System.out.println(topologyPath + " read finish!");

		} catch (Exception e) {
			e.printStackTrace();
		}
		
		return topologyMap;
	}
	
	public static void topologyWithCongestion(String tjamPath, String topologyPath, String topoWithConPath){
		int field_num = 13;//简单拓扑文件所应包括的字段数
		try {
			Map<String, String> topologyMap = getTopologyMap(topologyPath);
			
			File file = new File(tjamPath);
			List<String> list = Arrays.asList(file.list());	
			
			for (int i = 0; i < list.size(); i++) {

				String path = tjamPath + "/" + list.get(i);
				Map<String, ArrayList<String>> conMap = getCongestionMap(path);
				
				for(String key:conMap.keySet()) {
					if(topologyMap.containsKey(key)) {
						ArrayList<String> conList = conMap.get(key);
						String conIndex = "";//
						for(int j = 0; j < conList.size(); j++) {
							if(j == (conList.size()/2-1)) {
								conIndex += conList.get(j) + "  ";
							}else if(j < conList.size()-1) {
								conIndex += conList.get(j) + ":";
							}else {
								conIndex += conList.get(j);
							}
						}
						
						int curField = topologyMap.get(key).split(",").length;//该行当前的字段数
						String fillStr = "";//填补字段
						int limit = field_num - curField + 1;
						for(int m = 0; m < limit; m++) {
							fillStr += ",";
						}
													
						String value = topologyMap.get(key) + fillStr + conIndex;
						topologyMap.put(key, value);
					}
				}
				field_num++;
			}
			
			write(topoWithConPath, topologyMap);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static void write(String path, Map<String, String> map) {
		try {
			OutputStreamWriter writerStream = new OutputStreamWriter(new FileOutputStream(path), "utf-8");
			BufferedWriter writer = new BufferedWriter(writerStream);
			writer.write("id,下一ID,上一ID,长度,是否匝道,道路类型,Rname,起点经度,起点纬度,车道数,mid文件中行号,收费站,方向,13,14,15\n");
			for(String id:map.keySet()){
				writer.write(map.get(id));
				writer.write("\n");
			}

			writer.flush();
			writer.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		System.out.println(path + "  write finish!!");
	}

	
	//与原始方法的唯一区别在于将一天内的早晚高峰数据从隶属于同一数据字段划分为两个字段——对于CSV文件来说，即用逗号隔开
	public static void topologyWithCongestionV2(String tjamPath, String topologyPath, String topoWithConPath){
		int field_num = 13;//简单拓扑文件所应包括的字段数
		try {
			Map<String, String> topologyMap = getTopologyMap(topologyPath);
			
			File file = new File(tjamPath);
			List<String> list = Arrays.asList(file.list());	
			
			for (int i = 0; i < list.size(); i++) {

				String path = tjamPath + "/" + list.get(i);
				Map<String, ArrayList<String>> conMap = getCongestionMap(path);
				
				for(String key:conMap.keySet()) {
					if(topologyMap.containsKey(key)) {
						ArrayList<String> conList = conMap.get(key);
						String conIndex = "";//
						for(int j = 0; j < conList.size(); j++) {
							if(j == conList.size()-1) {
								conIndex += conList.get(j);
							}else if((j+1) % 8 == 0) {
								conIndex += conList.get(j) + ",";
							}else{
								conIndex += conList.get(j) + ":";
							}
						}
						
						int curField = topologyMap.get(key).split(",").length;//该行当前的字段数
						String fillStr = "";//填补字段
						int limit = field_num - curField + 1;
						for(int m = 0; m < limit; m++) {
							fillStr += ",";
						}
													
						String value = topologyMap.get(key) + fillStr + conIndex;
						topologyMap.put(key, value);
					}
				}
				field_num += 2;
			}
			
			writeV2(topoWithConPath, topologyMap);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static void writeV2(String path, Map<String, String> map) {
		try {
			OutputStreamWriter writerStream = new OutputStreamWriter(new FileOutputStream(path), "utf-8");
			BufferedWriter writer = new BufferedWriter(writerStream);
			writer.write("id,下一ID,上一ID,长度,是否匝道,道路类型,Rname,起点经度,起点纬度,车道数,mid文件中行号,收费站,方向,"
					+ "6.13-早高峰,6.13-晚高峰,6.14-早高峰,6.14-晚高峰,6.15-早高峰,6.15-晚高峰\n");
			for(String id:map.keySet()){
				writer.write(map.get(id));
				writer.write("\n");
			}

			writer.flush();
			writer.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		System.out.println(path + "  write finish!!");
	}

}
