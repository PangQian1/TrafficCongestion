package grid;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.OutputStreamWriter;
import java.util.HashSet;

public class DealRMidFile {
	public static final HashSet<String> excludedLinkType = new HashSet<String>() {{
		add("0a");//辅路
		add("0b");//匝道
		add("0e");//POI连接路
		add("01");//无属性01（即双向均可通行）
		add("03");//JCT
		add("05");//IC
		add("11");//公交专用道
		add("13");//风景线
	}}; 
	
	public static void main(String[] args) {
		String oriMapPath = "E:/G-1149/trafficCongestion/网格化/originMap.csv";
		String resMapPath = "E:/G-1149/trafficCongestion/网格化/resMap.csv";
		String _02LinkMapPath = "E:/G-1149/trafficCongestion/网格化/02LinkMap.csv";
		
		//dealRMidFile("E:\\G-1149\\trafficCongestion\\北京地图数据\\beijing\\dealedMap\\RbjWithoutDidir.MID", "D:\\program\\congestion\\originMapWithOutBiDirec.csv");
		//filteLink(oriMapPath, resMapPath);
		get_02Link(oriMapPath, _02LinkMapPath);
	}
	
	public static void get_02Link(String inPath, String outPath){
		try {
			BufferedReader reader = new BufferedReader(new FileReader(inPath));
			OutputStreamWriter writerStream = new OutputStreamWriter(new FileOutputStream(outPath), "utf-8");
			BufferedWriter writer = new BufferedWriter(writerStream);
			
			String line;
			String[] lineArr;
			while((line = reader.readLine()) != null){
				lineArr = line.split(",");
				if(lineArr[4].equals("0") || lineArr[4].equals("1")) continue;//去掉双向可通行
				String[] kind = lineArr[2].split("\\|");
				boolean flag = true;
				for(int i = 0; i < kind.length; i++){
					if(!kind[i].substring(2,4).equals("02")){//不是上下线分离
						flag = false;
						break;
					}
				}
				if(!flag) continue; //去掉指定的道路类型
				writer.write(line + "\n");
			}
			
			reader.close();
			writer.flush();
			writer.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public static void filteLink(String inPath, String outPath){
		try {
			BufferedReader reader = new BufferedReader(new FileReader(inPath));
			OutputStreamWriter writerStream = new OutputStreamWriter(new FileOutputStream(outPath), "utf-8");
			BufferedWriter writer = new BufferedWriter(writerStream);
			
			String line;
			String[] lineArr;
			while((line = reader.readLine()) != null){
				lineArr = line.split(",");
				if(lineArr[4].equals("0") || lineArr[4].equals("1")) continue;//去掉双向可通行
				String[] kind = lineArr[2].split("\\|");
				boolean flag = true;
				for(int i = 0; i < kind.length; i++){
					if(excludedLinkType.contains(kind[i].substring(2,4))){
						flag = false;
						break;
					}
				}
				if(!flag) continue; //去掉指定的道路类型
				writer.write(line + "\n");
			}
			
			reader.close();
			writer.flush();
			writer.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public static void dealRMidFile(String inPath, String outPath){
		try {
			BufferedReader reader = new BufferedReader(new FileReader(inPath));
			OutputStreamWriter writerStream = new OutputStreamWriter(new FileOutputStream(outPath), "utf-8");
			BufferedWriter writer = new BufferedWriter(writerStream);
			//writer.write("linkID,kindNum,kind,width,direction,sNodeID,eNodeID,laneNum,elevated,structure\n");
			String line;
			
			while((line = reader.readLine()) != null){
				line = line.replace("\"", "");
				String[] lineArr = line.split(",");
				if(lineArr.length == 42){
					//剔除双向均可通行道路
					if(lineArr[5].equals("1")) continue;
					writer.write(lineArr[1]+","+lineArr[2]+","+lineArr[3]+","+lineArr[4]+","+lineArr[5]+","
						+lineArr[9]+","+lineArr[10]+","+lineArr[12]+","+lineArr[27]+","+lineArr[29]+","+lineArr[30]+"\n");
				}
			}
			
			reader.close();
			writer.flush();
			writer.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	


}
