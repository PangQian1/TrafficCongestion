package grid;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.OutputStreamWriter;
import java.util.HashSet;

public class DealRMidFile {
	public static final HashSet<String> excludedLinkType = new HashSet<String>() {{
		add("0a");//��·
		add("0b");//�ѵ�
		add("0e");//POI����·
		add("01");//������01����˫�����ͨ�У�
		add("03");//JCT
		add("05");//IC
		add("11");//����ר�õ�
		add("13");//�羰��
	}}; 
	
	public static void main(String[] args) {
		String oriMapPath = "E:/G-1149/trafficCongestion/����/originMap.csv";
		String resMapPath = "E:/G-1149/trafficCongestion/����/resMap.csv";
		String _02LinkMapPath = "E:/G-1149/trafficCongestion/����/02LinkMap.csv";
		
		//dealRMidFile("E:\\G-1149\\trafficCongestion\\������ͼ����\\beijing\\dealedMap\\RbjWithoutDidir.MID", "D:\\program\\congestion\\originMapWithOutBiDirec.csv");
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
				if(lineArr[4].equals("0") || lineArr[4].equals("1")) continue;//ȥ��˫���ͨ��
				String[] kind = lineArr[2].split("\\|");
				boolean flag = true;
				for(int i = 0; i < kind.length; i++){
					if(!kind[i].substring(2,4).equals("02")){//���������߷���
						flag = false;
						break;
					}
				}
				if(!flag) continue; //ȥ��ָ���ĵ�·����
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
				if(lineArr[4].equals("0") || lineArr[4].equals("1")) continue;//ȥ��˫���ͨ��
				String[] kind = lineArr[2].split("\\|");
				boolean flag = true;
				for(int i = 0; i < kind.length; i++){
					if(excludedLinkType.contains(kind[i].substring(2,4))){
						flag = false;
						break;
					}
				}
				if(!flag) continue; //ȥ��ָ���ĵ�·����
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
					//�޳�˫�����ͨ�е�·
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
