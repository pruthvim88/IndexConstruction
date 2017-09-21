package com.ir.Project2;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.MultiFields;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.BytesRef;

public class InvertedIndex {
	
	public static HashMap<String, LinkedList<Integer>> hmap = new HashMap<String, LinkedList<Integer>>();
	public static int count_TaatAnd = 0;
	public static int count_TaatOr = 0;
	public static int count_DaatAnd = 0;
	public static int count_DaatOr = 0;
	public static final String UTF8_BOM = "\uFEFF";

	public static void main(String[] args) throws Exception, Exception{
	    IndexReader reader = DirectoryReader.open(FSDirectory.open(Paths.get(args[0])));
	    Writer pw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(args[1]), "UTF-8"));	
		Fields fields = MultiFields.getFields(reader);
		for (String field : fields){
		Terms terms = fields.terms(field);
		TermsEnum termsEnum = terms.iterator();
		BytesRef text;     
		while ((text = termsEnum.next()) != null && !field.equals("_version_") && !field.equals("id")) {
			LinkedList<Integer> docIdList = new LinkedList<Integer>();
			PostingsEnum docs = termsEnum.postings(null);
			while (docs.nextDoc() != PostingsEnum.NO_MORE_DOCS) {
				docIdList.add(docs.docID());       
				}
				hmap.put(text.utf8ToString(), docIdList);
		    }
		}		
		File queryReader = new File(args[2]);
		BufferedReader input = new BufferedReader(new InputStreamReader(new FileInputStream(queryReader), "UTF-8"));
		IR2Assignement irProj2 = new IR2Assignement();
		String output = null;
		String [] queryTerms = null;
		String queryTerm = null;
		while ((output = input.readLine()) != null) {
			queryTerm=removeUTF8BOM(output);
			queryTerms=queryTerm.trim().split(" ");
			for(int i=0;i<queryTerms.length;i++){
				pw.write(irProj2.getPostings(queryTerms[i]));
				//System.out.println(irProj2.getPostings(queryTerms[i]));
			}
			pw.write(irProj2.termAtATimeQueryAnd(queryTerms));
			//System.out.println(irProj2.termAtATimeQueryAnd(queryTerms));
			pw.write(irProj2.termAtATimeQueryOr(queryTerms));
			//System.out.println(irProj2.termAtATimeQueryOr(queryTerms));
			pw.write(irProj2.docAtATimeQueryAnd(queryTerms));
			//System.out.println(irProj2.docAtATimeQueryAnd(queryTerms));
			pw.write(irProj2.docAtATimeQueryOr(queryTerms));
			//System.out.println(irProj2.docAtATimeQueryOr(queryTerms));
			}
		input.close();
		pw.close();			
	}
	
	public String getPostings(String query_term){ //To get the list of postings for a given query term
		LinkedList<Integer> postings = hmap.get(query_term);
		String s = "";
		if (postings == null ){
			s=" empty";	
		}
		else{
			s=allElements(postings);
		}
		String output = "GetPostings\n"+query_term+"\nPostings list:"+s+"\n";
		return output;
    }
	
	public String termAtATimeQueryAnd(String [] queryTerms){//compare two postings list at a time and returns the list of postings present in both the lists
		LinkedList<Integer> intersect = new LinkedList<Integer>();
		HashMap<String, LinkedList<Integer>> termAtATimeQuery = new HashMap<String, LinkedList<Integer>>();
		String s = "";
		String output = "";
		count_TaatAnd=0;
		for (int i = 0; i < queryTerms.length; i++) {
			if( hmap.get(queryTerms[i]) == null ){
				return "empty";
			}
			termAtATimeQuery.put(queryTerms[i], hmap.get(queryTerms[i]));
			s= s+queryTerms[i]+" ";	
		}
		if(termAtATimeQuery!=null && termAtATimeQuery.size()==1){
			intersect = termAtATimeQuery.get(queryTerms[0]);
			output = "TaatAnd"+"\n"+s+"\n"+"Results: "+allElements(intersect)+"\n"+"Number of documents in results: "+intersect.size()+"\n"+"Number of comparisons: "+count_TaatAnd+"\n";
		}
		else if(termAtATimeQuery!=null && termAtATimeQuery.size()>1){
			intersect=intersectLists(termAtATimeQuery.get(queryTerms[0]),termAtATimeQuery.get(queryTerms[1]));
			for(int i=2; i<termAtATimeQuery.size();i++){
				intersect=intersectLists(intersect,termAtATimeQuery.get(queryTerms[i]));
			}
		output = "TaatAnd"+"\n"+s.trim()+"\n"+"Results:"+allElements(intersect)+"\n"+"Number of documents in results: "+intersect.size()+"\n"+"Number of comparisons: "+count_TaatAnd+"\n";
		}
		return output;
	}
	
	public String termAtATimeQueryOr(String [] queryTerms){//compare two postings list at a time and returns the all the list of postings in both the lists
		LinkedList<Integer> union = new LinkedList<Integer>();
		HashMap<String, LinkedList<Integer>> termAtATimeQuery = new HashMap<String, LinkedList<Integer>>();
		String s = "";
		String output = "";
		count_TaatOr=0;
		for (int i = 0; i < queryTerms.length; i++) {
			if( hmap.get(queryTerms[i]) == null ){
				return "empty";
			}
			termAtATimeQuery.put(queryTerms[i], hmap.get(queryTerms[i]));
			s= s+queryTerms[i]+" ";;
			
		}
		if(termAtATimeQuery!=null && termAtATimeQuery.size()==1){
			union = termAtATimeQuery.get(queryTerms[0]);
			output = "TaatOr"+"\n"+s+"\n"+"Results: "+allElements(union)+"\n"+"Number of documents in results: "+union.size()+"\n"+"Number of comparisons: "+count_TaatOr+"\n";
		}
		else if(termAtATimeQuery!=null && termAtATimeQuery.size()>1){
			union=unionLists(termAtATimeQuery.get(queryTerms[0]),termAtATimeQuery.get(queryTerms[1]));
			for(int i=2; i<termAtATimeQuery.size();i++){
				union=unionLists(union,termAtATimeQuery.get(queryTerms[i]));
			}
			output = "TaatOr"+"\n"+s.trim()+"\n"+"Results:"+allElements(union)+"\n"+"Number of documents in results: "+union.size()+"\n"+"Number of comparisons: "+count_TaatOr+"\n";
		}
		return output;
	}
	
	public String docAtATimeQueryAnd(String [] queryTerms){//Takes first postings of all the query terms and returns only the postings if present in all of them else move to the next posting till the end of that corresponding postings list
		LinkedList<Integer> intersect = new LinkedList<Integer>();
		HashMap<String, LinkedList<Integer>> docAtaTimeQuery = new HashMap<String, LinkedList<Integer>>();
		ArrayList<LinkedList<Integer>> arraybyDocId = new ArrayList<LinkedList<Integer>>();
		LinkedList<Integer> compare = new LinkedList<Integer>();
		LinkedList<Integer> minimumLinkedlist = new LinkedList<Integer>();
		int max=0,k = 0, countList = 0;
		int minDocId = 0;
		String s = "";
		String output = "";
		int countmin = 0;
		count_DaatAnd = 0;
		for (int i = 0; i < queryTerms.length; i++) {
			if( hmap.get(queryTerms[i]) == null ){		
				return "empty";
			}
			s= s+queryTerms[i]+" ";
			docAtaTimeQuery.put(queryTerms[i], hmap.get(queryTerms[i]));
			arraybyDocId.add(hmap.get(queryTerms[i]));
			compare.add(hmap.get(queryTerms[i]).get(0));
			minimumLinkedlist.add(hmap.get(queryTerms[i]).get(0));
			countList = countList + hmap.get(queryTerms[i]).size();	
			
		}
		if(minimumLinkedlist.size() != 0){
			minDocId = minElement(minimumLinkedlist);
		}

		minimumLinkedlist.clear();
		while (max < countList){
			for(int j=0; j < queryTerms.length; j++){
				k = docAtaTimeQuery.get(queryTerms[j]).indexOf(compare.get(j));
				if( k < docAtaTimeQuery.get(queryTerms[j]).size()){
					if(compare.get(j) == minDocId){
						if((k+1) < docAtaTimeQuery.get(queryTerms[j]).size()){
							minimumLinkedlist.add(docAtaTimeQuery.get(queryTerms[j]).get(k+1));
							compare.set(j, docAtaTimeQuery.get(queryTerms[j]).get(k+1));
							countmin++;
						}
						if(k+1 == docAtaTimeQuery.get(queryTerms[j]).size()){
							countmin++;
						}
					}
					else if(compare.get(j) > minDocId){
						minimumLinkedlist.add(docAtaTimeQuery.get(queryTerms[j]).get(k));
					}
				}
			}
			if (countmin == queryTerms.length){
				intersect.add(minDocId);	
			}
			countmin = 0;
			if(minimumLinkedlist.size() != 0){
				count_DaatAnd++;
				minDocId = minElement(minimumLinkedlist);
				minimumLinkedlist.clear();
			}	
			max++;
		}
		output = "DaatAnd"+"\n"+s.trim()+"\n"+"Results:"+allElements(intersect)+"\n"+"Number of documents in results: "+intersect.size()+"\n"+"Number of comparisons: "+count_DaatAnd+"\n";
		return output;
	}
	
	public String docAtATimeQueryOr(String [] queryTerms){//Takes the first posting of all posting lists of the query terms and returns only the minimum of all them.Then we start comparing all the first postings and increment to their next postings if equal to minimum else we retain the same element and load them to a temporary linked list. We do the same process till all the query terms reach the end of their posting lists.  
		LinkedList<Integer> union = new LinkedList<Integer>();
		HashMap<String, LinkedList<Integer>> docAtaTimeQuery = new HashMap<String, LinkedList<Integer>>();
		ArrayList<LinkedList<Integer>> arraybyDocId = new ArrayList<LinkedList<Integer>>();
		ArrayList<Integer> compare = new ArrayList<Integer>();
		LinkedList<Integer> minimumLinkedlist = new LinkedList<Integer>();
		int max=0,k = 0, countList = 0;
		int minDocId = 0;
		String s = "";
		String output = "";
		count_DaatOr = 0;
		for (int i = 0; i < queryTerms.length; i++) {
			if( hmap.get(queryTerms[i]) == null ){		
				return "empty";
			}
			s= s+queryTerms[i]+" ";
			docAtaTimeQuery.put(queryTerms[i], hmap.get(queryTerms[i]));
			arraybyDocId.add(hmap.get(queryTerms[i]));
			compare.add(hmap.get(queryTerms[i]).get(0));
			minimumLinkedlist.add(hmap.get(queryTerms[i]).get(0));
			countList = countList + hmap.get(queryTerms[i]).size();	
		}
		if(minimumLinkedlist.size() != 0){
			minDocId = minElement(minimumLinkedlist);
		}
		union.add(minDocId);
		minimumLinkedlist.clear();
		while (max < countList){
			for(int j=0; j < queryTerms.length; j++){
				k = docAtaTimeQuery.get(queryTerms[j]).indexOf(compare.get(j));
				if( k < docAtaTimeQuery.get(queryTerms[j]).size()){
					if(compare.get(j) == minDocId){
						if((k+1) < docAtaTimeQuery.get(queryTerms[j]).size()){
							minimumLinkedlist.add(docAtaTimeQuery.get(queryTerms[j]).get(k+1));
							compare.set(j, docAtaTimeQuery.get(queryTerms[j]).get(k+1));
							if(union.getLast() != minDocId){
								union.add(minDocId);
							}	
						}
					}
					else if(compare.get(j) > minDocId){
						minimumLinkedlist.add(docAtaTimeQuery.get(queryTerms[j]).get(k));
						compare.set(j, docAtaTimeQuery.get(queryTerms[j]).get(k));	
					}
					if(k+1 == docAtaTimeQuery.get(queryTerms[j]).size()){
						if(compare.get(j) == minDocId){
							if(union.getLast() != minDocId){
								union.add(minDocId);
							}
						}
					}
				}
			}
			if(minimumLinkedlist.size() != 0){
				count_DaatOr++;
				minDocId = minElement(minimumLinkedlist);
				minimumLinkedlist.clear();
			}	
			max++;
		}
		output = "DaatOr"+"\n"+s.trim()+"\n"+"Results:"+allElements(union)+"\n"+"Number of documents in results: "+union.size()+"\n"+"Number of comparisons: "+count_DaatOr+"\n";
		return output;
	}
	
	public LinkedList<Integer> intersectLists(LinkedList<Integer> a, LinkedList<Integer> b){
		LinkedList<Integer> intersectList = new LinkedList<Integer>();
		int i=0,j=0;
		while (i < a.size() && j < b.size()){
			if(a.get(i).equals(b.get(j))){
				intersectList.add((Integer) a.get(i));
				i++;
				j++;
				count_TaatAnd++;
			}
			else if (a.get(i) < b.get(j)){ 
				i++;
				count_TaatAnd++;
			}	
			else{ 	
				j++;
				count_TaatAnd++;
			}
		}	
		return intersectList;
	}
	
	public LinkedList<Integer> unionLists(LinkedList<Integer> a, LinkedList<Integer> b){
			LinkedList<Integer> unionList = new LinkedList<Integer>();
			int i=0,j=0;
			while (i < a.size() && j < b.size()){
				if(a.get(i).equals(b.get(j))){
					unionList.add((Integer) a.get(i));
					i++;
					j++;
					count_TaatOr++;	
				}	
				else if (a.get(i) < b.get(j)){
					unionList.add((Integer) a.get(i));
						i++;
						count_TaatOr++;
				}	
				else if (a.get(i) > b.get(j)){	
					unionList.add((Integer) b.get(j));
						j++;
						count_TaatOr++;
				}
			}
			while(i < a.size()){
				unionList.add((Integer) a.get(i));
				i++;
			}	
			while(j < b.size()){
				unionList.add((Integer) b.get(j));
				j++;
			}	
			return unionList;
		}
		
//	public int maxDocCount(ArrayList<LinkedList<Integer>> arraybyDocId){
//		int j,count = 0;
//		int[] counts = new int[arraybyDocId.size()];
//		counts[0] = arraybyDocId.get(0).size();
//		for (int i = 1; i< arraybyDocId.size(); i++){
//			j=i-1;
//			counts[i] = arraybyDocId.get(i).size();
//			if(counts[i]<counts[j])
//				count = counts[j];
//			else
//				count = counts[i];
//			}
//		return count;
//		}
	
	public int minElement(LinkedList<Integer> a){
		int min = a.get(0);
		for(int i=1; i<a.size(); i++){
			if(a.get(i)<min)
				min = a.get(i);		
		}		
		return min;
	}
	
//	public int maxElement(LinkedList<Integer> a){
//		int max = a.get(0);
//		for(int i=1; i<a.size(); i++){
//			if(a.get(i)>max)
//				max = a.get(i);		
//		}
//		
//		return max;
//	}
	
	public static String allElements(LinkedList<Integer> l){
		String s = "";
		if(l.size() == 0)
			return " empty";
		else {
			for(int i=0; i<l.size(); i++){
				s = s+" "+l.get(i);		
			}
		}
		return s;
	}
	
	public static String removeUTF8BOM(String s) {
	    if (s.startsWith(UTF8_BOM)) {
	        s = s.substring(1);
	    }
	    return s;
	}
}