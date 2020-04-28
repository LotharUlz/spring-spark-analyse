package com.nttdata.spring.spark.springsparkanalyse;

import java.io.File;
import java.io.FileNotFoundException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Scanner;
import java.util.TreeMap;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.stat.MultivariateStatisticalSummary;
import org.apache.spark.mllib.stat.Statistics;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class AnalyseService {
 
    @Autowired
    JavaSparkContext sc;
 
    public Long getCountSpark(String pattern) {
        JavaRDD<String> lines = sc.textFile("./big.txt");
        return (long)lines.flatMap(
        		x -> Arrays.asList(x.split("\\s+"))
        		.iterator())
        		.filter(x -> x.equalsIgnoreCase(pattern))
        		.collect().size();
    }
 
    public Long getCountLegacy(String pattern) {
    	long result = 0;
        try {
            File myObj = new File("./big.txt");
            Scanner myReader = new Scanner(myObj);
            while (myReader.hasNextLine()) {
            	String data = myReader.nextLine();
            	List<String> wordsPerLine = Arrays.asList(data.split("\\s+"));
            	for (String word : wordsPerLine) {
            		if (pattern.equalsIgnoreCase(word)) {
            			result++;
            		}
            	}
            }
            myReader.close();
          } catch (FileNotFoundException e) {
            System.out.println("An error occurred.");
            e.printStackTrace();
          }
        return result;
    }
       
    public TreeMap<String, Long> getFilesMatchingPatternFromPathAndCount(String pattern) {
    	JavaPairRDD<String, String> allFiles = sc.wholeTextFiles("./2020*.csv");
    	Map<String, String> fileMap = allFiles.collectAsMap();
    	TreeMap<String, Long> result = new TreeMap<String, Long>();
    	Iterator<Entry<String, String>> it = fileMap.entrySet().iterator();
    	while (it.hasNext()) {
    		@SuppressWarnings("rawtypes")
			Map.Entry pair = (Map.Entry)it.next();
    		List<String> wordsPerFile = Arrays.asList(((String)pair.getValue()).split("\\;"));
    		long counter = 0;
            for (String word : wordsPerFile) {
    			if (word.equalsIgnoreCase(pattern)) {
    				counter++;
    			}
    		}
            result.put((String)pair.getKey(), counter);
    	}
    	return result;
    }
    
    public TreeMap<String, String> getFilesMatchingPatternSumAmount(String pattern) {
    	JavaPairRDD<String, String> allFiles = sc.wholeTextFiles("./2020*.csv");
    	Map<String, String> fileMap = allFiles.collectAsMap();
    	TreeMap<String, String> result = new TreeMap<String, String>();
    	Double sum = 0.0;
    	for (String currentValue : fileMap.values()) {
    		String[] lines = currentValue.split(System.getProperty("line.separator"));
    		for (String stringValue : lines) {
    			String[] valuesPerLine = stringValue.split(";");
    		    if (valuesPerLine[1].equalsIgnoreCase(pattern)) {
    		    	Double amount = Double.valueOf(valuesPerLine[2]);
    		    	sum += amount;
    		    }
			}
		}
    	result.put(pattern, String.format("%.2f", sum));
    	return result;
    }
    
    public List<String> getFilesMatchingPatternDate(String pattern) {
    	JavaPairRDD<String, String> allFiles = sc.wholeTextFiles("./2020*.csv");
    	Map<String, String> fileMap = allFiles.collectAsMap();
    	List<String> result = new ArrayList<String>();
    	for (String currentValue : fileMap.values()) {
    		String[] lines = currentValue.split(System.getProperty("line.separator"));
    		for (String stringValue : lines) {
    			String[] valuesPerLine = stringValue.split(";");
    		    if (valuesPerLine[1].equalsIgnoreCase(pattern)) {
    		    	try {
						Date currentDate = new SimpleDateFormat("yyyymmdd").parse(valuesPerLine[0]);
						DateFormat dateFormat = new SimpleDateFormat("yyyy-mm-dd");
						String strDate = dateFormat.format(currentDate);
						result.add(strDate);
					} catch (ParseException e) {
						e.printStackTrace();
					}
    		    }
			}
		}
    	Collections.sort(result, null);
    	return result;
    }
    
    public Map<String, String> getStatistics() {
    	Map<String, String> result = new HashMap<String, String>();
    	JavaRDD<String> data = sc.textFile("./doubles.csv");
    	JavaRDD<Vector> inputData = data
    			  .map(line -> {
    			      String[] parts = line.split(";");
    			      double[] v = new double[parts.length - 1];
    			      for (int i = 0; i < parts.length - 1; i++) {
    			          v[i] = Double.parseDouble(parts[i]);
    			      }
    			      return Vectors.dense(v);
    			});
    	MultivariateStatisticalSummary summary = Statistics.colStats(inputData.rdd());
    	//Matrix correlMatrix = Statistics.corr(inputData.rdd(), "pearson");
    	
    	String maxValue = "";
    	int counter = 0;
    	for (Double current : summary.max().toArray()) {
			maxValue += counter + ": " + String.valueOf(current) + " ";
			counter++;
		}
    	
    	String minValue = "";
    	counter = 0;
    	for (Double current : summary.min().toArray()) {
			minValue += counter + ": " + String.valueOf(current) + " ";
			counter++;
		}
    	
    	String meanValue = "";
    	counter = 0;
    	for (Double current : summary.mean().toArray()) {
			meanValue += counter + ": " + String.valueOf(current) + " ";
			counter++;
		}
    	
    	String normL1Value = "";
    	counter = 0;
    	for (Double current : summary.normL1().toArray()) {
    		normL1Value += counter + ": " + String.valueOf(current) + " ";
			counter++;
		}
    	
    	String normL2Value = "";
    	counter = 0;
    	for (Double current : summary.normL2().toArray()) {
    		normL2Value += counter + ": " + String.valueOf(current) + " ";
			counter++;
		}
    	
    	result.put("max", maxValue);
    	result.put("min", minValue);
    	result.put("mean", meanValue);
    	result.put("normL1", normL1Value);
    	result.put("normL2", normL2Value);
    	
    	return result;
    }
}