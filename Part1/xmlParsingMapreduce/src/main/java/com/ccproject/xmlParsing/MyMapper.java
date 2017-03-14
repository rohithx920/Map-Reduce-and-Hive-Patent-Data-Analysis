package com.ccproject.xmlParsing;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamReader;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

/**
 * @author Lakshmi Sridhar
 *
 */
public class MyMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
 
	
	    boolean publication_ref_flag = false;
	    boolean abstract_flag = false;
	    boolean classification_flag = false;
	    boolean assignee_flag = false;
	    boolean document_id_flag = false;
	    MultipleOutputs mos;
	    
	    protected void cleanup(Mapper<LongWritable, Text, Text, IntWritable>.Context context)
	      throws IOException, InterruptedException
	    {
	      this.mos.close();
	      super.cleanup(context);
	    }
	    
	    protected void setup(Mapper<LongWritable, Text, Text, IntWritable>.Context context)
	      throws IOException, InterruptedException
	    {
	      this.mos = new MultipleOutputs(context);
	      super.setup(context);
	    }
	    
	    protected void map(LongWritable key, Text value, Mapper.Context context)
	      throws IOException, InterruptedException
	    {
	      String document = value.toString();
	      String id = new String();
	      String title = new String();
	      String class_name = new String();
	      String year = new String();
	      String orgname = new String();
	      String abs = new String();
	      String word1 = new String();
	      String word2 = new String();
	      String word3 = new String();
	      try
	      {
	        XMLStreamReader reader = XMLInputFactory.newInstance()
	          .createXMLStreamReader(
	          new ByteArrayInputStream(document.getBytes()));
	        String currentElement = "";
	        String propertyName = "";
	        int count = 0;
	        while (reader.hasNext())
	        {
	          int code = reader.next();
	          switch (code)
	          {
	          case 1: 
	            currentElement = reader.getLocalName();
	            break;
	          case 4: 
	            if (currentElement.equalsIgnoreCase("publication-reference"))
	            {
	              this.publication_ref_flag = true;
	            }
	            else if (currentElement.equalsIgnoreCase("document-id"))
	            {
	              if (this.publication_ref_flag) {
	                this.document_id_flag = true;
	              }
	            }
	            else if (currentElement.equalsIgnoreCase("doc-number"))
	            {
	              if (this.document_id_flag)
	              {
	                this.document_id_flag = false;
	                propertyName = reader.getText().trim()
	                  .toLowerCase();
	                if (propertyName.length() > 1) {
	                  id = propertyName;
	                }
	              }
	            }
	            else if (currentElement.equalsIgnoreCase("date"))
	            {
	              if (this.publication_ref_flag)
	              {
	                this.publication_ref_flag = false;
	                propertyName = reader.getText().trim()
	                  .toLowerCase();
	                if (propertyName.length() > 1) {
	                  year = 
	                    propertyName.substring(0, 4).toString();
	                }
	              }
	            }
	            else if (currentElement.equalsIgnoreCase("invention-title"))
	            {
	              propertyName = 
	                reader.getText().trim().toLowerCase();
	              if (propertyName.length() > 1) {
	                title = propertyName;
	              }
	            }
	            else if (currentElement.equalsIgnoreCase("classification-national"))
	            {
	              this.classification_flag = true;
	              count++;
	            }
	            else if (currentElement.equalsIgnoreCase("main-classification"))
	            {
	              if ((this.classification_flag) && (count == 1))
	              {
	                this.classification_flag = false;
	                propertyName = reader.getText().trim()
	                  .toLowerCase();
	                if (propertyName.length() > 1) {
	                  class_name = propertyName;
	                }
	              }
	            }
	            else if (currentElement.equalsIgnoreCase("abstract"))
	            {
	              this.abstract_flag = true;
	            }
	            else if (currentElement.equalsIgnoreCase("p"))
	            {
	              if (this.abstract_flag)
	              {
	                this.abstract_flag = false;
	                HashMap<String, Integer> hashMap = new HashMap();
	                propertyName = reader.getText().trim()
	                  .toLowerCase();
	                abs = propertyName.replaceAll("[^A-Za-z0-9]", 
	                  " ").replaceAll(" +", " ");
	                String[] abs_array = abs.split(" ");
	                for (int i = 0; i < abs_array.length; i++) {
	                  if (!hashMap.containsKey(abs_array[i])) {
	                    hashMap.put(abs_array[i], Integer.valueOf(1));
	                  } else {
	                    hashMap.put(abs_array[i], 
	                      Integer.valueOf(((Integer)hashMap.get(abs_array[i])).intValue() + 1));
	                  }
	                }
	                List<Map.Entry<String, Integer>> list = new LinkedList(
	                  hashMap.entrySet());
	                
	                Collections.sort(list, 
	                  new Comparator()
	                  {
	                    public int compare(Map.Entry<String, Integer> o1, Map.Entry<String, Integer> o2)
	                    {
	                      return ((Integer)o2.getValue()).compareTo(
	                        (Integer)o1.getValue());
	                    }

						public int compare(Object o1, Object o2) {
							// TODO Auto-generated method stub
							return 0;
						}
	                  });
	                Map<String, Integer> result = new LinkedHashMap();
	                for (Map.Entry<String, Integer> entry : list) {
	                  result.put((String)entry.getKey(), (Integer)entry.getValue());
	                }
	                Iterator<String> iterator = result.keySet()
	                  .iterator();
	                if (iterator.hasNext()) {
	                  word1 = (String)iterator.next();
	                }
	                if (iterator.hasNext()) {
	                  word2 = (String)iterator.next();
	                }
	                if (iterator.hasNext()) {
	                  word3 = (String)iterator.next();
	                }
	              }
	            }
	            else if (currentElement.equalsIgnoreCase("assignees"))
	            {
	              this.assignee_flag = true;
	            }
	            else if ((currentElement.equalsIgnoreCase("orgname")) && 
	              (this.assignee_flag))
	            {
	              this.assignee_flag = false;
	              propertyName = reader.getText().trim()
	                .toLowerCase();
	              propertyName = propertyName.replace("%ampr%", 
	                "&");
	              propertyName = propertyName.replaceAll(
	                "[^&a-z0-9 ]+", "");
	              if (propertyName.length() > 1) {
	                orgname = propertyName;
	              }
	            }
	            break;
	          }
	        }
	        if (id.length() < 1) {
	          id = "null";
	        }
	        if (title.length() < 1) {
	          title = "null";
	        }
	        if (class_name.length() < 1) {
	          class_name = "null";
	        }
	        if (year.length() < 1) {
	          year = "null";
	        }
	        if (orgname.length() < 1) {
	          orgname = "null";
	        }
	        if (word1.length() < 1) {
	          word1 = "null";
	        }
	        if (word2.length() < 1) {
	          word2 = "null";
	        }
	        if (word3.length() < 1) {
	          word3 = "null";
	        }
	        this.mos.write("patentData", id + "," + title + "," + class_name + 
	          "," + year + "," + orgname + "," + word1 + "," + 
	          word2 + "," + word3, NullWritable.get());
	        Text att = new Text();
	        att.set("doc-number,invention-title,classification-national,year,orgname,word1,word2,word3");
	        context.write(att, new IntWritable(1));
	        reader.close();
	      }
	      catch (Exception e)
	      {
	        throw new IOException(e);
	      }
	    }
	  }
	  