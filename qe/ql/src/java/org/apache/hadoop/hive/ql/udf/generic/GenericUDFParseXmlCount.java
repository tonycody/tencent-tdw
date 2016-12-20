/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.ql.udf.generic;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.exec.description;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableStringObjectInspector;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

@description(name = "parse_xml_count", value = "_FUNC_(strXml, tag1, ...) - get the label count in strXml "
    + "the tags must start from the root tag layer-by-layer", extended = "Example:\n"
    + "  > SELECT _FUNC_('<a><b id='b1'><c id='c1'></c><c id='c2'></c></b><b id='b2'></b></a>', 'a', 'b id='b1'', 'c id='c1'') FROM src LIMIT 1;\n"
    + "  1")
public class GenericUDFParseXmlCount extends GenericUDF {
  private ObjectInspectorConverters.Converter[] converters;
  private IntWritable result = new IntWritable(0);

  public ObjectInspector initialize(ObjectInspector[] arguments)
      throws UDFArgumentException {
    if (arguments.length < 2) {
      throw new UDFArgumentLengthException(
          "The function parse_xml_count(strXml, label, ...) takes more than 1 argument.");
    }

    converters = new ObjectInspectorConverters.Converter[arguments.length];
    for (int i = 0; i < arguments.length; i++) {
      converters[i] = ObjectInspectorConverters.getConverter(arguments[i],
          PrimitiveObjectInspectorFactory.writableStringObjectInspector);
    }

    return PrimitiveObjectInspectorFactory.writableIntObjectInspector;
  }

  public Object evaluate(DeferredObject[] arguments) throws HiveException {
    assert (arguments.length >= 2);

    String strs[] = new String[arguments.length];
    for (int i = 0; i < arguments.length; i++) {
      if (arguments[i].get() == null)
    	 return null;
      strs[i] = ((Text)converters[i].convert(arguments[i].get())).toString();
      if (strs[i].trim().equals(""))
        return null;
    }

    int tagReCount = 0;

    int tagCount = strs.length - 1;
    int attrCount;

    // get the labels and attributes of each label
    String tag[] = new String[tagCount];
    String attrs[][] = new String[tagCount][];

    String str_tmp[];
    for (int i = 1; i <= tagCount; i++) {
        strs[i] = strs[i].trim();
        strs[i] = strs[i].replaceAll("\\s{0,}=\\s{0,}'\\s{0,}", "='");
        strs[i] = strs[i].replaceAll("\\s{1,}'", "'");

        strs[i] = strs[i].replaceAll("\\s{0,}=\\s{0,}\"\\s{0,}", "=\"");
        strs[i] = strs[i].replaceAll("\\s{1,}\"", "\"");
        
        strs[i] = strs[i].replaceAll("\\s{1,}", " ");
        str_tmp = strs[i].split(" ");
        attrCount = str_tmp.length - 1;

        tag[i-1] = str_tmp[0];

        attrs[i-1] = new String[attrCount];
        System.arraycopy(str_tmp, 1, attrs[i-1], 0, attrCount);
    }

    // begin to parse
    try {
        DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
        DocumentBuilder db = dbf.newDocumentBuilder();
        Document document = db.parse(new InputSource(new StringReader(
                strs[0])));

        NodeList nl = document.getChildNodes();
        tagReCount = countTag(nl, 0, tagCount, tag, attrs);

    } catch (ParserConfigurationException e) {
        throw new HiveException("failed to get documentBuilder");
    } catch (SAXException e) {
        throw new UDFArgumentException("invalid xml content");
    } catch (IOException e) {
      e.printStackTrace();
    }

    if (tagReCount == -1)
      throw new UDFArgumentException("invalid parse_xml_count arguments");
    
    result.set(tagReCount);

    return result;
  }

  public String getDisplayString(String[] children) {
    assert (children.length >= 2);
    return "parse_xml_count(strXml, ...)";
  }
  
  private int countTag(NodeList nl, int index, int tagCount, String[] tag, String[][] attrs) {
    int count = 0;
    for (int i = 0; i < nl.getLength(); i++) {
      int matchCount = 0;
      Node type = nl.item(i);
      if ( !tag[index].equalsIgnoreCase(type.getNodeName()) ) {
        continue;
      }
      for (int attrIndex = 0; attrIndex < attrs[index].length; attrIndex++) {
        String attrPair[] = attrs[index][attrIndex].split("=");
        if (attrPair.length != 2 || attrPair[1].trim().length() < 2 || !(attrPair[1].trim().startsWith("\"") && attrPair[1].trim().endsWith("\"")) && !(attrPair[1].trim().startsWith("'") && attrPair[1].trim().endsWith("'")) ) {
          return -1;
        }
        String attrValue = ((Element) type).getAttribute(attrPair[0]);
        if (attrValue != null
            && attrValue.equals(attrPair[1].substring(1,
                attrPair[1].length() - 1))) {
          matchCount++;
        }
      }

      if (matchCount == attrs[index].length) {
        if (index == tagCount-1) {
          count++;
        } else {
          int tmp_count = countTag(type.getChildNodes(), index+1, tagCount, tag, attrs);
          if (tmp_count == -1)
            return -1;
          count += tmp_count;
        }
      }
    }
    return count;
  }

}
