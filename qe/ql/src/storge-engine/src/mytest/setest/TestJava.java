/**
* Tencent is pleased to support the open source community by making TDW available.
* Copyright (C) 2014 THL A29 Limited, a Tencent company. All rights reserved.
* Licensed under the Apache License, Version 2.0 (the "License"); you may not use 
* this file except in compliance with the License. You may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software distributed 
* under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS 
* OF ANY KIND, either express or implied. See the License for the specific language governing
* permissions and limitations under the License.
*/

public class TestJava
{
	static public void main(String[] argv)
	{
		String str1 = "hello konten";

	    System.out.println("str1.len:"+str1.length()+"str1.value:"+str1);

	    byte[] bytes = str1.getBytes();
	    System.out.println("bytes.len:"+bytes.length);
	    
	    String str2 = new String(bytes);
	    
	    System.out.println("str2.len"+str2.length()+"str2.value:"+str2);
	    
	    String str3 = "1;2;3";
	    String[] field = str3.split(";");
	    for(int i = 0; i < field.length; i++)
	    {
	    	System.out.println("i: "+field[i]);
	    }
	}      
	  

}
