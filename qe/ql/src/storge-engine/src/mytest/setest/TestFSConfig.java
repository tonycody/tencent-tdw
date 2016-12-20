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

import org.apache.hadoop.conf.Configuration;

import Comm.ConstVar;

public class TestFSConfig
{
    public static void main(String[] argv)
    {
        Configuration conf = new Configuration();
        conf.addResource(ConstVar.FormatStorageConf);
        
        int segmentSize = conf.getInt(ConstVar.ConfSegmentSize, -1);
        int unitSize = conf.getInt(ConstVar.ConfUnitSize, -2);
        int poolSize = conf.getInt(ConstVar.ConfPoolSize, -3);
        
        System.out.println("seg:"+segmentSize+",unit:"+unitSize+",pool:"+poolSize);
    }
}
