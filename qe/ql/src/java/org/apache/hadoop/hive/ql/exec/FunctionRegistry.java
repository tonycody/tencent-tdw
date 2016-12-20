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

package org.apache.hadoop.hive.ql.exec;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import org.apache.hadoop.hive.ql.udf.generic.GenericUDFArray;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFArrayContains;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.plan.exprNodeDesc;
import org.apache.hadoop.hive.ql.plan.exprNodeGenericFuncDesc;
import org.apache.hadoop.hive.serde2.typeinfo.ListTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.MapTypeInfo;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.UDAFMax;
import org.apache.hadoop.hive.ql.udf.UDAFMin;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFCorrelation;
import org.apache.hadoop.hive.ql.udf.UDFSIGN;
import org.apache.hadoop.hive.ql.udf.UDFADD_MONTHS;
import org.apache.hadoop.hive.ql.udf.UDFTRUNC;
import org.apache.hadoop.hive.ql.udf.UDFConvert;
import org.apache.hadoop.hive.ql.udf.UDFMONTHS_BETWEEN;
import org.apache.hadoop.hive.ql.udf.UDFLAST_DAY;
import org.apache.hadoop.hive.ql.udf.UDFSessionTimeZone;
import org.apache.hadoop.hive.ql.udf.UDFTz_Offset;
import org.apache.hadoop.hive.ql.udf.UDFCHR;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFLeast;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFDecode;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFBetween;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFIn;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFTimestamp;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFFromUtcTimestamp;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFToUtcTimestamp;
import org.apache.hadoop.hive.ql.udf.*;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFAverage;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFBridge;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFCount;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFParameterInfo;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFResolver2;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFStd;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFStdSample;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFSum;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFVariance;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFVarianceSample;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFnGrams;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFContextNGrams;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTFPosExplode;
import org.apache.hadoop.hive.ql.udf.generic.SimpleGenericUDAFParameterInfo;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFCovariance;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFCovarianceSample;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFCardinalityEstimation;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFCollectSet;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFMax;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFMin;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFPercentile;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFWm_concat;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFBridge;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFCase;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFCoalesce;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFElt;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFHash;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFIf;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFIndex;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFLocate;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPAnd;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqual;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqualOrGreaterThan;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqualOrLessThan;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPGreaterThan;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPLessThan;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPNot;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPNotEqual;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPNotNull;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPNull;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPOr;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFRegExpInstr;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFSize;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFSplit;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFParseXmlCount;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFParseXmlContent;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFStruct;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFTestGBK;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFUnion;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFWhen;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFSentences;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDWFAvg;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDWFCount;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDWFDenseRank;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDWFEvaluator;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDWFFirst_Value;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDWFLag;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDWFLast_Value;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDWFLead;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDWFMax;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDWFMin;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDWFRatioToReport;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDWFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDWFRowNumber;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDWFSum;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDWRank;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTFExplode;
import org.apache.hadoop.hive.serde.Constants;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFGreatest;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFUrlDecode;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFRc4;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFBitor;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFParseQQFriendFlag;

public class FunctionRegistry {

  private static Log LOG = LogFactory
      .getLog("org.apache.hadoop.hive.ql.exec.FunctionRegistry");

  static LinkedHashMap<String, FunctionInfo> mFunctions;
  static {
    mFunctions = new LinkedHashMap<String, FunctionInfo>();
    registerUDF("concat", UDFConcat.class, false);
    registerUDF("||", UDFConcat.class, true, "concat");
    registerUDF("substr", UDFSubstr.class, false);
    registerUDF("substring", UDFSubstr.class, false);
    registerUDF("space", UDFSpace.class, false);
    registerUDF("repeat", UDFRepeat.class, false);
    registerUDF("ascii", UDFAscii.class, false);
    registerUDF("lpad", UDFLpad.class, false);
    registerUDF("rpad", UDFRpad.class, false);

    registerUDF("ipinfo", UDFIpInfo.class, false);

    registerGenericUDF("size", GenericUDFSize.class);

    registerGenericUDF(Constants.TIMESTAMP_TYPE_NAME, GenericUDFTimestamp.class);
    registerGenericUDF("from_utc_timestamp", GenericUDFFromUtcTimestamp.class);
    registerGenericUDF("to_utc_timestamp", GenericUDFToUtcTimestamp.class);
    registerUDF("sign", UDFSIGN.class, false);
    registerUDF("add_months", UDFADD_MONTHS.class, false);
    registerUDF("trunc", UDFTRUNC.class, false);
    registerUDF("numformat", UDFNumberFormat.class, false);
    registerUDF("convert", UDFConvert.class, false);
    registerUDF("months_between", UDFMONTHS_BETWEEN.class, false);
    registerUDF("last_day", UDFLAST_DAY.class, false);
    registerUDF("sessiontimezone", UDFSessionTimeZone.class, false);
    registerUDF("tz_offset", UDFTz_Offset.class, false);
    registerUDF("chr", UDFCHR.class, false);
    registerUDF("between", UDFBetween.class, true);
    registerGenericUDF("least", GenericUDFLeast.class);
    registerGenericUDF("decode", GenericUDFDecode.class);
    registerGenericUDF("oneBetween", GenericUDFBetween.class);
    registerGenericUDF("in", GenericUDFIn.class);

    registerUDF("round", UDFRound.class, false);
    registerUDF("floor", UDFFloor.class, false);
    registerUDF("sqrt", UDFSqrt.class, false);
    registerUDF("ceil", UDFCeil.class, false);
    registerUDF("ceiling", UDFCeil.class, false);
    registerUDF("rand", UDFRand.class, false);
    registerUDF("abs", UDFAbs.class, false);
    registerUDF("pmod", UDFPosMod.class, false);

    registerUDF("ln", UDFLn.class, false);
    registerUDF("log2", UDFLog2.class, false);
    registerUDF("sin", UDFSin.class, false);
    registerUDF("asin", UDFAsin.class, false);
    registerUDF("cos", UDFCos.class, false);
    registerUDF("acos", UDFAcos.class, false);
    registerUDF("log10", UDFLog10.class, false);
    registerUDF("log", UDFLog.class, false);
    registerUDF("exp", UDFExp.class, false);
    registerUDF("power", UDFPower.class, false);
    registerUDF("pow", UDFPower.class, false);

    registerUDF("conv", UDFConv.class, false);
    registerUDF("bin", UDFBin.class, false);
    registerUDF("hex", UDFHex.class, false);

    registerUDF("unhex", UDFUnhex.class, false);

    registerUDF("upper", UDFUpper.class, false);
    registerUDF("lower", UDFLower.class, false);
    registerUDF("ucase", UDFUpper.class, false);
    registerUDF("lcase", UDFLower.class, false);
    registerUDF("trim", UDFTrim.class, false);
    registerUDF("ltrim", UDFLTrim.class, false);
    registerUDF("rtrim", UDFRTrim.class, false);
    registerUDF("length", UDFLength.class, false);
    registerUDF("reverse", UDFReverse.class, false);

    registerUDF("like", UDFLike.class, true);
    registerUDF("rlike", UDFRegExp.class, true);
    registerUDF("regexp", UDFRegExp.class, true);
    registerUDF("regexp_replace", UDFRegExpReplace.class, false);
    registerUDF("regexp_extract", UDFRegExpExtract.class, false);
    registerUDF("parse_url", UDFParseUrl.class, false);
    registerGenericUDF("split", GenericUDFSplit.class);
    registerGenericUDF("parse_xml_count", GenericUDFParseXmlCount.class);
    registerGenericUDF("parse_xml_content", GenericUDFParseXmlContent.class);

    registerGenericUDF("array_contains", GenericUDFArrayContains.class);
    registerGenericUDF("array", GenericUDFArray.class);

    registerUDF("positive", UDFOPPositive.class, true, "+");
    registerUDF("negative", UDFOPNegative.class, true, "-");

    registerUDF("day", UDFDayOfMonth.class, false);
    registerUDF("dayofmonth", UDFDayOfMonth.class, false);
    registerUDF("month", UDFMonth.class, false);
    registerUDF("year", UDFYear.class, false);
    registerUDF("from_unixtime", UDFFromUnixTime.class, false);
    registerUDF("unix_timestamp", UDFUnixTimeStamp.class, false);
    registerUDF("hour", UDFHour.class, false);
    registerUDF("minute", UDFMinute.class, false);
    registerUDF("second", UDFSecond.class, false);
    registerUDF("weekofyear", UDFWeekOfYear.class, false);
    registerUDF("commonStr", UDFCommonStr.class, false);
    registerUDF("base64", UDFBase64.class, false);    
    registerUDF("unbase64", UDFUnbase64.class, false);
    registerUDF("week", UDFWeek.class, false);

    registerUDF("date_add", UDFDateAdd.class, false);
    registerUDF("date_sub", UDFDateSub.class, false);
    registerUDF("datediff", UDFDateDiff.class, false);

    registerUDF("get_json_object", UDFJson.class, false);
    registerUDF("get_main_domain", UDFMainDomain.class, false); 

    registerUDF("+", UDFOPPlus.class, true);
    registerUDF("-", UDFOPMinus.class, true);
    registerUDF("*", UDFOPMultiply.class, true);
    registerUDF("/", UDFOPDivide.class, true);
    registerUDF("//", UDFOPDivideZeroReturnNull.class, true); // added by
    registerUDF("%", UDFOPMod.class, true);
    registerUDF("div", UDFOPLongDivide.class, true);

    registerUDF("&", UDFOPBitAnd.class, true);
    registerUDF("|", UDFOPBitOr.class, true);
    registerUDF("^", UDFOPBitXor.class, true);
    registerUDF("~", UDFOPBitNot.class, true);
    
    registerUDF("<<", UDFOPBitShiftLeft.class, true);
    registerUDF(">>", UDFOPBitShiftRight.class, true);

    registerUDF("comparison", UDFComparison.class, true);

    registerGenericUDF("and", GenericUDFOPAnd.class);
    registerGenericUDF("or", GenericUDFOPOr.class);
    registerGenericUDF("=", GenericUDFOPEqual.class);
    registerGenericUDF("==", GenericUDFOPEqual.class);
    registerGenericUDF("!=", GenericUDFOPNotEqual.class);
    registerGenericUDF("<>", GenericUDFOPNotEqual.class);
    registerGenericUDF("<", GenericUDFOPLessThan.class);
    registerGenericUDF("<=", GenericUDFOPEqualOrLessThan.class);
    registerGenericUDF(">", GenericUDFOPGreaterThan.class);
    registerGenericUDF(">=", GenericUDFOPEqualOrGreaterThan.class);
    registerGenericUDF("not", GenericUDFOPNot.class);
    registerGenericUDF("!", GenericUDFOPNot.class);
    registerGenericUDF("&&", GenericUDFOPAnd.class);

    registerUDF("nvl", UDFNVL.class, true, "nvl");
    registerUDF("nvl2", UDFNVL2.class, true, "nvl2");
    registerUDF("systimestamp", UDFSysTimestamp.class, true, "systimestamp");
    registerUDF("sysdate", UDFSysdate.class, true, "sysdate");
    registerUDF("instr", UDFInstr.class, true, "instr");
    registerUDF("mod", UDFMod.class, true, "mod");
    registerUDF("bitand", UDFBitand.class, true, "bitand");
    registerUDF("next_day", UDFNextDay.class, true, "next_day");
    registerUDF("to_number", UDFToNumber.class, true, "to_number");
    registerUDF("to_char", UDFToChar.class, true, "to_char");
    registerUDF("to_date", UDFToDate2.class, true, "to_date");

    registerUDF("inet_aton", UDFInet_aton.class, true, "inet_aton");
    registerUDF("inet_ntoa", UDFInet_ntoa.class, true, "inet_ntoa");
    
    registerUDF("intersect_count", UDFIntersect.class, true, "intersect_count");

    registerGenericUDF("isnull", GenericUDFOPNull.class);
    registerGenericUDF("isnotnull", GenericUDFOPNotNull.class);

    registerGenericUDF("if", GenericUDFIf.class);

    registerUDF(Constants.BOOLEAN_TYPE_NAME, UDFToBoolean.class, false,
        UDFToBoolean.class.getSimpleName());
    registerUDF(Constants.TINYINT_TYPE_NAME, UDFToByte.class, false,
        UDFToByte.class.getSimpleName());
    registerUDF(Constants.SMALLINT_TYPE_NAME, UDFToShort.class, false,
        UDFToShort.class.getSimpleName());
    registerUDF(Constants.INT_TYPE_NAME, UDFToInteger.class, false,
        UDFToInteger.class.getSimpleName());
    registerUDF(Constants.BIGINT_TYPE_NAME, UDFToLong.class, false,
        UDFToLong.class.getSimpleName());
    registerUDF(Constants.FLOAT_TYPE_NAME, UDFToFloat.class, false,
        UDFToFloat.class.getSimpleName());
    registerUDF(Constants.DOUBLE_TYPE_NAME, UDFToDouble.class, false,
        UDFToDouble.class.getSimpleName());
    registerUDF(Constants.STRING_TYPE_NAME, UDFToString.class, false,
        UDFToString.class.getSimpleName());

    registerUDF(Constants.BOOLEAN_TYPE_NAME + "1", UDF1ToBoolean.class, false,
        UDF1ToBoolean.class.getSimpleName());
    registerUDF(Constants.TINYINT_TYPE_NAME + "1", UDF1ToByte.class, false,
        UDF1ToByte.class.getSimpleName());
    registerUDF(Constants.SMALLINT_TYPE_NAME + "1", UDF1ToShort.class, false,
        UDF1ToShort.class.getSimpleName());
    registerUDF(Constants.INT_TYPE_NAME + "1", UDF1ToInteger.class, false,
        UDF1ToInteger.class.getSimpleName());
    registerUDF(Constants.BIGINT_TYPE_NAME + "1", UDF1ToLong.class, false,
        UDF1ToLong.class.getSimpleName());
    registerUDF(Constants.FLOAT_TYPE_NAME + "1", UDF1ToFloat.class, false,
        UDF1ToFloat.class.getSimpleName());
    registerUDF(Constants.DOUBLE_TYPE_NAME + "1", UDF1ToDouble.class, false,
        UDF1ToDouble.class.getSimpleName());
    registerUDF(Constants.STRING_TYPE_NAME + "1", UDF1ToString.class, false,
        UDF1ToString.class.getSimpleName());

    registerUDF("uinchange", UDFUinChange.class, false);
    registerUDF("ipchange", UDFIPChange.class, false);
    registerUDF("md5", UDFMd5Hash.class, false);
    registerUDF("str_hash_bigint", UDFBKDRHash2Long.class, false);
    registerUDF("AesEncryption", AesEncryption.class, false);
    registerUDF("AesDecryption", AesDecryption.class, false);

    registerGenericUDF("sentences", GenericUDFSentences.class);

    registerGenericUDF("create_union", GenericUDFUnion.class);
    registerGenericUDAF("sum", new GenericUDAFSum());
    registerGenericUDAF("count", new GenericUDAFCount());
    registerGenericUDAF("avg", new GenericUDAFAverage());
    registerGenericUDAF("bit_aggr_or", new GenericUDAFBitor());

    registerGenericUDAF("std", new GenericUDAFStd());
    registerGenericUDAF("stddev", new GenericUDAFStd());
    registerGenericUDAF("stddev_pop", new GenericUDAFStd());
    registerGenericUDAF("stddev_samp", new GenericUDAFStdSample());
    registerGenericUDAF("variance", new GenericUDAFVariance());
    registerGenericUDAF("var_pop", new GenericUDAFVariance());
    registerGenericUDAF("var_samp", new GenericUDAFVarianceSample());
    registerGenericUDAF("ngrams", new GenericUDAFnGrams());
    registerGenericUDAF("corr", new GenericUDAFCorrelation());
    registerGenericUDAF("covar_pop", new GenericUDAFCovariance());
    registerGenericUDAF("covar_samp", new GenericUDAFCovarianceSample());
    registerGenericUDAF("context_ngrams", new GenericUDAFContextNGrams());
    registerGenericUDAF("est_distinct", new GenericUDAFCardinalityEstimation());

    registerGenericUDAF("wm_concat", new GenericUDAFWm_concat());

    registerGenericUDAF("max", new GenericUDAFMax());
    registerGenericUDAF("min", new GenericUDAFMin());

    registerGenericUDAF("collect_set", new GenericUDAFCollectSet());

    registerGenericUDWF("lagover", new GenericUDWFLag());
    registerGenericUDWF("leadover", new GenericUDWFLead());
    registerGenericUDWF("rankover", new GenericUDWRank());
    registerGenericUDWF("dense_rankover", new GenericUDWFDenseRank());
    registerGenericUDWF("row_numberover", new GenericUDWFRowNumber());
    registerGenericUDWF("sumover", new GenericUDWFSum());
    registerGenericUDWF("countover", new GenericUDWFCount());
    registerGenericUDWF("avgover", new GenericUDWFAvg());
    registerGenericUDWF("maxover", new GenericUDWFMax());
    registerGenericUDWF("minover", new GenericUDWFMin());
    registerGenericUDWF("ratio_to_reportover", new GenericUDWFRatioToReport());
    registerGenericUDWF("first_valueover", new GenericUDWFFirst_Value());
    registerGenericUDWF("last_valueover", new GenericUDWFLast_Value());

    registerGenericUDF("struct", GenericUDFStruct.class);

    registerGenericUDF("case", GenericUDFCase.class);
    registerGenericUDF("when", GenericUDFWhen.class);
    registerGenericUDF("hash", GenericUDFHash.class);
    registerGenericUDF("coalesce", GenericUDFCoalesce.class);
    registerGenericUDF("index", GenericUDFIndex.class);
    registerGenericUDF("locate", GenericUDFLocate.class);
    registerGenericUDF("elt", GenericUDFElt.class);

    registerGenericUDTF("explode", GenericUDTFExplode.class);
    registerGenericUDTF("posexplode", GenericUDTFPosExplode.class);

    registerGenericUDF("greatest", GenericUDFGreatest.class);
    registerGenericUDF("least", GenericUDFLeast.class);
    registerGenericUDF("regexp_instr", GenericUDFRegExpInstr.class);
    registerGenericUDF("url_decode", GenericUDFUrlDecode.class);
    registerGenericUDF("parse_qqfriend_flag", GenericUDFParseQQFriendFlag.class);
    registerGenericUDF("rc4", GenericUDFRc4.class);

    registerGenericUDF("gbk2utf8", GenericUDFTestGBK.class);

    registerGenericUDAF("percentile", new GenericUDAFPercentile());
  }

  public static void registerTemporaryUDF(String functionName,
      Class<? extends UDF> UDFClass, boolean isOperator) {
    registerUDF(false, functionName, UDFClass, isOperator);
  }

  public static void registerTemporaryGenericUDTF(String functionName,
      Class<? extends GenericUDTF> genericUDTFClass) {
    registerGenericUDTF(false, functionName, genericUDTFClass);
  }

  static void registerUDF(String functionName, Class<? extends UDF> UDFClass,
      boolean isOperator) {
    registerUDF(true, functionName, UDFClass, isOperator);
  }

  public static void registerUDF(boolean isNative, String functionName,
      Class<? extends UDF> UDFClass,

      boolean isOperator) {
    registerUDF(isNative, functionName, UDFClass, isOperator,
        functionName.toLowerCase());
  }

  public static void registerUDF(String functionName,
      Class<? extends UDF> UDFClass,

      boolean isOperator, String displayName) {
    registerUDF(true, functionName, UDFClass, isOperator, displayName);
  }

  public static void registerUDF(boolean isNative, String functionName,
      Class<? extends UDF> UDFClass, boolean isOperator, String displayName) {
    if (UDF.class.isAssignableFrom(UDFClass)) {

      FunctionInfo fI = new FunctionInfo(isNative, displayName,
          new GenericUDFBridge(displayName, isOperator, UDFClass));
      mFunctions.put(functionName.toLowerCase(), fI);
    } else {
      throw new RuntimeException("Registering UDF Class " + UDFClass
          + " which does not extends " + UDF.class);
    }
  }

  public static void registerTemporaryGenericUDF(String functionName,
      Class<? extends GenericUDF> genericUDFClass) {
    registerGenericUDF(false, functionName, genericUDFClass);
  }

  static void registerGenericUDF(String functionName,
      Class<? extends GenericUDF> genericUDFClass) {
    registerGenericUDF(true, functionName, genericUDFClass);
  }

  public static void registerGenericUDF(boolean isNative, String functionName,
      Class<? extends GenericUDF> genericUDFClass) {
    if (GenericUDF.class.isAssignableFrom(genericUDFClass)) {

      FunctionInfo fI = new FunctionInfo(isNative, functionName,
          (GenericUDF) ReflectionUtils.newInstance(genericUDFClass, null));
      mFunctions.put(functionName.toLowerCase(), fI);
    } else {
      throw new RuntimeException("Registering GenericUDF Class "
          + genericUDFClass + " which does not extends " + GenericUDF.class);
    }
  }

  static void registerGenericUDTF(String functionName,
      Class<? extends GenericUDTF> genericUDTFClass) {
    registerGenericUDTF(true, functionName, genericUDTFClass);
  }

  public static void registerGenericUDTF(boolean isNative, String functionName,
      Class<? extends GenericUDTF> genericUDTFClass) {
    if (GenericUDTF.class.isAssignableFrom(genericUDTFClass)) {
      FunctionInfo fI = new FunctionInfo(isNative, functionName,
          (GenericUDTF) ReflectionUtils.newInstance(genericUDTFClass, null));
      mFunctions.put(functionName.toLowerCase(), fI);
    } else {
      throw new RuntimeException("Registering GenericUDTF Class "
          + genericUDTFClass + " which does not extend " + GenericUDTF.class);
    }
  }

  public static GenericUDTF cloneGenericUDTF(GenericUDTF genericUDTF) {
    return (GenericUDTF) ReflectionUtils.newInstance(genericUDTF.getClass(),
        null);
  }

  public static FunctionInfo getFunctionInfo(String functionName) {
    return mFunctions.get(functionName.toLowerCase());
  }

  public static Set<String> getFunctionNames() {
    return mFunctions.keySet();
  }

  public static Set<String> getFunctionNames(String funcPatternStr) {
    TreeSet<String> funcNames = new TreeSet<String>();
    Pattern funcPattern = null;
    try {
      funcPattern = Pattern.compile(funcPatternStr);
    } catch (PatternSyntaxException e) {
      return funcNames;
    }
    for (String funcName : mFunctions.keySet()) {
      if (funcPattern.matcher(funcName).matches()) {
        funcNames.add(funcName);
      }
    }
    return funcNames;
  }

  static Map<TypeInfo, Integer> numericTypes = new HashMap<TypeInfo, Integer>();
  static List<TypeInfo> numericTypeList = new ArrayList<TypeInfo>();

  static void registerNumericType(String typeName, int level) {
    TypeInfo t = TypeInfoFactory.getPrimitiveTypeInfo(typeName);
    numericTypeList.add(t);
    numericTypes.put(t, level);
  }

  static {
    registerNumericType(Constants.TINYINT_TYPE_NAME, 1);
    registerNumericType(Constants.SMALLINT_TYPE_NAME, 2);
    registerNumericType(Constants.INT_TYPE_NAME, 3);
    registerNumericType(Constants.BIGINT_TYPE_NAME, 4);
    registerNumericType(Constants.FLOAT_TYPE_NAME, 5);
    registerNumericType(Constants.DOUBLE_TYPE_NAME, 6);
    registerNumericType(Constants.STRING_TYPE_NAME, 7);
  }

  public static TypeInfo getCommonClassForComparison(TypeInfo a, TypeInfo b) {
    if (a.equals(b))
      return a;

    for (TypeInfo t : numericTypeList) {
      if (FunctionRegistry.implicitConvertable(a, t)
          && FunctionRegistry.implicitConvertable(b, t)) {
        return t;
      }
    }
    return null;
  }

  public static TypeInfo getCommonClass(TypeInfo a, TypeInfo b) {
    if (a.equals(b)) {
      return a;
    }
    Integer ai = numericTypes.get(a);
    Integer bi = numericTypes.get(b);
    if (ai == null || bi == null) {
      return null;
    }
    return (ai > bi) ? a : b;
  }

  public static boolean implicitConvertable(TypeInfo from, TypeInfo to) {
    if (from.equals(to)) {
      return true;
    }
    if (from.equals(TypeInfoFactory.stringTypeInfo)
        && to.equals(TypeInfoFactory.doubleTypeInfo)) {
      return true;
    }
    if (from.equals(TypeInfoFactory.voidTypeInfo)) {
      return true;
    }

    if (from.equals(TypeInfoFactory.timestampTypeInfo)
        && to.equals(TypeInfoFactory.stringTypeInfo)) {
      return true;
    }
    Integer f = numericTypes.get(from);
    Integer t = numericTypes.get(to);
    if (f == null || t == null)
      return false;
    if (f.intValue() > t.intValue())
      return false;
    return true;
  }

  public static GenericUDAFEvaluator getGenericUDAFEvaluator(String name,
      List<TypeInfo> argumentTypeInfos, boolean isDistinct, boolean isAllColumns)
      throws SemanticException {

    GenericUDAFResolver udafResolver = getGenericUDAFResolver(name);
    if (udafResolver == null) {
      return null;
    }

    TypeInfo[] parameters = new TypeInfo[argumentTypeInfos.size()];
    for (int i = 0; i < parameters.length; i++) {
      parameters[i] = argumentTypeInfos.get(i);
    }

    GenericUDAFEvaluator udafEvaluator = null;
    if (udafResolver instanceof GenericUDAFResolver2) {
      GenericUDAFParameterInfo paramInfo = new SimpleGenericUDAFParameterInfo(
          parameters, isDistinct, isAllColumns);
      udafEvaluator = ((GenericUDAFResolver2) udafResolver)
          .getEvaluator(paramInfo);
    } else {
      udafEvaluator = udafResolver.getEvaluator(parameters);
    }
    return udafEvaluator;
  }

  public static GenericUDWFEvaluator getGenericUDWFEvaluator(String name,
      List<TypeInfo> argumentTypeInfos, boolean isDistinct, boolean isOrderBy)
      throws SemanticException {

    GenericUDWFResolver udwfResolver = getGenericUDWFResolver(name);
    if (udwfResolver == null) {
      return null;
    }

    TypeInfo[] parameters = null;
    if (argumentTypeInfos != null) {
      parameters = new TypeInfo[argumentTypeInfos.size()];
      for (int i = 0; i < parameters.length; i++) {
        parameters[i] = argumentTypeInfos.get(i);
      }
    }

    GenericUDWFEvaluator udwfEvaluator = null;
    udwfEvaluator = udwfResolver.getEvaluator(parameters);
    return udwfEvaluator;
  }

  public static GenericUDWFResolver getGenericUDWFResolver(String functionName) {
    LOG.debug("Looking up GenericUDWF: " + functionName);
    FunctionInfo finfo = mFunctions.get(functionName.toLowerCase());
    if (finfo == null) {
      return null;
    }
    GenericUDWFResolver result = finfo.getGenericUDWFResolver();
    return result;
  }

  public static <T> Method getMethodInternal(Class<? extends T> udfClass,
      String methodName, boolean exact, List<TypeInfo> argumentClasses) {

    ArrayList<Method> mlist = new ArrayList<Method>();

    for (Method m : Arrays.asList(udfClass.getMethods())) {
      if (m.getName().equals(methodName)) {
        mlist.add(m);
      }
    }

    return getMethodInternal(mlist, exact, argumentClasses);
  }

  public static int matchCost(TypeInfo argumentPassed,
      TypeInfo argumentAccepted, boolean exact) {
    if (argumentAccepted.equals(argumentPassed)) {
      return 0;
    }
    if (argumentPassed.equals(TypeInfoFactory.voidTypeInfo)) {
      return 0;
    }
    if (argumentPassed.getCategory().equals(Category.LIST)
        && argumentAccepted.getCategory().equals(Category.LIST)) {
      TypeInfo argumentPassedElement = ((ListTypeInfo) argumentPassed)
          .getListElementTypeInfo();
      TypeInfo argumentAcceptedElement = ((ListTypeInfo) argumentAccepted)
          .getListElementTypeInfo();
      return matchCost(argumentPassedElement, argumentAcceptedElement, exact);
    }
    if (argumentPassed.getCategory().equals(Category.MAP)
        && argumentAccepted.getCategory().equals(Category.MAP)) {
      TypeInfo argumentPassedKey = ((MapTypeInfo) argumentPassed)
          .getMapKeyTypeInfo();
      TypeInfo argumentAcceptedKey = ((MapTypeInfo) argumentAccepted)
          .getMapKeyTypeInfo();
      TypeInfo argumentPassedValue = ((MapTypeInfo) argumentPassed)
          .getMapValueTypeInfo();
      TypeInfo argumentAcceptedValue = ((MapTypeInfo) argumentAccepted)
          .getMapValueTypeInfo();
      int cost1 = matchCost(argumentPassedKey, argumentAcceptedKey, exact);
      int cost2 = matchCost(argumentPassedValue, argumentAcceptedValue, exact);
      if (cost1 < 0 || cost2 < 0)
        return -1;
      return Math.max(cost1, cost2);
    }

    if (argumentAccepted.equals(TypeInfoFactory.unknownTypeInfo)) {
      return 1;
    }
    if (!exact && implicitConvertable(argumentPassed, argumentAccepted)) {
      return 1;
    }

    return -1;
  }

  public static void registerTemporaryGenericUDAF(String functionName,
      GenericUDAFResolver genericUDAFResolver) {
    registerGenericUDAF(false, functionName, genericUDAFResolver);
  }

  static void registerGenericUDAF(String functionName,
      GenericUDAFResolver genericUDAFResolver) {
    registerGenericUDAF(true, functionName, genericUDAFResolver);
  }

  public static void registerGenericUDAF(boolean isNative, String functionName,
      GenericUDAFResolver genericUDAFResolver) {
    mFunctions.put(functionName.toLowerCase(), new FunctionInfo(isNative,
        functionName.toLowerCase(), genericUDAFResolver));
  }

  static void registerGenericUDWF(String functionName,
      GenericUDWFResolver genericUDWFResolver) {
    registerGenericUDWF(true, functionName, genericUDWFResolver);
  }

  public static void registerGenericUDWF(boolean isNative, String functionName,
      GenericUDWFResolver genericUDWFResolver) {
    mFunctions.put(functionName.toLowerCase(), new FunctionInfo(isNative,
        functionName.toLowerCase(), genericUDWFResolver));
  }

  public static void registerTemporaryUDAF(String functionName,
      Class<? extends UDAF> udafClass) {
    registerUDAF(false, functionName, udafClass);
  }

  static void registerUDAF(String functionName, Class<? extends UDAF> udafClass) {
    registerUDAF(true, functionName, udafClass);
  }

  public static void registerUDAF(boolean isNative, String functionName,
      Class<? extends UDAF> udafClass) {
    mFunctions.put(functionName.toLowerCase(), new FunctionInfo(isNative,
        functionName.toLowerCase(), new GenericUDAFBridge(
            (UDAF) ReflectionUtils.newInstance(udafClass, null))));
  }

  public static void unregisterTemporaryUDF(String functionName)
      throws HiveException {
    FunctionInfo fi = mFunctions.get(functionName.toLowerCase());
    if (fi != null) {
      if (!fi.isNative())
        mFunctions.remove(functionName.toLowerCase());
      else
        throw new HiveException("Function " + functionName
            + " is hive native, it can't be dropped");
    }
  }

  public static GenericUDAFResolver getGenericUDAFResolver(String functionName) {
    LOG.debug("Looking up GenericUDAF: " + functionName);
    FunctionInfo finfo = mFunctions.get(functionName.toLowerCase());
    if (finfo == null) {
      return null;
    }
    GenericUDAFResolver result = finfo.getGenericUDAFResolver();
    return result;
  }

  public static Object invoke(Method m, Object thisObject, Object... arguments)
      throws HiveException {
    Object o;
    try {
      o = m.invoke(thisObject, arguments);
    } catch (Exception e) {
      String thisObjectString = "" + thisObject + " of class "
          + (thisObject == null ? "null" : thisObject.getClass().getName());

      StringBuilder argumentString = new StringBuilder();
      if (arguments == null) {
        argumentString.append("null");
      } else {
        argumentString.append("{");
        for (int i = 0; i < arguments.length; i++) {
          if (i > 0) {
            argumentString.append(", ");
          }
          if (arguments[i] == null) {
            argumentString.append("null");
          } else {
            argumentString.append("" + arguments[i] + ":"
                + arguments[i].getClass().getName());
          }
        }
        argumentString.append("} of size " + arguments.length);
      }

      throw new HiveException("Unable to execute method " + m + " "
          + " on object " + thisObjectString + " with arguments "
          + argumentString.toString(), e);
    }
    return o;
  }

  public static Method getMethodInternal(ArrayList<Method> mlist,
      boolean exact, List<TypeInfo> argumentsPassed) {
    int leastConversionCost = Integer.MAX_VALUE;
    Method udfMethod = null;

    for (Method m : mlist) {
      List<TypeInfo> argumentsAccepted = TypeInfoUtils.getParameterTypeInfos(m,
          argumentsPassed.size());
      if (argumentsAccepted == null) {
        continue;
      }

      boolean match = (argumentsAccepted.size() == argumentsPassed.size());
      int conversionCost = 0;

      for (int i = 0; i < argumentsPassed.size() && match; i++) {

        int cost = matchCost(argumentsPassed.get(i), argumentsAccepted.get(i),
            exact);
        if (cost == -1) {
          match = false;
        } else {
          conversionCost += cost;
        }
      }

      //LOG.info("Method " + (match ? "did" : "didn't") + " match: passed = "
      //    + argumentsPassed + " accepted = " + argumentsAccepted + " method = "
      //    + m);
      if (match) {
        if (conversionCost < leastConversionCost) {
          udfMethod = m;
          leastConversionCost = conversionCost;
          if (leastConversionCost == 0)
            break;
        } else if (conversionCost == leastConversionCost) {
          LOG.info("Ambigious methods: passed = " + argumentsPassed
              + " method 1 = " + udfMethod + " method 2 = " + m);
          udfMethod = null;
        } else {
        }
      }
    }
    return udfMethod;
  }

  public static GenericUDF getGenericUDFForIndex() {
    return FunctionRegistry.getFunctionInfo("index").getGenericUDF();
  }

  public static GenericUDF getGenericUDFForAnd() {
    return FunctionRegistry.getFunctionInfo("and").getGenericUDF();
  }

  public static GenericUDF cloneGenericUDF(GenericUDF genericUDF) {
    if (genericUDF instanceof GenericUDFBridge) {
      GenericUDFBridge bridge = (GenericUDFBridge) genericUDF;
      return new GenericUDFBridge(bridge.getUdfName(), bridge.isOperator(),
          bridge.getUdfClass());
    } else {
      return (GenericUDF) ReflectionUtils.newInstance(genericUDF.getClass(),
          null);
    }
  }

  private static Class<? extends UDF> getUDFClassFromExprDesc(exprNodeDesc desc) {
    if (!(desc instanceof exprNodeGenericFuncDesc)) {
      return null;
    }
    exprNodeGenericFuncDesc genericFuncDesc = (exprNodeGenericFuncDesc) desc;
    if (!(genericFuncDesc.getGenericUDF() instanceof GenericUDFBridge)) {
      return null;
    }
    GenericUDFBridge bridge = (GenericUDFBridge) (genericFuncDesc
        .getGenericUDF());
    return bridge.getUdfClass();
  }

  public static boolean isDeterministic(GenericUDF genericUDF) {
    UDFType genericUDFType = genericUDF.getClass().getAnnotation(UDFType.class);
    if (genericUDFType != null && genericUDFType.deterministic() == false) {
      return false;
    }

    if (genericUDF instanceof GenericUDFBridge) {
      GenericUDFBridge bridge = (GenericUDFBridge) (genericUDF);
      UDFType bridgeUDFType = bridge.getUdfClass().getAnnotation(UDFType.class);
      if (bridgeUDFType != null && bridgeUDFType.deterministic() == false) {
        return false;
      }
    }

    return true;
  }

  private static Class<? extends GenericUDF> getGenericUDFClassFromExprDesc(
      exprNodeDesc desc) {
    if (!(desc instanceof exprNodeGenericFuncDesc)) {
      return null;
    }
    exprNodeGenericFuncDesc genericFuncDesc = (exprNodeGenericFuncDesc) desc;
    return genericFuncDesc.getGenericUDF().getClass();
  }

  public static boolean isOpAndOrNot(exprNodeDesc desc) {
    Class<? extends GenericUDF> genericUdfClass = getGenericUDFClassFromExprDesc(desc);
    return GenericUDFOPAnd.class == genericUdfClass
        || GenericUDFOPOr.class == genericUdfClass
        || GenericUDFOPNot.class == genericUdfClass;
  }

  public static boolean isOpAnd(exprNodeDesc desc) {
    return GenericUDFOPAnd.class == getGenericUDFClassFromExprDesc(desc);
  }

  public static boolean isOpPositive(exprNodeDesc desc) {
    Class<? extends UDF> udfClass = getUDFClassFromExprDesc(desc);
    return UDFOPPositive.class == udfClass;
  }

  public static boolean isOpOr(exprNodeDesc desc) {
    return GenericUDFOPOr.class == getGenericUDFClassFromExprDesc(desc);
  }

  public static boolean isOpNot(exprNodeDesc desc) {
    return GenericUDFOPNot.class == getGenericUDFClassFromExprDesc(desc);
  }

  public static boolean isOpEqual(exprNodeDesc desc) {
    return GenericUDFOPEqual.class == getGenericUDFClassFromExprDesc(desc);
  }

  public static boolean isOpEqualOrGreaterThan(exprNodeDesc desc) {
    return GenericUDFOPEqualOrGreaterThan.class == getGenericUDFClassFromExprDesc(desc);
  }

  public static boolean isOpEqualOrLessThan(exprNodeDesc desc) {
    return GenericUDFOPEqualOrLessThan.class == getGenericUDFClassFromExprDesc(desc);
  }

  public static boolean isOpGreaterThan(exprNodeDesc desc) {
    return GenericUDFOPGreaterThan.class == getGenericUDFClassFromExprDesc(desc);
  }

  public static boolean isOpLessThan(exprNodeDesc desc) {
    return GenericUDFOPLessThan.class == getGenericUDFClassFromExprDesc(desc);
  }
    public static boolean isOpIn(exprNodeDesc desc)   {          return GenericUDFIn.class == getGenericUDFClassFromExprDesc(desc);         }    public static boolean isFuncRunningCompute(exprNodeDesc desc)   {          return ((UDFRand.class == getUDFClassFromExprDesc(desc))        || UDFSysTimestamp.class == getUDFClassFromExprDesc(desc));        }  }
