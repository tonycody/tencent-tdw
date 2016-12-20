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
/*
 * =====================================================================================
 *
 *       Filename:  tdw_helpers.h
 *
 *    Description:  
 *
 *        Version:  1.0
 *        Created:  12/20/2010 04:30:46 PM
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  Xinjie Li (Engineer), xinjieli@tencent.com
 *        Company:  Tencent
 *
 * =====================================================================================
 */

#ifndef TDW_HELPERS_H__
#define TDW_HELPERS_H__

#include <google/protobuf/stubs/common.h>
#include <google/protobuf/descriptor.h>
#include <algorithm>
#include <limits>
#include <cstdio>

namespace google {
namespace protobuf {
namespace compiler {
namespace tdw {

// Commonly-used separator comments.  Thick is a line of '=', thin is a line
// of '-'.
extern const char kThickSeparator[];                                                                                                                                                                      
extern const char kThinSeparator[];

// Strips ".proto" or ".protodevel" from the end of a filename.
string StripProto(const string& filename);

vector<const Descriptor*> GetNestedMessages(const Descriptor* message);

// Get package prefixed name of the message in a dependent file.
string GetDependentMessageName(const Descriptor* message);

// Get field type (message) name.
string GetFieldMessageName(const Descriptor* field_message,
    const Descriptor* containing_message,
    bool containing_message_in_dependency);

void GetMessageNameHierarchy(const Descriptor* message, bool in_dependency, vector<string>* res);
vector<string> GetMessageNameHierarchy(const Descriptor* message, bool in_dependency);


// Get code that evaluates to the field's default value.
string DefaultValue(const FieldDescriptor* field);

inline char ToLower(char c) {
  if ('A' <= c && c <= 'Z') {
    return c + 'a' - 'A';
  }
  return c;
}
inline char ToUpper(char c) {
  if ('a' <= c && c <= 'z') {
    return c + 'A' - 'a';
  }
  return c;
}

inline string ToLower(const string& s) {
  string r;
  std::transform(s.begin(), s.end(), back_inserter(r), static_cast<char(*)(char)>(&ToLower));
  return r;
}
inline string ToUpper(const string& s) {
  string r;
  std::transform(s.begin(), s.end(), back_inserter(r), static_cast<char(*)(char)>(&ToLower));
  return r;
}

inline string ToGroupName(const string& s) {
  string r;
  string::const_iterator pos = s.begin(), end = s.end();
  if (pos != end) {
    r.push_back(ToUpper(*(pos++)));
    std::transform(pos, end, back_inserter(r), static_cast<char(*)(char)>(&ToLower));
  }
  return r;
}

// ----------------------------------------------------------------------
// CEscapeString()
//    Copies 'src' to 'dest', escaping dangerous characters using
//    C-style escape sequences. This is very useful for preparing query
//    flags. 'src' and 'dest' should not overlap.
//    Returns the number of bytes written to 'dest' (not including the \0)
//    or -1 if there was insufficient space.
//
//    Currently only \n, \r, \t, ", ', \ and !isprint() chars are escaped.
// ----------------------------------------------------------------------
LIBPROTOBUF_EXPORT int CEscapeString(const char* src, int src_len,
                                     char* dest, int dest_len);

// ----------------------------------------------------------------------
// CEscape()
//    More convenient form of CEscapeString: returns result as a "string".
//    This version is slower than CEscapeString() because it does more
//    allocation.  However, it is much more convenient to use in
//    non-speed-critical code like logging messages etc.
// ----------------------------------------------------------------------
LIBPROTOBUF_EXPORT string CEscape(const string& src);

namespace strings {
// Like CEscape() but does not escape bytes with the upper bit set.
LIBPROTOBUF_EXPORT string Utf8SafeCEscape(const string& src);

// Like CEscape() but uses hex (\x) escapes instead of octals.
LIBPROTOBUF_EXPORT string CHexEscape(const string& src);
}  // namespace strings


// ----------------------------------------------------------------------
// HasSuffixString()
//    Return true if str ends in suffix.
// StripSuffixString()
//    Given a string and a putative suffix, returns the string minus the
//    suffix string if the suffix matches, otherwise the original
//    string.
// ----------------------------------------------------------------------
inline bool HasSuffixString(const string& str,
                            const string& suffix) {
  return str.size() >= suffix.size() &&
         str.compare(str.size() - suffix.size(), suffix.size(), suffix) == 0;
}

inline string StripSuffixString(const string& str, const string& suffix) {
  if (HasSuffixString(str, suffix)) {
    return str.substr(0, str.size() - suffix.size());
  } else {
    return str;
  }
}

// ----------------------------------------------------------------------
// StringReplace()
//    Give me a string and two patterns "old" and "new", and I replace
//    the first instance of "old" in the string with "new", if it
//    exists.  RETURN a new string, regardless of whether the replacement
//    happened or not.
// ----------------------------------------------------------------------

LIBPROTOBUF_EXPORT string StringReplace(const string& s, const string& oldsub,
                                        const string& newsub, bool replace_all);

// ----------------------------------------------------------------------
// JoinStrings()
//    These methods concatenate a vector of strings into a C++ string, using
//    the C-string "delim" as a separator between components. There are two
//    flavors of the function, one flavor returns the concatenated string,
//    another takes a pointer to the target string. In the latter case the
//    target string is cleared and overwritten.
// ----------------------------------------------------------------------
LIBPROTOBUF_EXPORT void JoinStrings(const vector<string>& components,
                                    const char* delim, string* result);

inline string JoinStrings(const vector<string>& components,
                          const char* delim) {
  string result;
  JoinStrings(components, delim, &result);
  return result;
}

// ----------------------------------------------------------------------
// FastIntToBuffer()
// FastHexToBuffer()
// FastHex64ToBuffer()
// FastHex32ToBuffer()
// FastTimeToBuffer()
//    These are intended for speed.  FastIntToBuffer() assumes the
//    integer is non-negative.  FastHexToBuffer() puts output in
//    hex rather than decimal.  FastTimeToBuffer() puts the output
//    into RFC822 format.
//
//    FastHex64ToBuffer() puts a 64-bit unsigned value in hex-format,
//    padded to exactly 16 bytes (plus one byte for '\0')
//
//    FastHex32ToBuffer() puts a 32-bit unsigned value in hex-format,
//    padded to exactly 8 bytes (plus one byte for '\0')
//
//       All functions take the output buffer as an arg.
//    They all return a pointer to the beginning of the output,
//    which may not be the beginning of the input buffer.
// ----------------------------------------------------------------------

// Suggested buffer size for FastToBuffer functions.  Also works with
// DoubleToBuffer() and FloatToBuffer().
static const int kFastToBufferSize = 32;

LIBPROTOBUF_EXPORT char* FastInt32ToBuffer(int32 i, char* buffer);
LIBPROTOBUF_EXPORT char* FastInt64ToBuffer(int64 i, char* buffer);
char* FastUInt32ToBuffer(uint32 i, char* buffer);  // inline below
char* FastUInt64ToBuffer(uint64 i, char* buffer);  // inline below
LIBPROTOBUF_EXPORT char* FastHexToBuffer(int i, char* buffer);
LIBPROTOBUF_EXPORT char* FastHex64ToBuffer(uint64 i, char* buffer);
LIBPROTOBUF_EXPORT char* FastHex32ToBuffer(uint32 i, char* buffer);

// at least 22 bytes long
inline char* FastIntToBuffer(int i, char* buffer) {
  return (sizeof(i) == 4 ?
          FastInt32ToBuffer(i, buffer) : FastInt64ToBuffer(i, buffer));
}
inline char* FastUIntToBuffer(unsigned int i, char* buffer) {
  return (sizeof(i) == 4 ?
          FastUInt32ToBuffer(i, buffer) : FastUInt64ToBuffer(i, buffer));
}
inline char* FastLongToBuffer(long i, char* buffer) {
  return (sizeof(i) == 4 ?
          FastInt32ToBuffer(i, buffer) : FastInt64ToBuffer(i, buffer));
}
inline char* FastULongToBuffer(unsigned long i, char* buffer) {
  return (sizeof(i) == 4 ?
          FastUInt32ToBuffer(i, buffer) : FastUInt64ToBuffer(i, buffer));
}

// ----------------------------------------------------------------------
// FastInt32ToBufferLeft()
// FastUInt32ToBufferLeft()
// FastInt64ToBufferLeft()
// FastUInt64ToBufferLeft()
//
// Like the Fast*ToBuffer() functions above, these are intended for speed.
// Unlike the Fast*ToBuffer() functions, however, these functions write
// their output to the beginning of the buffer (hence the name, as the
// output is left-aligned).  The caller is responsible for ensuring that
// the buffer has enough space to hold the output.
//
// Returns a pointer to the end of the string (i.e. the null character
// terminating the string).
// ----------------------------------------------------------------------

LIBPROTOBUF_EXPORT char* FastInt32ToBufferLeft(int32 i, char* buffer);
LIBPROTOBUF_EXPORT char* FastUInt32ToBufferLeft(uint32 i, char* buffer);
LIBPROTOBUF_EXPORT char* FastInt64ToBufferLeft(int64 i, char* buffer);
LIBPROTOBUF_EXPORT char* FastUInt64ToBufferLeft(uint64 i, char* buffer);

// Just define these in terms of the above.
inline char* FastUInt32ToBuffer(uint32 i, char* buffer) {
  FastUInt32ToBufferLeft(i, buffer);
  return buffer;
}
inline char* FastUInt64ToBuffer(uint64 i, char* buffer) {
  FastUInt64ToBufferLeft(i, buffer);
  return buffer;
}

// ----------------------------------------------------------------------
// SimpleItoa()
//    Description: converts an integer to a string.
//
//    Return value: string
// ----------------------------------------------------------------------
LIBPROTOBUF_EXPORT string SimpleItoa(int i);
LIBPROTOBUF_EXPORT string SimpleItoa(unsigned int i);
LIBPROTOBUF_EXPORT string SimpleItoa(long i);
LIBPROTOBUF_EXPORT string SimpleItoa(unsigned long i);
LIBPROTOBUF_EXPORT string SimpleItoa(long long i);
LIBPROTOBUF_EXPORT string SimpleItoa(unsigned long long i);

// ----------------------------------------------------------------------
// SimpleDtoa()
// SimpleFtoa()
// DoubleToBuffer()
// FloatToBuffer()
//    Description: converts a double or float to a string which, if
//    passed to NoLocaleStrtod(), will produce the exact same original double
//    (except in case of NaN; all NaNs are considered the same value).
//    We try to keep the string short but it's not guaranteed to be as
//    short as possible.
//
//    DoubleToBuffer() and FloatToBuffer() write the text to the given
//    buffer and return it.  The buffer must be at least
//    kDoubleToBufferSize bytes for doubles and kFloatToBufferSize
//    bytes for floats.  kFastToBufferSize is also guaranteed to be large
//    enough to hold either.
//
//    Return value: string
// ----------------------------------------------------------------------
LIBPROTOBUF_EXPORT string SimpleDtoa(double value);
LIBPROTOBUF_EXPORT string SimpleFtoa(float value);

LIBPROTOBUF_EXPORT char* DoubleToBuffer(double i, char* buffer);
LIBPROTOBUF_EXPORT char* FloatToBuffer(float i, char* buffer);

// In practice, doubles should never need more than 24 bytes and floats
// should never need more than 14 (including null terminators), but we
// overestimate to be safe.
static const int kDoubleToBufferSize = 32;
static const int kFloatToBufferSize = 24;

} // namespace tdw
} // namespace compiler
} // namespace protobuf
} // namespace google

#endif // TDW_HELPERS_H__

