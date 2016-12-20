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
 *       Filename:  tdw_helpers.cc
 *
 *    Description:  
 *
 *        Version:  1.0
 *        Created:  12/20/2010 04:38:16 PM
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  Xinjie Li (Engineer), xinjieli@tencent.com
 *        Company:  Tencent
 *
 * =====================================================================================
 */

#include <google/protobuf/compiler/tdw/tdw_helpers.h>
#include <deque>
#include <set>
#include <float.h>
#include <errno.h>

namespace google {
namespace protobuf {
namespace compiler {
namespace tdw {

const char kThickSeparator[] =
  "// ===================================================================\n";                        
const char kThinSeparator[] =                                                                        
  "// -------------------------------------------------------------------\n"; 

string StripProto(const string& filename) {
  if (HasSuffixString(filename, ".protodevel")) {
    return StripSuffixString(filename, ".protodevel");
  } else {
    return StripSuffixString(filename, ".proto");
  }
}

vector<const Descriptor*> GetNestedMessages(const Descriptor* message) {
  std::set<string> group_names;
  for (int i=0; i<message->field_count(); i++) {
    const FieldDescriptor* field = message->field(i);
    if (field->type() == FieldDescriptor::TYPE_GROUP) {
      group_names.insert(field->message_type()->name());
    }
  }
  vector<const Descriptor*> nested_messages;
  for (int i=0; i<message->nested_type_count(); i++) {
    const Descriptor* nested_message = message->nested_type(i);
    if (group_names.find(nested_message->name()) == group_names.end()) {
      nested_messages.push_back(nested_message);
    }
  }
  return nested_messages;
}

void GetMessageNameHierarchy(const Descriptor* message, bool in_dependency, vector<string>* res) {
  std::deque<string> names;
  while (message->containing_type()) {
    names.push_front(message->name());
    message = message->containing_type();
  }
  if (in_dependency) {
    names.push_front(StringReplace(message->full_name(), ".", "_", true));
  } else {
    names.push_front(message->name());
  }
  res->clear();
  std::copy(names.begin(), names.end(), back_inserter(*res));
}

vector<string> GetMessageNameHierarchy(const Descriptor* message, bool in_dependency) {
  vector<string> res;
  GetMessageNameHierarchy(message, in_dependency, &res);
  return res;
}

string GetDependentMessageName(const Descriptor* message) {
  vector<string> names = GetMessageNameHierarchy(message, true);
  return JoinStrings(names, ".");
}

string GetFieldMessageName(const Descriptor* field_message,
    const Descriptor* containing_message,
    bool containing_message_in_dependency) {
  if (field_message->file()->name() == containing_message->file()->name()) {
    // two message defined in the same file
    vector<string> field_message_hierarchy = GetMessageNameHierarchy(field_message, 
        containing_message_in_dependency);
    vector<string> containing_message_hierarchy = GetMessageNameHierarchy(containing_message,
        containing_message_in_dependency);

    size_t i = 0;
    for (; i<containing_message_hierarchy.size(); i++) {
      if (field_message_hierarchy[i] != containing_message_hierarchy[i]) {
        break;
      }
    }
    vector<string>::iterator start = field_message_hierarchy.begin();
    std::advance(start, i);
    vector<string> names;
    std::copy(start, field_message_hierarchy.end(), std::back_inserter(names));
    return JoinStrings(names, ".");
  } else {
    // two message defined in different files
    vector<string> field_message_hierarchy = GetMessageNameHierarchy(field_message, true);
    return JoinStrings(field_message_hierarchy, ".");
  }
}


string DefaultValue(const FieldDescriptor* field) {
  switch (field->cpp_type()) {
    case FieldDescriptor::CPPTYPE_INT32:
      return SimpleItoa(field->default_value_int32());
    case FieldDescriptor::CPPTYPE_UINT32:
      return SimpleItoa(field->default_value_uint32());
    case FieldDescriptor::CPPTYPE_INT64:
      return SimpleItoa(field->default_value_int64());
    case FieldDescriptor::CPPTYPE_UINT64:
      return SimpleItoa(field->default_value_uint64());
    case FieldDescriptor::CPPTYPE_DOUBLE: {
      double value = field->default_value_double();
      if (value == numeric_limits<double>::infinity()) {
        return "inf";
      } else if (value == -numeric_limits<double>::infinity()) {
        return "-inf";
      } else if (value != value) {
        return "nan";
      } else {
        return SimpleDtoa(value);
      }
    }
    case FieldDescriptor::CPPTYPE_FLOAT: {
      float value = field->default_value_float();
      if (value == numeric_limits<float>::infinity()) {
        return "inf";
      } else if (value == -numeric_limits<float>::infinity()) {
        return "-inf";
      } else if (value != value) {
        return "nan";
      } else {
        return SimpleFtoa(value);
      }
    }
    case FieldDescriptor::CPPTYPE_BOOL:
      return field->default_value_bool() ? "true" : "false";
    case FieldDescriptor::CPPTYPE_ENUM:
      return SimpleItoa(field->default_value_enum()->number());
    case FieldDescriptor::CPPTYPE_STRING:
      return "\"" + CEscape(field->default_value_string()) + "\"";
    case FieldDescriptor::CPPTYPE_MESSAGE:
      assert(0);
  }
  // Can't actually get here; make compiler happy.  (We could add a default
  // case above but then we wouldn't get the nice compiler warning when a
  // new type is added.)
  GOOGLE_LOG(FATAL) << "Can't get here.";
  return "";
}


inline bool IsNaN(double value) {
  // NaN is never equal to anything, even itself.
  return value != value;
}

// ----------------------------------------------------------------------
// CEscapeString()
// CHexEscapeString()
//    Copies 'src' to 'dest', escaping dangerous characters using
//    C-style escape sequences. This is very useful for preparing query
//    flags. 'src' and 'dest' should not overlap. The 'Hex' version uses
//    hexadecimal rather than octal sequences.
//    Returns the number of bytes written to 'dest' (not including the \0)
//    or -1 if there was insufficient space.
//
//    Currently only \n, \r, \t, ", ', \ and !isprint() chars are escaped.
// ----------------------------------------------------------------------
int CEscapeInternal(const char* src, int src_len, char* dest,
                    int dest_len, bool use_hex, bool utf8_safe) {
  const char* src_end = src + src_len;
  int used = 0;
  bool last_hex_escape = false; // true if last output char was \xNN

  for (; src < src_end; src++) {
    if (dest_len - used < 2)   // Need space for two letter escape
      return -1;

    bool is_hex_escape = false;
    switch (*src) {
      case '\n': dest[used++] = '\\'; dest[used++] = 'n';  break;
      case '\r': dest[used++] = '\\'; dest[used++] = 'r';  break;
      case '\t': dest[used++] = '\\'; dest[used++] = 't';  break;
      case '\"': dest[used++] = '\\'; dest[used++] = '\"'; break;
      case '\'': dest[used++] = '\\'; dest[used++] = '\''; break;
      case '\\': dest[used++] = '\\'; dest[used++] = '\\'; break;
      default:
        // Note that if we emit \xNN and the src character after that is a hex
        // digit then that digit must be escaped too to prevent it being
        // interpreted as part of the character code by C.
        if ((!utf8_safe || static_cast<uint8>(*src) < 0x80) &&
            (!isprint(*src) ||
             (last_hex_escape && isxdigit(*src)))) {
          if (dest_len - used < 4) // need space for 4 letter escape
            return -1;
          sprintf(dest + used, (use_hex ? "\\x%02x" : "\\%03o"),
                  static_cast<uint8>(*src));
          is_hex_escape = use_hex;
          used += 4;
        } else {
          dest[used++] = *src; break;
        }
    }
    last_hex_escape = is_hex_escape;
  }

  if (dest_len - used < 1)   // make sure that there is room for \0
    return -1;

  dest[used] = '\0';   // doesn't count towards return value though
  return used;
}

int CEscapeString(const char* src, int src_len, char* dest, int dest_len) {
  return CEscapeInternal(src, src_len, dest, dest_len, false, false);
}

// ----------------------------------------------------------------------
// CEscape()
// CHexEscape()
//    Copies 'src' to result, escaping dangerous characters using
//    C-style escape sequences. This is very useful for preparing query
//    flags. 'src' and 'dest' should not overlap. The 'Hex' version
//    hexadecimal rather than octal sequences.
//
//    Currently only \n, \r, \t, ", ', \ and !isprint() chars are escaped.
// ----------------------------------------------------------------------
string CEscape(const string& src) {
  const int dest_length = src.size() * 4 + 1; // Maximum possible expansion
  scoped_array<char> dest(new char[dest_length]);
  const int len = CEscapeInternal(src.data(), src.size(),
                                  dest.get(), dest_length, false, false);
  GOOGLE_DCHECK_GE(len, 0);
  return string(dest.get(), len);
}

namespace strings {

string Utf8SafeCEscape(const string& src) {
  const int dest_length = src.size() * 4 + 1; // Maximum possible expansion
  scoped_array<char> dest(new char[dest_length]);
  const int len = CEscapeInternal(src.data(), src.size(),
                                  dest.get(), dest_length, false, true);
  GOOGLE_DCHECK_GE(len, 0);
  return string(dest.get(), len);
}

string CHexEscape(const string& src) {
  const int dest_length = src.size() * 4 + 1; // Maximum possible expansion
  scoped_array<char> dest(new char[dest_length]);
  const int len = CEscapeInternal(src.data(), src.size(),
                                  dest.get(), dest_length, true, false);
  GOOGLE_DCHECK_GE(len, 0);
  return string(dest.get(), len);
}

}  // namespace strings


// ----------------------------------------------------------------------
// StringReplace()
//    Replace the "old" pattern with the "new" pattern in a string,
//    and append the result to "res".  If replace_all is false,
//    it only replaces the first instance of "old."
// ----------------------------------------------------------------------

void StringReplace(const string& s, const string& oldsub,
                   const string& newsub, bool replace_all,
                   string* res) {
  if (oldsub.empty()) {
    res->append(s);  // if empty, append the given string.
    return;
  }

  string::size_type start_pos = 0;
  string::size_type pos;
  do {
    pos = s.find(oldsub, start_pos);
    if (pos == string::npos) {
      break;
    }
    res->append(s, start_pos, pos - start_pos);
    res->append(newsub);
    start_pos = pos + oldsub.size();  // start searching again after the "old"
  } while (replace_all);
  res->append(s, start_pos, s.length() - start_pos);
}

// ----------------------------------------------------------------------
// StringReplace()
//    Give me a string and two patterns "old" and "new", and I replace
//    the first instance of "old" in the string with "new", if it
//    exists.  If "global" is true; call this repeatedly until it
//    fails.  RETURN a new string, regardless of whether the replacement
//    happened or not.
// ----------------------------------------------------------------------

string StringReplace(const string& s, const string& oldsub,
                     const string& newsub, bool replace_all) {
  string ret;
  StringReplace(s, oldsub, newsub, replace_all, &ret);
  return ret;
}

// ----------------------------------------------------------------------
// JoinStrings()
//    This merges a vector of string components with delim inserted
//    as separaters between components.
//
// ----------------------------------------------------------------------
template <class ITERATOR>
static void JoinStringsIterator(const ITERATOR& start,
                                const ITERATOR& end,
                                const char* delim,
                                string* result) {
  GOOGLE_CHECK(result != NULL);
  result->clear();
  int delim_length = strlen(delim);

  // Precompute resulting length so we can reserve() memory in one shot.
  int length = 0;
  for (ITERATOR iter = start; iter != end; ++iter) {
    if (iter != start) {
      length += delim_length;
    }
    length += iter->size();
  }
  result->reserve(length);

  // Now combine everything.
  for (ITERATOR iter = start; iter != end; ++iter) {
    if (iter != start) {
      result->append(delim, delim_length);
    }
    result->append(iter->data(), iter->size());
  }
}

void JoinStrings(const vector<string>& components,
                 const char* delim,
                 string * result) {
  JoinStringsIterator(components.begin(), components.end(), delim, result);
}

// ----------------------------------------------------------------------
// FastIntToBuffer()
// FastInt64ToBuffer()
// FastHexToBuffer()
// FastHex64ToBuffer()
// FastHex32ToBuffer()
// ----------------------------------------------------------------------

// Offset into buffer where FastInt64ToBuffer places the end of string
// null character.  Also used by FastInt64ToBufferLeft.
static const int kFastInt64ToBufferOffset = 21;

char *FastInt64ToBuffer(int64 i, char* buffer) {
  // We could collapse the positive and negative sections, but that
  // would be slightly slower for positive numbers...
  // 22 bytes is enough to store -2**64, -18446744073709551616.
  char* p = buffer + kFastInt64ToBufferOffset;
  *p-- = '\0';
  if (i >= 0) {
    do {
      *p-- = '0' + i % 10;
      i /= 10;
    } while (i > 0);
    return p + 1;
  } else {
    // On different platforms, % and / have different behaviors for
    // negative numbers, so we need to jump through hoops to make sure
    // we don't divide negative numbers.
    if (i > -10) {
      i = -i;
      *p-- = '0' + i;
      *p = '-';
      return p;
    } else {
      // Make sure we aren't at MIN_INT, in which case we can't say i = -i
      i = i + 10;
      i = -i;
      *p-- = '0' + i % 10;
      // Undo what we did a moment ago
      i = i / 10 + 1;
      do {
        *p-- = '0' + i % 10;
        i /= 10;
      } while (i > 0);
      *p = '-';
      return p;
    }
  }
}

// Offset into buffer where FastInt32ToBuffer places the end of string
// null character.  Also used by FastInt32ToBufferLeft
static const int kFastInt32ToBufferOffset = 11;

// Yes, this is a duplicate of FastInt64ToBuffer.  But, we need this for the
// compiler to generate 32 bit arithmetic instructions.  It's much faster, at
// least with 32 bit binaries.
char *FastInt32ToBuffer(int32 i, char* buffer) {
  // We could collapse the positive and negative sections, but that
  // would be slightly slower for positive numbers...
  // 12 bytes is enough to store -2**32, -4294967296.
  char* p = buffer + kFastInt32ToBufferOffset;
  *p-- = '\0';
  if (i >= 0) {
    do {
      *p-- = '0' + i % 10;
      i /= 10;
    } while (i > 0);
    return p + 1;
  } else {
    // On different platforms, % and / have different behaviors for
    // negative numbers, so we need to jump through hoops to make sure
    // we don't divide negative numbers.
    if (i > -10) {
      i = -i;
      *p-- = '0' + i;
      *p = '-';
      return p;
    } else {
      // Make sure we aren't at MIN_INT, in which case we can't say i = -i
      i = i + 10;
      i = -i;
      *p-- = '0' + i % 10;
      // Undo what we did a moment ago
      i = i / 10 + 1;
      do {
        *p-- = '0' + i % 10;
        i /= 10;
      } while (i > 0);
      *p = '-';
      return p;
    }
  }
}

char *FastHexToBuffer(int i, char* buffer) {
  GOOGLE_CHECK(i >= 0) << "FastHexToBuffer() wants non-negative integers, not " << i;

  static const char *hexdigits = "0123456789abcdef";
  char *p = buffer + 21;
  *p-- = '\0';
  do {
    *p-- = hexdigits[i & 15];   // mod by 16
    i >>= 4;                    // divide by 16
  } while (i > 0);
  return p + 1;
}

char *InternalFastHexToBuffer(uint64 value, char* buffer, int num_byte) {
  static const char *hexdigits = "0123456789abcdef";
  buffer[num_byte] = '\0';
  for (int i = num_byte - 1; i >= 0; i--) {
    buffer[i] = hexdigits[uint32(value) & 0xf];
    value >>= 4;
  }
  return buffer;
}

char *FastHex64ToBuffer(uint64 value, char* buffer) {
  return InternalFastHexToBuffer(value, buffer, 16);
}

char *FastHex32ToBuffer(uint32 value, char* buffer) {
  return InternalFastHexToBuffer(value, buffer, 8);
}

static inline char* PlaceNum(char* p, int num, char prev_sep) {
   *p-- = '0' + num % 10;
   *p-- = '0' + num / 10;
   *p-- = prev_sep;
   return p;
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

static const char two_ASCII_digits[100][2] = {
  {'0','0'}, {'0','1'}, {'0','2'}, {'0','3'}, {'0','4'},
  {'0','5'}, {'0','6'}, {'0','7'}, {'0','8'}, {'0','9'},
  {'1','0'}, {'1','1'}, {'1','2'}, {'1','3'}, {'1','4'},
  {'1','5'}, {'1','6'}, {'1','7'}, {'1','8'}, {'1','9'},
  {'2','0'}, {'2','1'}, {'2','2'}, {'2','3'}, {'2','4'},
  {'2','5'}, {'2','6'}, {'2','7'}, {'2','8'}, {'2','9'},
  {'3','0'}, {'3','1'}, {'3','2'}, {'3','3'}, {'3','4'},
  {'3','5'}, {'3','6'}, {'3','7'}, {'3','8'}, {'3','9'},
  {'4','0'}, {'4','1'}, {'4','2'}, {'4','3'}, {'4','4'},
  {'4','5'}, {'4','6'}, {'4','7'}, {'4','8'}, {'4','9'},
  {'5','0'}, {'5','1'}, {'5','2'}, {'5','3'}, {'5','4'},
  {'5','5'}, {'5','6'}, {'5','7'}, {'5','8'}, {'5','9'},
  {'6','0'}, {'6','1'}, {'6','2'}, {'6','3'}, {'6','4'},
  {'6','5'}, {'6','6'}, {'6','7'}, {'6','8'}, {'6','9'},
  {'7','0'}, {'7','1'}, {'7','2'}, {'7','3'}, {'7','4'},
  {'7','5'}, {'7','6'}, {'7','7'}, {'7','8'}, {'7','9'},
  {'8','0'}, {'8','1'}, {'8','2'}, {'8','3'}, {'8','4'},
  {'8','5'}, {'8','6'}, {'8','7'}, {'8','8'}, {'8','9'},
  {'9','0'}, {'9','1'}, {'9','2'}, {'9','3'}, {'9','4'},
  {'9','5'}, {'9','6'}, {'9','7'}, {'9','8'}, {'9','9'}
};

char* FastUInt32ToBufferLeft(uint32 u, char* buffer) {
  int digits;
  const char *ASCII_digits = NULL;
  // The idea of this implementation is to trim the number of divides to as few
  // as possible by using multiplication and subtraction rather than mod (%),
  // and by outputting two digits at a time rather than one.
  // The huge-number case is first, in the hopes that the compiler will output
  // that case in one branch-free block of code, and only output conditional
  // branches into it from below.
  if (u >= 1000000000) {  // >= 1,000,000,000
    digits = u / 100000000;  // 100,000,000
    ASCII_digits = two_ASCII_digits[digits];
    buffer[0] = ASCII_digits[0];
    buffer[1] = ASCII_digits[1];
    buffer += 2;
sublt100_000_000:
    u -= digits * 100000000;  // 100,000,000
lt100_000_000:
    digits = u / 1000000;  // 1,000,000
    ASCII_digits = two_ASCII_digits[digits];
    buffer[0] = ASCII_digits[0];
    buffer[1] = ASCII_digits[1];
    buffer += 2;
sublt1_000_000:
    u -= digits * 1000000;  // 1,000,000
lt1_000_000:
    digits = u / 10000;  // 10,000
    ASCII_digits = two_ASCII_digits[digits];
    buffer[0] = ASCII_digits[0];
    buffer[1] = ASCII_digits[1];
    buffer += 2;
sublt10_000:
    u -= digits * 10000;  // 10,000
lt10_000:
    digits = u / 100;
    ASCII_digits = two_ASCII_digits[digits];
    buffer[0] = ASCII_digits[0];
    buffer[1] = ASCII_digits[1];
    buffer += 2;
sublt100:
    u -= digits * 100;
lt100:
    digits = u;
    ASCII_digits = two_ASCII_digits[digits];
    buffer[0] = ASCII_digits[0];
    buffer[1] = ASCII_digits[1];
    buffer += 2;
done:
    *buffer = 0;
    return buffer;
  }

  if (u < 100) {
    digits = u;
    if (u >= 10) goto lt100;
    *buffer++ = '0' + digits;
    goto done;
  }
  if (u  <  10000) {   // 10,000
    if (u >= 1000) goto lt10_000;
    digits = u / 100;
    *buffer++ = '0' + digits;
    goto sublt100;
  }
  if (u  <  1000000) {   // 1,000,000
    if (u >= 100000) goto lt1_000_000;
    digits = u / 10000;  //    10,000
    *buffer++ = '0' + digits;
    goto sublt10_000;
  }
  if (u  <  100000000) {   // 100,000,000
    if (u >= 10000000) goto lt100_000_000;
    digits = u / 1000000;  //   1,000,000
    *buffer++ = '0' + digits;
    goto sublt1_000_000;
  }
  // we already know that u < 1,000,000,000
  digits = u / 100000000;   // 100,000,000
  *buffer++ = '0' + digits;
  goto sublt100_000_000;
}

char* FastInt32ToBufferLeft(int32 i, char* buffer) {
  uint32 u = i;
  if (i < 0) {
    *buffer++ = '-';
    u = -i;
  }
  return FastUInt32ToBufferLeft(u, buffer);
}

char* FastUInt64ToBufferLeft(uint64 u64, char* buffer) {
  int digits;
  const char *ASCII_digits = NULL;

  uint32 u = static_cast<uint32>(u64);
  if (u == u64) return FastUInt32ToBufferLeft(u, buffer);

  uint64 top_11_digits = u64 / 1000000000;
  buffer = FastUInt64ToBufferLeft(top_11_digits, buffer);
  u = u64 - (top_11_digits * 1000000000);

  digits = u / 10000000;  // 10,000,000
  GOOGLE_DCHECK_LT(digits, 100);
  ASCII_digits = two_ASCII_digits[digits];
  buffer[0] = ASCII_digits[0];
  buffer[1] = ASCII_digits[1];
  buffer += 2;
  u -= digits * 10000000;  // 10,000,000
  digits = u / 100000;  // 100,000
  ASCII_digits = two_ASCII_digits[digits];
  buffer[0] = ASCII_digits[0];
  buffer[1] = ASCII_digits[1];
  buffer += 2;
  u -= digits * 100000;  // 100,000
  digits = u / 1000;  // 1,000
  ASCII_digits = two_ASCII_digits[digits];
  buffer[0] = ASCII_digits[0];
  buffer[1] = ASCII_digits[1];
  buffer += 2;
  u -= digits * 1000;  // 1,000
  digits = u / 10;
  ASCII_digits = two_ASCII_digits[digits];
  buffer[0] = ASCII_digits[0];
  buffer[1] = ASCII_digits[1];
  buffer += 2;
  u -= digits * 10;
  digits = u;
  *buffer++ = '0' + digits;
  *buffer = 0;
  return buffer;
}

char* FastInt64ToBufferLeft(int64 i, char* buffer) {
  uint64 u = i;
  if (i < 0) {
    *buffer++ = '-';
    u = -i;
  }
  return FastUInt64ToBufferLeft(u, buffer);
}

// ----------------------------------------------------------------------
// SimpleItoa()
//    Description: converts an integer to a string.
//
//    Return value: string
// ----------------------------------------------------------------------

string SimpleItoa(int i) {
  char buffer[kFastToBufferSize];
  return (sizeof(i) == 4) ?
    FastInt32ToBuffer(i, buffer) :
    FastInt64ToBuffer(i, buffer);
}

string SimpleItoa(unsigned int i) {
  char buffer[kFastToBufferSize];
  return string(buffer, (sizeof(i) == 4) ?
    FastUInt32ToBufferLeft(i, buffer) :
    FastUInt64ToBufferLeft(i, buffer));
}

string SimpleItoa(long i) {
  char buffer[kFastToBufferSize];
  return (sizeof(i) == 4) ?
    FastInt32ToBuffer(i, buffer) :
    FastInt64ToBuffer(i, buffer);
}

string SimpleItoa(unsigned long i) {
  char buffer[kFastToBufferSize];
  return string(buffer, (sizeof(i) == 4) ?
    FastUInt32ToBufferLeft(i, buffer) :
    FastUInt64ToBufferLeft(i, buffer));
}

string SimpleItoa(long long i) {
  char buffer[kFastToBufferSize];
  return (sizeof(i) == 4) ?
    FastInt32ToBuffer(i, buffer) :
    FastInt64ToBuffer(i, buffer);
}

string SimpleItoa(unsigned long long i) {
  char buffer[kFastToBufferSize];
  return string(buffer, (sizeof(i) == 4) ?
    FastUInt32ToBufferLeft(i, buffer) :
    FastUInt64ToBufferLeft(i, buffer));
}

// ----------------------------------------------------------------------
// SimpleDtoa()
// SimpleFtoa()
// DoubleToBuffer()
// FloatToBuffer()
//    We want to print the value without losing precision, but we also do
//    not want to print more digits than necessary.  This turns out to be
//    trickier than it sounds.  Numbers like 0.2 cannot be represented
//    exactly in binary.  If we print 0.2 with a very large precision,
//    e.g. "%.50g", we get "0.2000000000000000111022302462515654042363167".
//    On the other hand, if we set the precision too low, we lose
//    significant digits when printing numbers that actually need them.
//    It turns out there is no precision value that does the right thing
//    for all numbers.
//
//    Our strategy is to first try printing with a precision that is never
//    over-precise, then parse the result with strtod() to see if it
//    matches.  If not, we print again with a precision that will always
//    give a precise result, but may use more digits than necessary.
//
//    An arguably better strategy would be to use the algorithm described
//    in "How to Print Floating-Point Numbers Accurately" by Steele &
//    White, e.g. as implemented by David M. Gay's dtoa().  It turns out,
//    however, that the following implementation is about as fast as
//    DMG's code.  Furthermore, DMG's code locks mutexes, which means it
//    will not scale well on multi-core machines.  DMG's code is slightly
//    more accurate (in that it will never use more digits than
//    necessary), but this is probably irrelevant for most users.
//
//    Rob Pike and Ken Thompson also have an implementation of dtoa() in
//    third_party/fmt/fltfmt.cc.  Their implementation is similar to this
//    one in that it makes guesses and then uses strtod() to check them.
//    Their implementation is faster because they use their own code to
//    generate the digits in the first place rather than use snprintf(),
//    thus avoiding format string parsing overhead.  However, this makes
//    it considerably more complicated than the following implementation,
//    and it is embedded in a larger library.  If speed turns out to be
//    an issue, we could re-implement this in terms of their
//    implementation.
// ----------------------------------------------------------------------

string SimpleDtoa(double value) {
  char buffer[kDoubleToBufferSize];
  return DoubleToBuffer(value, buffer);
}

string SimpleFtoa(float value) {
  char buffer[kFloatToBufferSize];
  return FloatToBuffer(value, buffer);
}

static inline bool IsValidFloatChar(char c) {
  return ('0' <= c && c <= '9') ||
         c == 'e' || c == 'E' ||
         c == '+' || c == '-';
}

void DelocalizeRadix(char* buffer) {
  // Fast check:  if the buffer has a normal decimal point, assume no
  // translation is needed.
  if (strchr(buffer, '.') != NULL) return;

  // Find the first unknown character.
  while (IsValidFloatChar(*buffer)) ++buffer;

  if (*buffer == '\0') {
    // No radix character found.
    return;
  }

  // We are now pointing at the locale-specific radix character.  Replace it
  // with '.'.
  *buffer = '.';
  ++buffer;

  if (!IsValidFloatChar(*buffer) && *buffer != '\0') {
    // It appears the radix was a multi-byte character.  We need to remove the
    // extra bytes.
    char* target = buffer;
    do { ++buffer; } while (!IsValidFloatChar(*buffer) && *buffer != '\0');
    memmove(target, buffer, strlen(buffer) + 1);
  }
}

char* DoubleToBuffer(double value, char* buffer) {
  // DBL_DIG is 15 for IEEE-754 doubles, which are used on almost all
  // platforms these days.  Just in case some system exists where DBL_DIG
  // is significantly larger -- and risks overflowing our buffer -- we have
  // this assert.
  GOOGLE_COMPILE_ASSERT(DBL_DIG < 20, DBL_DIG_is_too_big);

  if (value == numeric_limits<double>::infinity()) {
    strcpy(buffer, "inf");
    return buffer;
  } else if (value == -numeric_limits<double>::infinity()) {
    strcpy(buffer, "-inf");
    return buffer;
  } else if (IsNaN(value)) {
    strcpy(buffer, "nan");
    return buffer;
  }

  int snprintf_result =
    snprintf(buffer, kDoubleToBufferSize, "%.*g", DBL_DIG, value);

  // The snprintf should never overflow because the buffer is significantly
  // larger than the precision we asked for.
  GOOGLE_DCHECK(snprintf_result > 0 && snprintf_result < kDoubleToBufferSize);

  // We need to make parsed_value volatile in order to force the compiler to
  // write it out to the stack.  Otherwise, it may keep the value in a
  // register, and if it does that, it may keep it as a long double instead
  // of a double.  This long double may have extra bits that make it compare
  // unequal to "value" even though it would be exactly equal if it were
  // truncated to a double.
  volatile double parsed_value = strtod(buffer, NULL);
  if (parsed_value != value) {
    int snprintf_result =
      snprintf(buffer, kDoubleToBufferSize, "%.*g", DBL_DIG+2, value);

    // Should never overflow; see above.
    GOOGLE_DCHECK(snprintf_result > 0 && snprintf_result < kDoubleToBufferSize);
  }

  DelocalizeRadix(buffer);
  return buffer;
}

bool safe_strtof(const char* str, float* value) {
  char* endptr;
  errno = 0;  // errno only gets set on errors
#if defined(_WIN32) || defined (__hpux)  // has no strtof()
  *value = strtod(str, &endptr);
#else
  *value = strtof(str, &endptr);
#endif
  return *str != 0 && *endptr == 0 && errno == 0;
}

char* FloatToBuffer(float value, char* buffer) {
  // FLT_DIG is 6 for IEEE-754 floats, which are used on almost all
  // platforms these days.  Just in case some system exists where FLT_DIG
  // is significantly larger -- and risks overflowing our buffer -- we have
  // this assert.
  GOOGLE_COMPILE_ASSERT(FLT_DIG < 10, FLT_DIG_is_too_big);

  if (value == numeric_limits<double>::infinity()) {
    strcpy(buffer, "inf");
    return buffer;
  } else if (value == -numeric_limits<double>::infinity()) {
    strcpy(buffer, "-inf");
    return buffer;
  } else if (IsNaN(value)) {
    strcpy(buffer, "nan");
    return buffer;
  }

  int snprintf_result =
    snprintf(buffer, kFloatToBufferSize, "%.*g", FLT_DIG, value);

  // The snprintf should never overflow because the buffer is significantly
  // larger than the precision we asked for.
  GOOGLE_DCHECK(snprintf_result > 0 && snprintf_result < kFloatToBufferSize);

  float parsed_value;
  if (!safe_strtof(buffer, &parsed_value) || parsed_value != value) {
    int snprintf_result =
      snprintf(buffer, kFloatToBufferSize, "%.*g", FLT_DIG+2, value);

    // Should never overflow; see above.
    GOOGLE_DCHECK(snprintf_result > 0 && snprintf_result < kFloatToBufferSize);
  }

  DelocalizeRadix(buffer);
  return buffer;
}

} // namespace tdw
} // namespace compiler
} // namespace protobuf
} // namespace google

