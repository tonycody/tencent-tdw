grammar protobuf;
options
{
output=AST;
ASTLabelType=CommonTree;
backtrack=false;
k=3;
}
tokens
{
TOK_MSG;
TOK_FIELD;
TOK_USER_TYPE;
TOK_OPTION_DEFAULT;
TOK_OPTION_BODY;
TOK_MSG_BODY;

TOK_OPTION;
TOK_IMPORT;
TOK_PACKAGE;


}

@header {
package org.apache.hadoop.hive.ql.parse;
}
@lexer::header {package org.apache.hadoop.hive.ql.parse;}

proto
  :
  (message
  | extendstmt
  //| enumstmt
  | importstmt
  | packagestmt
  | optionstmt)*
  ;
  
  extendstmt
  : 'extend' userType messageBody ';'//do not support now
  ;
 // enumstmt :: "enum" ID "{" (option | enumField | ";")* "}"
 // enumField :: ID "=" INTLit ";"
 //
 
 importstmt
 :
 'import'  ipt=STRING ';'
 -> ^(TOK_IMPORT $ipt)
 ;
 packagestmt
 :
 'package' name=ID ';'
 -> ^(TOK_PACKAGE $name)
 
 ;
 
 optionstmt
 :
 'option' optionBody ';'
 -> ^(TOK_OPTION optionBody)
 ;

message 
  :
  'message' name=ID messageBody
  -> ^(TOK_MSG $name messageBody)
;

messageBody
  :
  '{' 
    bodyList
  '}' 
  -> ^(TOK_MSG_BODY bodyList)
  ;

bodyList 
  : 
  ( field
  /*| enum
  | message
  | extend
  | extensions
  | option
  | group
  | ':'*/
  )*
  ;
field 
  :
  modifier type name=ID '=' intLit ('[' folist+=fieldOption (',' folist+=fieldOption)* ']')? ';'
  -> ^(TOK_FIELD modifier type $name intLit $folist?)
  ;
modifier 
  : 'required'
  | 'optional'
  | 'repeated'
  ;
type  :
  'double'
  | 'float'
  | 'int32'
  | 'int64'
  | 'uint32'
  | 'uint64'
  | 'sint32'
  | 'sint64'
  | 'fixed32'
  | 'fixed64'
  | 'sfixed32'
  | 'sfixed64'
  | 'bool'
  | 'string'
  | 'bytes'
  | userType
  ;
userType 
  :
  (full='.')? lead=ID ('.' follow+=ID)*
  -> ^(TOK_USER_TYPE $lead $full? $follow)
  ;
fieldOption 
  : 
  optionBody
  | 'default' '=' constant
  -> ^(TOK_OPTION_DEFAULT constant)
  ;

optionBody 
  : 
  names+=ID ('.' names+=ID)* '=' constant
  -> ^(TOK_OPTION_BODY $names constant)
  ;
constant 
  :
  ID
  | intLit
  | FLOAT
  | STRING
  | boolLit
  ;

intLit  :
  INT
  | hexInt
  /*| octInt*/  
  ;

hexInt  :
  '0' ('x' | 'X') HEX_DIGIT+  
  ;
/*
octInt  :
  '0' ('0'..'7')+ 
  ;
  */
boolLit :
  'true'
  | 'false' 
  ;


ID  : ('a'..'z'|'A'..'Z'|'_') ('a'..'z'|'A'..'Z'|'0'..'9'|'_')*
    ;

INT : '0'..'9'+
    ;

FLOAT
    :   ('0'..'9')+ '.' ('0'..'9')* EXPONENT?
    |   '.' ('0'..'9')+ EXPONENT?
    |   ('0'..'9')+ EXPONENT
    ;

COMMENT
    :   '//' ~('\n'|'\r')* '\r'? '\n' {$channel=HIDDEN;}
    |   '/*' ( options {greedy=false;} : . )* '*/' {$channel=HIDDEN;}
    ;

WS  :   ( ' '
        | '\t'
        | '\r'
        | '\n'
        ) {$channel=HIDDEN;}
    ;

STRING
    :  '"' ( ESC_SEQ | ~('\\'|'"') )* '"'
    ;

CHAR:  '\'' ( ESC_SEQ | ~('\''|'\\') ) '\''
    ;

fragment
EXPONENT : ('e'|'E') ('+'|'-')? ('0'..'9')+ ;

fragment
HEX_DIGIT : ('0'..'9'|'a'..'f'|'A'..'F') ;

fragment
ESC_SEQ
    :   '\\' ('b'|'t'|'n'|'f'|'r'|'\"'|'\''|'\\')
    |   UNICODE_ESC
    |   OCTAL_ESC
    ;

fragment
OCTAL_ESC
    :   '\\' ('0'..'3') ('0'..'7') ('0'..'7')
    |   '\\' ('0'..'7') ('0'..'7')
    |   '\\' ('0'..'7')
    ;

fragment
UNICODE_ESC
    :   '\\' 'u' HEX_DIGIT HEX_DIGIT HEX_DIGIT HEX_DIGIT
    ;
