#clean user doc which get from tdw wiki
cd $(dirname $0)
sed -i '/printfooter/,+78d' ../doc/user_sql.htm
sed -i '/printfooter/,+86d' ../doc/user_function.htm
sed -i 's/http:\/\/tdw.boss.com\/wiki\/index.php\/TDW_.*#/#/g' ../doc/user_sql.htm
sed -i 's/http:\/\/tdw.boss.com\/wiki\/index.php\/TDW_.*#/#/g' ../doc/user_function.htm
