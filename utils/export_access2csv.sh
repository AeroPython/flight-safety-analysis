fullfilename=$1
destfolder=$2
filename=$(basename "$fullfilename")
dbname="${filename%.*}"

mkdir $destfolder/$dbname

echo .mode csv > $destfolder/$dbname/export_data.sql

for table in $(mdb-tables $fullfilename); do
    echo "Export table "$table
    mdb-export $fullfilename $table > $destfolder/$dbname/$table.csv
    echo .import $destfolder/$dbname/$table.csv  $table >> $destfolder/$dbname/export_data.sql
done

echo "Export schema "$dbname
mdb-schema $fullfilename > $destfolder/$dbname/schema.sql sqlite

sqlite3 $destfolder/$dbname.db < $destfolder/$dbname/schema.sql
sqlite3 $destfolder/$dbname.db < $destfolder/$dbname/export_data.sql

rm -r $destfolder/$dbname
