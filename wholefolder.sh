#!/bin/bash
FILES=~/csvtodatabase/*.csv
for f in $FILES
do
php csvtotable.php clean $f
done
CLEANFILES=~/csvtodatabase/clean_*.csv
for g in $CLEANFILES
do
php csvtotable.php $g
done

