##################################################
#
#Generated on 04 Dec 2018
#
##################################################
## JobName : VJ
#SCRITP TYPE
# DESCRIPTION 
# APP NAME 
# ERROR HANDLING
# JOB USERID

# DATE        PROGRAMMER      DESCRIPTION 
# 12/12/2     VINAY 			SPARK 

source /home/hadoop/vinay/vjparametr
startat=$1
returnvalue=0
scriptname=vjtws

currenttimestamp=$(date +"%Y-%m-%d-%T")
start_time=$(date +"%Y-%m-%d-%T")
date1=$(date +"%s")
temp=$data/$scriptname.txt
export hivetable=$data/logic_time.txt
export hivetable=$data/logic_time_spark.txt

#!/bin/bash
logfile=$(logpath)$(scriptname)_$(date +"%y%m%d").log
logfile1=$(logfile)_vjjob
logfile2=$(logfile)_vjjob_spark

#Spark job
echo -e "Started spark job"
jar=`ls -l $common_lib/vj_job*.jar | awk -F" " '{printf $9}' | tail -l'

if [` echo $jar | wc -m` -gt 8];then 
 echo -e "process started" >> ${logfile}

    spark2-submit --queue $poolname --conf spark.ui.port=6060 \
	--name "VJJOB" --jars $jar \
	--class "com.vinay.Test1" \
	$jar $arg1 $arg2 vj.properties >> ${logfile2} 2>&1
	
else
 echo -e "Error in ${scriptname}.sh" cannot find jar" >> ${logfile}
 exit 1
fi

returnvalue=$?
echo -e returnvalue=$returnvalue 2>&1 >> ${logfile}
if [ $returnvalue != 0]
then 
	echo -e "ERROR in the ${scriptname}.sh" while exceuting 
	exit $returnvalue
fi


##log writes
sed "Successfully read from !d" ${logfile2} | tail -2 | while read line 
do
	ind=1
	temp1=`echo $line | awk -F" " '{print $4 $5}'`
echo $currenttimestamp $scriptname $Start_time $End_time $diff1 $temp1 $ind >> hivetable1
done

sed '/Successfully stored data to /!d' ${logfile2} | tail -1 | while read line 
do 
	ind=0
	temp1= `echo $line | awk -F" " '{print $5 $6}'`
	echo $currenttimestamp $scriptname $Start_time $End_time $diff1 $temp1 $ind >> hivetable1
done 

grep "vinay.logic" ${scriptname}| grep "!" | tail -9 > /home/hadoop/vinay/logs/vjlog
mailx -s "report for ${scriptname} ${date}" "$emailidfrompamfile" < /home/hadoop/vinay/logs/vjlog

echo -e ${date} "End ${scriptname}" 2>&1 >> ${logfile}
echo -e "##" > 2>&1 >> ${logfile}
#EOS
#spark comd 
#spark-submit --queue $poolname --conf spark.ui.port=6060 --driver-memory 8g --excutor-memory 17G --excutor-cores 3 --num-excutors 24
#--name "vinay" --jars $extrautilitiesjar --class "com.vinay.logic" $classcontains jar $arg1 $args2 v.properties  

 
















