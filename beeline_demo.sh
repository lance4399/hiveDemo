  #!/bin/bash
  sql=$1
  /usr/bin/beeline -u 'jdbc:hive2://demo.com:10000/ai;principal=hive/demo.com@DEMO.COM;'  --hiveconf mapreduce.job.queuename=ai 
  --delimiterForDSV=DELIMITER --outputformat=tsv2  --showHeader=false -e "${sql}"