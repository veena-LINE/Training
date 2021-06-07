energy-device.py

    pushing data every 5 seconds

    add a rule
        select * from 'energy/readings'
        
        add a action for kinesis firehose
        
        push data to kinesis firehose

        part 0:
            300 seconds 
            group the messages
            s3 

       
        part 1:
            300 seconds
            group the messages
            call lambda /new lambda 
            whihc convert json to csv 


     add a rule, if the v > 145
            call a lambda as action
               let that lambda to insert or update the readings into dynamo db table "Alerts" , key can be device_id, and attributes are  v, a
