process config logic:

scenario : the scenario is like this,  get the data collection from [debt_cus_details] by using time frame that i created logic as 
 (current_process_time - last process time) >= Process_Config_param [this is the time that we need to config from the user this is work like a trigger ]  
Process_Config_param = 15 min

after geting data from above time period, it should be insert to a jason file to move it to mongo db.

if there is same account number it should insert in jason with same account no and other data as aobject of a array.