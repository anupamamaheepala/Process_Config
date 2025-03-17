Process Config Logic

There is a set of data in my sql database.
we have to get data using sql query
these are the condions,
    we have get call to do this task
    if we get call we have first check last time get the call then check current time
    then if current time - last call time >= 15 get data include that time period
    if current time - last call time < 15 process not be happend

after get data using where query we have to reqest to api and get response and save response to database.