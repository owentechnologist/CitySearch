import datetime
import json

## This file should be registered as a gear with the redis instance/cluster involved
## use the RedisInsight tool for simple upload, activation, and monitoring
## https://redislabs.com/redis-enterprise/redis-insight/

# This gear is registered to listen for changes to keys beginning with the prefix: gbg:
# these keys are expected to be strings containing a proposed city name - misspelled and / or wrong
# this gear
# 1. checks to see if the provided city-word has been checked before using a cuckoo filter
# 2. uses redis search to find the closest accurate match to the city name based on the known indexed cities
# 3. broadcasts its findings using PUBLISH to a channel called CITY_CLEANUP
# interested listeners can SUBSCRIBE to this channel to get realtime updates
# 4. Writes the RECEIVED city-word and the proposed BEST_MATCH city name to a stream called x:DATA_UPDATES
# interested parties can check the stream for a history of submitted /proposed city words

def cleanGarbage(cityName, eventType):
  if eventType == "set":
    starttime = datetime.datetime.now()
    if execute('CF.EXISTS','cfCities',cityName) == 0:
      stimestart = datetime.datetime.now().strftime('%H:%M:%S.%f')
      execute('CF.ADD','cfCities',cityName)
      sresult = execute('FT.SEARCH','IDX:cities',cityName)
      cleanCityName = execute('HGET', sresult[1], 'city')
      duration = datetime.datetime.now()-starttime
      timeComment="started processing at:"+stimestart+" finished at: "+datetime.datetime.now().strftime('%H:%M:%S.%f')
      execute('PUBLISH', "CITY_CLEANUP", "RECIEVED: "+cityName+" BEST_MATCH: "+cleanCityName)
      execute('XADD','x:DATA_UPDATES','*','garbageIn',cityName,'bestMatchOut',cleanCityName,'processTimeDuration: ',duration,'timeComment',timeComment)
    else:
      a = 1
      #do nothing
  else:
    b = 1
    #do nothing


# GB is the GearBuilder (a factory for gears)
s_gear = GB( desc = "When a bit of garbage (bad city name) comes in - search for something similar and publish the clean result" )

s_gear.foreach(
  lambda x: cleanGarbage(x['value'],x['event'])
)

s_gear.register(
    'gbg:*',
    mode='sync',
    readValue=True
    #NB: mode='sync' ensures events are propagated immediately
    #readValue=True shows the value of the key
    #readValue=False shows just the key and the operation
)
