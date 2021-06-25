import datetime
import json

## This file should be registered as a gear with the redis instance/cluster involved
## use the RedisInsight tool for simple upload, activation, and monitoring
## https://redislabs.com/redis-enterprise/redis-insight/

# This gear is registered to listen for events on the stream: X:GBG:CITY
# these stream events are expected to contain strings containing a proposed city name - misspelled and / or wrong
# 1) 1621846248588-0
#     2) 1) spellCheckMe
#        2) burnabee
#        3) requestID
#        4) PM_UID75439582505275
# this gear
# 1. checks to see if the provided city-word has been checked before using a cuckoo filter
# 2. uses redis search to find the closest accurate match to the city name based on the known indexed cities
# 3. broadcasts its findings using PUBLISH to a channel called CITY_CLEANUP
# interested listeners can SUBSCRIBE to this channel to get realtime updates
# 4. Writes the RECEIVED city-word and the proposed BEST_MATCH city name to a stream called X:BEST_MATCHED_CITY_NAMES_BY_SEARCH
# interested parties can check the stream for a history of submitted /proposed city words
# stream data looks like this:
# {\"key\": \"X:GBG:CITY\", \"id\": \"1621833630358-0\", \"value\": {\"requestID\": \"PM_UID4\", \"spellCheckMe\": \"tauranto\"}}

def cleanGarbage(starttime,s1):
  jsonVersion =json.loads(s1)
  cityName = jsonVersion['value']['spellCheckMe']
  requestID = jsonVersion['value']['requestID']
  execute('PUBLISH', 'CITY_CLEANUP', 'RECIEVED--> '+s1+' cityName: '+cityName+' requestID: '+requestID)
  if execute('CF.EXISTS','cfCities',cityName) == 0:
    execute('CF.ADD','cfCities',cityName)
    sresult = execute('FT.SEARCH','IDX:cities',cityName)
    cleanCityName = execute('HGET', sresult[1], 'city')
    duration = datetime.datetime.now()-starttime
    execute('PUBLISH', "CITY_CLEANUP", "RECIEVED: "+cityName+" BEST_MATCH: "+cleanCityName)
    execute('XADD','X:BEST_MATCHED_CITY_NAMES_BY_SEARCH','*','garbageIn',cityName,'bestMatchOut',cleanCityName,'processTimeDuration: ',duration)
  else:
    a = 1
    #do nothing

# GB is the GearBuilder (a factory for gears)
s_gear = GB('StreamReader',desc = "When a bit of garbage (bad city name) comes in - search for something similar and publish the clean result" )
#class GearsBuilder('StreamReader').run(prefix='*', batch=1, duration=0, onFailedPolicy='continue', onFailedRetryInterval=1, trimStream=True)
s_gear.foreach(
  lambda x: cleanGarbage(datetime.datetime.now(),json.dumps(x))
)

s_gear.register(
    'X:GBG:CITY',
    trimStream=False
    # setting trimStream=True  causes this gear to remove events from the stream it listens to as it processes them
)
