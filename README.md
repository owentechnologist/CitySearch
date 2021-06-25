# CitySearch
Load cities into redisSearch

This example - Loads City Data into Redis Search which can then be - you guessed it... Searched! 

The data is loaded from a csv file populated with data from a free data set provided by:  https://simplemaps.com/ 

This CitySearch project is part of a larger microservices example and does Four interesting things.

1) Loads hashes into Redis
2) Creates a search index in RediSearch
3) Subscribes to a Redis Stream for notifications of when a city name query is matched to a city record - this activity is performed by a separate microservice 
4) Publishes its heartbeat to Redis TimeSeries for monitoring 'Alive-ness' purposes 

Example searches:

Get me the cities asociated with the Zip-code 11213:
FT.SEARCH "IDX:cities" "11213"

Get me the cities asociated with the Zip-code 11213 (but exclude Brooklyn from the results):
FT.SEARCH "IDX:cities" "11213 -Brooklyn"

Get me the cities that sounds like 'brewklin' 
FT.SEARCH "IDX:cities" brewklin

Get me the cities that sound like 'wight' but don't include any that have a zip equal to 'K4P'
FT.SEARCH "IDX:cities" "wight -K4P"

Get me the cities within 50 Km of this location on Earth:
FT.SEARCH "IDX:cities" "@geopoint:[-122.8, 49, 50 km]"




