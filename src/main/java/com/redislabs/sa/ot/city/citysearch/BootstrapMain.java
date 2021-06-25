package com.redislabs.sa.ot.city.citysearch;

import com.redislabs.sa.ot.city.CSVCities;
import com.redislabs.sa.ot.city.City;
import com.redislabs.sa.ot.util.*;
import io.redisearch.client.Client;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import io.redisearch.Schema;
import io.redisearch.client.IndexDefinition;

/**
 * Prepares the database with the hashes of city information and creates the search index
 */
public class BootstrapMain {

    static JedisPool jedisPool = JedisConnectionFactory.getInstance().getJedisPool();
    static io.rebloom.client.Client cfClient = new io.rebloom.client.Client(jedisPool);
    static String citySearchIndex = "IDX:cities";
    static String cfCitiesList = "CF_CITIES_LIST";
    static String garbageCityNamesStreamName = "X:BEST_MATCHED_CITY_NAMES_BY_SEARCH"; //"x:gbg:city"; (could listen to any)

    public static void main(String[] args){
        TimeSeriesHeartBeatEmitter heartBeatEmitter = new TimeSeriesHeartBeatEmitter(jedisPool,BootstrapMain.class.getCanonicalName());
        cleanupIDX();
        cleanupCF();
        createCF();
        loadCitiesAsHashes();
        createCitySearchIndex();
        System.out.println("index created");
        startStreamAdapter();
    }

    static void startStreamAdapter() {
        RedisStreamAdapter redisStreamAdapter = new RedisStreamAdapter(garbageCityNamesStreamName,jedisPool);
        redisStreamAdapter.listenToStream(new BestCityNameMatchMapProcesor()); // need to build a MapProcessor suitable to the use case
    }

    static void cleanupCF(){
        Jedis jedis = jedisPool.getResource();
        try {
            jedis.del(cfCitiesList);
        }catch(Throwable t){t.printStackTrace();}
        finally {
            jedis.close();
        }
    }

    static void createCF(){
        cfClient.cfCreate(cfCitiesList,100000);
    }

    static void cleanupIDX(){
        try{
            Client client = new Client(citySearchIndex,jedisPool);
            client.dropIndex();
        }catch(Throwable t){t.printStackTrace();}
    }

    static boolean noDupe(String cityRecord){
        boolean isNoDupe = true;
        if(cfClient.cfExists(cfCitiesList,cityRecord)){
            isNoDupe = false;
        }
        return isNoDupe;
    }

    static void loadCitiesAsHashes(){
        Jedis jedis = jedisPool.getResource();
        CSVCities cHelper = CSVCities.getInstance();
        ArrayList<City> cities = cHelper.getCities();
        for(City c : cities){
            Map<String,String> map = new HashMap<String, String>();
            map.put("city",c.getCityName());
            map.put("name_length",c.getCityName().length()+"");
            map.put("state_or_province",c.getProvinceID());
            map.put("geopoint",""+c.getLng()+","+c.getLat());
            String zipPostalCodes = "";
            for(String cd:c.getPostalCodes()){
                zipPostalCodes+=cd+" , ";
            }
            map.put("zip_codes_or_postal_codes",zipPostalCodes);
            if(noDupe("city:"+c.getId())) {
                jedis.hmset("city:" + c.getId(), map);
            }
        }
        jedis.close();
    }

    static void createCitySearchIndex(){
        Schema sc = new Schema()
//                .addTextField("city", 5.0)
                .addField(new Schema.TextField("city", 5.0, false, false, false, "dm:en"))
                .addTextField("state_or_province", 1.0)
                .addTextField("zip_codes_or_postal_codes",10.0)
                .addGeoField("geopoint")
                .addSortableNumericField("name_length");
        IndexDefinition def = new IndexDefinition()
                .setPrefixes(new String[] {"city:"});
        Client client = new Client(citySearchIndex,jedisPool);
        client.createIndex(sc, Client.IndexOptions.defaultOptions().setDefinition(def));
    }
}

class BestCityNameMatchMapProcesor implements MapProcessor{
    @Override
    public void processMap(Map<String, String> payload) {
        System.out.println(payload);
    }
}

