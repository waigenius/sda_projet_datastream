package com.nifi.anonymization;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.commons.lang3.StringUtils;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.math.BigInteger;
import java.util.Iterator;

import static org.apache.commons.codec.digest.DigestUtils.sha256Hex;

public class JsonAnonymize {

    public static String anonymize(final JSONObject jsonInput, final JSONArray attrList) throws JSONException {
        JSONObject json = new JSONObject(jsonInput.toString());

        for (int i = 0; i < attrList.length(); i++) {
            JSONObject attr = attrList.getJSONObject(i);
            String name = attr.getString("name");
            String type = attr.getString("type");

            findAndTransform(json, name, type);
        }

        //Passing by a gson object is used to keep all the special characters (< # \" >) without loosing the mandatory \ before the " contained in the json values
        JsonParser jsonParser = new JsonParser();
        JsonObject jsonObject = jsonParser.parse(json.toString()).getAsJsonObject();
        Gson gson = new GsonBuilder().disableHtmlEscaping().create();
        return gson.toJson(jsonObject);
    }


    private static void transform(JSONObject obj, String k, String attr_type) throws NumberFormatException, JSONException {
        if (!obj.isNull(k) && !"".equals(obj.get(k))) {
            switch (attr_type) {
                case "date":
                case "Edm.DateTimeOffset":
                    obj.put(k, hashDate(obj.get(k).toString()));
                    break;
                case "int":
                case "long":
                case "number":
                case "Edm.Int16":
                case "Edm.Int32":
                case "Edm.Int64":
                    obj.put(k, hashInt(obj.get(k).toString()));
                    break;
                case "string":
                case "char":
                case "Edm.Guid":
                case "Edm.String":
                    obj.put(k, sha256Hex(obj.get(k).toString()));
                    break;

                default:
                    obj.put(k, sha256Hex(obj.get(k).toString()));
                    break;
            }
        }
    }


    private static JSONObject findAndTransform(JSONObject obj, String keyMain, String type) throws JSONException {
        // We need to know keys of Jsonobject
        JSONObject json = new JSONObject();
        Iterator iterator = obj.keys();
        String key = null;
        while (iterator.hasNext()) {
            key = (String) iterator.next();
            // if object is just string we change value in key
            if ((obj.optJSONArray(key) == null) && (obj.optJSONObject(key) == null)) {
                if ((key.equals(keyMain))) {
                    // put new value
                    transform(obj, key, type);
                    return obj;
                }
            }

            // if it's jsonobject
            if (obj.optJSONObject(key) != null) {
                findAndTransform(obj.getJSONObject(key), keyMain, type);
            }

            // if it's jsonarray
            if (obj.optJSONArray(key) != null) {
                JSONArray jArray = obj.getJSONArray(key);
                for (int i = 0; i < jArray.length(); i++) {
                    findAndTransform(jArray.getJSONObject(i), keyMain, type);
                }
            }
        }
        return obj;
    }

    private static String hashDate(String date) {
        return "1970-01-01T00:00:00.000Z";
    }

    private static BigInteger hashInt(String val) {
        // Hash the string value with sha256 and then convert it to a big integer
        // StringUtils.left is used to reduce the size of the returned value
        return new BigInteger((StringUtils.left(sha256Hex(val), 6).getBytes()));
    }
}
