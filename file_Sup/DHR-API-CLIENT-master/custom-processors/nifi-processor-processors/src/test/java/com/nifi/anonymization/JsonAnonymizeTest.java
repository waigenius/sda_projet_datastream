package com.nifi.anonymization;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.junit.Before;
import org.junit.Test;
import org.skyscreamer.jsonassert.JSONAssert;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

public class JsonAnonymizeTest {
    private JSONObject jsonObject;
    private JSONObject expectedJsonObject;
    private JSONArray attrList;


    public static String readFile(String path) throws IOException {
        byte[] encoded = Files.readAllBytes(Paths.get(path));
        return new String(encoded);
    }

    @Before
    public void init() throws IOException, JSONException{
        String path = new File("src/test/resources/anonymization").getAbsolutePath() + "/";
        jsonObject = new JSONObject(readFile(path + "file.json"));
        expectedJsonObject = new JSONObject(readFile(path + "expectedOutput.json"));
        attrList = new JSONArray(readFile(path + "fieldsToAnonymize.json"));
    }

    @Test
    public void anonymizationTest() throws JSONException {
        JSONObject result = new JSONObject(JsonAnonymize.anonymize(jsonObject, attrList));
        //System.out.println("result.toString(2) = " + result.toString(2));
        JSONAssert.assertEquals(expectedJsonObject, result, true);

    }

    @Test
    public void specialSymbolTest() throws JSONException {
        JSONObject json = new JSONObject("{\"symbol\":\"€\",\"classification\": \"<p>Objectifs : _test_08/08aaaaaaa Objectifs : _test_08/08aaaaaaaObjectifs : _test_08/08aaaaaaaaaaObjectifs &nbsp; :&nbsp; </p>\"}");
        String result = JsonAnonymize.anonymize(json, attrList);
        //System.out.println("result = " + result);
        assert (result.contains("€"));
    }

    @Test
    public void specialSymbolTest2() throws JSONException{
        JSONObject json = new JSONObject("{\"symbol\":\"\\\"€\\\"\",\"classification\":\"<p>Objectifs : _test_08/08aaaaaaa Objectifs : _test_08/08aaaaaaaObjectifs : _test_08/08aaaaaaaaaaObjectifs &nbsp; :&nbsp; </p>\"}");
        String result = JsonAnonymize.anonymize(json, attrList);

//        System.out.println("result = " + result);
        assert (result.contains("\\\"€\\\""));
    }

    @Test
    public void gsonTest() {
        String jsonString = " {\"symbol\":\"\\u20ac\",\"classification\":\"<p>Objectifs : _test_08/08aaaaaaa Objectifs : _test_08/08aaaaaaaObjectifs : _test_08/08aaaaaaaaaaObjectifs &nbsp; :&nbsp; <\\/p>\"}";

        JsonParser parser = new JsonParser();
        JsonObject json = parser.parse(jsonString).getAsJsonObject();

        Gson gson = new GsonBuilder().disableHtmlEscaping().setPrettyPrinting().create();
        String prettyJson = gson.toJson(json);
        System.out.println("prettyJson = " + prettyJson);
    }

    @Test
    public void nullValueTest() throws JSONException {
        //Null value should not be anonymized
        JSONObject json = new JSONObject("{\"int\":null,\"string\":null, \"date\":null}");
        JSONArray attrToAnon = new JSONArray("[{\"name\":\"int\",\"type\":\"int\"},{\"name\":\"date\",\"type\":\"date\"},{\"name\":\"string\",\"type\":\"string\"}]");
        JSONObject expected = new JSONObject("{\"int\":null,\"string\":null, \"date\":null}");
        //System.out.println("jsonObject.toString(2) = " + jsonObject.toString(2));
        String result = JsonAnonymize.anonymize(json, attrToAnon);
        //System.out.println("result = " + result);
        JSONAssert.assertEquals(expected, json, true);
    }

    @Test
    public void emptyValueTest() throws JSONException {
        //empty value should not be anonymized
        JSONObject json = new JSONObject("{\"int\":null,\"string\":\"\", \"date\":\"\"}");
        JSONArray attrToAnon = new JSONArray("[{\"name\":\"int\",\"type\":\"int\"},{\"name\":\"date\",\"type\":\"date\"},{\"name\":\"string\",\"type\":\"string\"}]");
        JSONObject expected = new JSONObject("{\"int\":null,\"string\":\"\", \"date\":\"\"}");
        //System.out.println("jsonObject.toString(2) = " + jsonObject.toString(2));
        String result = JsonAnonymize.anonymize(json, attrToAnon);
        //System.out.println("result = " + result);
        JSONAssert.assertEquals(expected, json, true);
    }
}