package com.srujan.twittergetter.controller;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.util.Iterator;

import com.fasterxml.jackson.core.JsonParser;

import org.json.simple.JSONArray;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.json.simple.JSONObject;

import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.mvc.method.annotation.StreamingResponseBody;

@RestController
public class DataController {

    @GetMapping("/data")
    public ResponseEntity<StreamingResponseBody> getTwitterData() throws FileNotFoundException {
        FileReader fileReader = new FileReader("/home/kafka/twitter-data/bitcoin-data.json");

        return ResponseEntity.ok()
                .contentType(MediaType.APPLICATION_JSON)
                .body(
                        out -> {
                            JSONParser jsonParser = new JSONParser();

                            try {
                                JSONArray jsonArray = (JSONArray) jsonParser.parse(fileReader);
                                Iterator<JSONObject> iteratorObj = jsonArray.iterator();
                                while (iteratorObj.hasNext()) {
                                    JSONObject jsonObject = iteratorObj.next();
                                    out.write(jsonObject.toJSONString().getBytes());
                                    out.flush();
                                }
                            } catch (ParseException e) {
                                e.printStackTrace();
                            }
                        });
    }
}
