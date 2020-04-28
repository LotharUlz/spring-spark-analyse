package com.nttdata.spring.spark.springsparkanalyse;

import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class AnalyseController {
 
    @Autowired
    AnalyseService service;
 
    @RequestMapping(method = RequestMethod.POST, path = "/wordcountSpark")
    public Long wordCountSpark(@RequestParam(required = true) String pattern) {
        return service.getCountSpark(pattern);
    }
    
    @RequestMapping(method = RequestMethod.POST, path = "/wordcountLegacy")
    public Long wordCountLegacy(@RequestParam(required = true) String pattern) {
        return service.getCountLegacy(pattern);
    }
    
    @RequestMapping(method = RequestMethod.POST, path="/analyseFilesCount")
    public Map<String, Long> analyseFilesCount(@RequestParam(required = true) String pattern) {
        return service.getFilesMatchingPatternFromPathAndCount(pattern);
    }
    
    @RequestMapping(method = RequestMethod.POST, path="/analyseAmount")
    public Map<String, String> analyseAmount(@RequestParam(required = true) String pattern) {
        return service.getFilesMatchingPatternSumAmount(pattern);
    }

    @RequestMapping(method = RequestMethod.POST, path="/analyseDate")
    public List<String> analyseDate(@RequestParam(required = true) String pattern) {
        return service.getFilesMatchingPatternDate(pattern);
    }
    
    @RequestMapping(method = RequestMethod.POST, path="/statistics")
    public Map<String, String> statistics() {
        return service.getStatistics();
    }
}

