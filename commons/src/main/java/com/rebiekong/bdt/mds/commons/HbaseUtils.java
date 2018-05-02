package com.rebiekong.bdt.mds.commons;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.stream.Collectors;

public class HbaseUtils {

    public static Filter getIdInfo(String idType, List<String> dimensions) {
        return new FilterList(
                FilterList.Operator.MUST_PASS_ALL,
                new SingleColumnValueFilter(
                        "META".getBytes(),
                        "type".getBytes(),
                        CompareFilter.CompareOp.EQUAL, idType.getBytes()
                ),
                new SingleColumnValueFilter(
                        "META".getBytes(),
                        "dimension".getBytes(),
                        CompareFilter.CompareOp.EQUAL,
                        String.join("|", dimensions.stream().sorted().collect(Collectors.toList())).getBytes()
                )
        );
    }


    public static String searchKey(Map<String, String> dimensions) {
        return DigestUtils.md5Hex(
                String.join(
                        "|",
                        new TreeMap<>(dimensions).entrySet().stream()
                                .sorted(Comparator.comparing(Entry::getKey)).map(Entry::getValue)
                                .collect(Collectors.toSet())
                ));
    }

}
