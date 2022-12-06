package com.example.springbatchintegrationsample;

import org.junit.jupiter.api.Test;
import org.springframework.util.Assert;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

class Main {

    private static final long SIZE = 3;

    @Test
    void test() {

        List<String> list = Arrays.asList("1", "2", "3", "4", "5", "6", "7", "8", "9", "10");

        final long partitionCount = partitionCount(list.size(), SIZE);

        List<List<String>> splitList =
                Stream.iterate(0, n -> n + 1)
                        .limit(partitionCount).parallel()
                        .map(a ->
                                list.stream().skip(a * SIZE)
                                        .limit(SIZE).parallel()
                                        .collect(Collectors.toList()))
                        .collect(Collectors.toList());


        System.out.println(splitList);

    }

    @Test
    void testPartitionCount() {
        final long listSize = 0L;
        final long partitionSize = 1L;

        final long l = partitionCount(listSize, partitionSize);

        System.out.println(l);
    }

    /**
     * 计算切片.
     */
    private static long partitionCount(final long listSize, final long partitionSize) {
        Assert.isTrue(listSize > 0, "List size must be greater than 0");
        Assert.isTrue(partitionSize > 0, "Partition size must be greater than 0");

        return (listSize + partitionSize - 1) / partitionSize;
    }
}