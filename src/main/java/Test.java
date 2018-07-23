//  Source file: P:/advantage/com.optiva.unified.aspects/Test.java

/*
 * Copyright (c) Optiva Inc. 2000 - 2018  All Rights Reserved
 * The reproduction, transmission or use of this document or
 * its contents is not permitted without express written
 * authority. Offenders will be liable for damages. All rights,
 * including rights created by patent grant or registration of
 * a utility model or design, are reserved.
 * Technical modifications possible.
 * Technical specifications and features are binding only
 * insofar as they are specifically and expressly agreed upon
 * in a written contract.
 */

import com.google.cloud.spanner.Key;
import com.google.cloud.spanner.KeySet;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.Statement;


import com.excellenceengineeringsolutions.profiler.output.SpannerDump;

import java.util.Arrays;

/**
 * Created by eXpert on 7/18/2018.
 */
public class Test
{
  public static void main(String[] args) throws Exception
  {
    deleteTestJava();
  }

  private static void deleteTestJava()
  {
    ResultSet resultSet = SpannerDump.getClient().singleUse().executeQuery(Statement.of("select timestamp from LOG1 where stacktrace like '%run(Test.java%'"));
    KeySet.Builder keySet = KeySet.newBuilder();
    while(resultSet.next()){
      keySet.addKey(Key.of(resultSet.getTimestamp(0)));
    }

    SpannerDump.getClient().write(Arrays.asList(Mutation.delete("LOG1", keySet.build())));
  }


}
