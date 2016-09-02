/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.zeppelin.spark;

import org.apache.zeppelin.interpreter.InterpreterContext;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileWriter;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;

/**
 * Utility and helper functions for the Spark Interpreter
 */
class Utils {
  public static Logger logger = LoggerFactory.getLogger(Utils.class);

  static Object invokeMethod(Object o, String name) {
    return invokeMethod(o, name, new Class[]{}, new Object[]{});
  }

  static Object invokeMethod(Object o, String name, Class[] argTypes, Object[] params) {
    try {
      return o.getClass().getMethod(name, argTypes).invoke(o, params);
    } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
      logger.error(e.getMessage(), e);
    }
    return null;
  }

  static Object invokeStaticMethod(Class c, String name, Class[] argTypes, Object[] params) {
    try {
      return c.getMethod(name, argTypes).invoke(null, params);
    } catch (NoSuchMethodException | InvocationTargetException | IllegalAccessException e) {
      logger.error(e.getMessage(), e);
    }
    return null;
  }

  static Object invokeStaticMethod(Class c, String name) {
    return invokeStaticMethod(c, name, new Class[]{}, new Object[]{});
  }

  static Class findClass(String name) {
    return findClass(name, false);
  }

  static Class findClass(String name, boolean silence) {
    try {
      return Utils.class.forName(name);
    } catch (ClassNotFoundException e) {
      if (!silence) {
        logger.error(e.getMessage(), e);
      }
      return null;
    }
  }

  static Object instantiateClass(String name, Class[] argTypes, Object[] params) {
    try {
      Constructor<?> constructor = Utils.class.getClassLoader()
              .loadClass(name).getConstructor(argTypes);
      return constructor.newInstance(params);
    } catch (NoSuchMethodException | ClassNotFoundException | IllegalAccessException |
      InstantiationException | InvocationTargetException e) {
      logger.error(e.getMessage(), e);
    }
    return null;
  }

  // function works after intp is initialized
  static boolean isScala2_10() {
    try {
      Utils.class.forName("org.apache.spark.repl.SparkIMain");
      return true;
    } catch (ClassNotFoundException e) {
      return false;
    }
  }

  static boolean isScala2_11() {
    return !isScala2_10();
  }

  static boolean isSpark2() {
    try {
      Utils.class.forName("org.apache.spark.sql.SparkSession");
      return true;
    } catch (ClassNotFoundException e) {
      return false;
    }
  }

  static void write_history(String file_path, InterpreterContext context, String cmd)
  {
    try {
      String user = context.getAuthenticationInfo().getUser();

      FileWriter wf = new FileWriter(file_path, true);

      wf.write("=======================\n");
      wf.write("----- Info ----\n");
      wf.write("DateTime: " + get_local_time() + "\n");
      wf.write("User: " + user + "\n");
      wf.write("NoteId: " + context.getNoteId() + "\n");
      wf.write("ParagraphId: " + context.getParagraphId() + "\n");

      wf.write("----- Command ----\n");
      wf.write(cmd + "\n");

      wf.close();
    }
    catch (java.io.IOException e) {
      logger.error("Can't open history '" + file_path + "'");
    }
  }

  static String get_local_time()
  {
    DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    df.setTimeZone(TimeZone.getTimeZone("Asia/Seoul"));

    return df.format(new Date());
  }
}
