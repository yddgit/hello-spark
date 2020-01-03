package com.my.project.spark.sql;

import org.apache.spark.sql.*;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class CreateDataset {
    public static void main(String[] args) {
        try(SparkSession spark = SparkSession.builder().appName("Create Datasets").config("spark.some.config.option", "some-value").getOrCreate()) {
            // Dataset类似RDD, 但不是用Java或Kyro做的序列化, 而是用的一种特殊Encoder对对象进行序列化, 然后用于处理或网络传输
            // 虽然Encoder和标准序列化都是将对象转换为字节数组, 但Encoder是动态生成的代码, 使用的格式允许Spark不用反序列化, 就能在其上执行过滤、排序和哈希操作

            // 为Java类创建Encoder
            List<Person> list = new ArrayList<Person>();
            for(int i=0; i<10; i++) {
                Person person = new Person();
                person.setName("Andy" + i);
                person.setAge(30 + i);
                list.add(person);
            }
            Encoder<Person> personEncoder = Encoders.bean(Person.class);
            Dataset<Person> javaBeanDS = spark.createDataset(list, personEncoder);
            javaBeanDS.show();

            // 通用类型Encoder
            Encoder<Integer> integerEncoder = Encoders.INT();
            Dataset<Integer> primitiveDS = spark.createDataset(Arrays.asList(1, 2, 3, 4, 5, 6), integerEncoder);
            Dataset<Integer> transformedDS = primitiveDS.map(value -> value + 1, integerEncoder);
            transformedDS.collectAsList().forEach(i -> System.out.println(i));

            // DataFrame转换Dataset, 只需要指定对应的Java实体类即可
            Dataset<Person> peopleDS = spark.read().json("D:\\tools\\spark-2.4.3-bin-hadoop2.7\\people.json").as(personEncoder);
            peopleDS.show();
        }
    }

    public static class Person implements Serializable {
        private String name;
        private int age;

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public int getAge() {
            return age;
        }

        public void setAge(int age) {
            this.age = age;
        }
    }
}
