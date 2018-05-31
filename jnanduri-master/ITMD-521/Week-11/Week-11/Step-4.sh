sqoop import --connect jdbc:mysql://localhost/hadoopguide --username root --password 1234 
--table widgets --where "(id >= 1000 and id < 3000)" --m 1
