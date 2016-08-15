package de.jkovacs.holopt

import org.apache.spark.sql._

object SparkDataframe {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[4]")
      .appName("Example A (Dataframe)")
      .getOrCreate()
    import spark.implicits._

    val students = University.students.toDF().alias("s")
    val courses = University.courses.toDF().alias("c")
    val attends = University.attends.toDF().alias("a")

    `Example A`(students, attends, courses).show(false)
    `Example A'`(students, attends, courses).show(false)

  }

  def `Example A`(students: DataFrame, attends: DataFrame, courses: DataFrame): DataFrame = {
    import students.sparkSession.implicits._

    val studentsAttendingCourses = students
      .join(attends, $"s.id" === $"a.sid").withColumnRenamed("faculty", "sfac")
      .join(courses, $"cid" === $"c.id").withColumnRenamed("faculty", "cfac")

    val k = studentsAttendingCourses.alias("l").joinWith(studentsAttendingCourses.alias("r"),
      $"l.cid" === $"r.cid"
        && $"l.sfac" =!= $"l.cfac"
        && $"r.sfac" === $"r.cfac"
        && $"l.points" >= $"r.points")

    // Note: sid not actually the same field as student.id, but originates from attends DataFrame
    val result = k.select("_1.sid", "_1.name.last", "_2.sid")
    result
  }

  def `Example A'`(students: DataFrame, attends: DataFrame, courses: DataFrame): DataFrame = {
    import students.sparkSession.implicits._

    val `s'` = students.selectExpr("id as sid", "faculty as sfac", "name.last as snmelast").alias("s")
    val `a'` = attends.selectExpr("sid as asid", "cid as acid", "points").alias("a")
    val `c'` = courses.selectExpr("id as cid", "faculty as cfac").alias("c")

    val studentsAttendingCourses = `s'`
      .join(`a'`, $"s.sid" === $"a.asid")
      .join(`c'`, $"a.acid" === $"c.cid")
      .select("sid", "sfac", "snmelast", "cfac", "cid", "points")

    val k = studentsAttendingCourses.alias("l").joinWith(studentsAttendingCourses.alias("r"),
      $"l.cid" === $"r.cid"
        && $"l.sfac" =!= $"l.cfac"
        && $"r.sfac" === $"r.cfac"
        && $"l.points" >= $"r.points")

    val result = k.select("_1.sid", "_1.snmelast", "_2.sid")
    result
  }
}
