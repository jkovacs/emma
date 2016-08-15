package de.jkovacs.holopt

import de.jkovacs.holopt.University.{Attends, Course, Student}
import org.apache.spark.sql._

object SparkDataset {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[4]")
      .appName("Example A (Datasets)")
      .getOrCreate()
    import spark.implicits._

    val students = University.students.toDS()
    val courses = University.courses.toDS()
    val attends = University.attends.toDS()

    `Example A`(students, attends, courses).show(false)
    `Example A'`(students, attends, courses).show(false)

  }

  def `Example A`(students: Dataset[Student], attends: Dataset[Attends], courses: Dataset[Course]): Dataset[(Int, String, Int)] = {
    import students.sparkSession.implicits._

    val studentsAttends = students.joinWith(attends, $"id" === $"sid")
    val studentsAttendingCourses = studentsAttends
      .joinWith(courses, $"_2.cid" === $"id")
      .map { case ((s, a), c) => (s, a, c) }

    val k = studentsAttendingCourses.alias("l").joinWith(studentsAttendingCourses.alias("r"),
      $"l._3.id" === $"r._3.id"
        && $"l._1.faculty" =!= $"l._3.faculty"
        && $"r._1.faculty" === $"r._3.faculty"
        && $"l._2.points" >= $"r._2.points")

    //    val result = k.map { case ((s1, a1, c1), (s2, a2, c2)) => (s1.name, a1.points, s2.name, a2.points, c1.title)}
    val result = k.map { case ((s1, a1, c1), (s2, a2, c2)) => (s1.id, s1.name.last, s2.id) }
    result
  }

  def `Example A'`(students: Dataset[Student], attends: Dataset[Attends], courses: Dataset[Course]): Dataset[(Int, String, Int)] = {
    import students.sparkSession.implicits._

    val `s'` = students.map(s => (s.id, s.faculty, s.name.last)).alias("s")
    val `a'` = attends.map(a => (a.sid, a.cid, a.points)).alias("a")
    val `c'` = courses.map(c => (c.id, c.faculty)).alias("c")

    val studentsAttends = `s'`.joinWith(`a'`, $"s._1" === $"a._1").alias("sa")
    val studentsAttendingCourses = studentsAttends
      .joinWith(`c'`, $"sa._2._2" === $"c._1")
      .map { case ((s, a), c) => (s._1, s._2, s._3, c._2, c._1, a._3) }

    val k = studentsAttendingCourses.alias("l").joinWith(studentsAttendingCourses.alias("r"),
      $"l._5" === $"r._5"
        && $"l._2" =!= $"l._4"
        && $"r._2" === $"r._4"
        && $"l._6" >= $"r._6")

    val result = k.map { case (sac1, sac2) => (sac1._1, sac1._3, sac2._1) }
    result
  }
}
