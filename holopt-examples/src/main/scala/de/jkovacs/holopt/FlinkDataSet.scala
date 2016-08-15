package de.jkovacs.holopt

import de.jkovacs.holopt.University.{Attends, Course, Student}
import org.apache.flink.api.scala._


object FlinkDataSet {

  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    env.getConfig.disableSysoutLogging()

    val students = env.fromCollection(University.students)
    val attends = env.fromCollection(University.attends)
    val courses = env.fromCollection(University.courses)

    `Example A`(students, attends, courses).print()
    `Example A'`(students, attends, courses).print()
  }

  def `Example A`(students: DataSet[Student], attends: DataSet[Attends], courses: DataSet[Course]): DataSet[(Int, String, Int)] = {
    val studentsAttendingCourses = students
      .join(attends).where("id").equalTo("sid")
      .join(courses).where("_2.cid").equalTo("id")
      .map(t => (t._1._1, t._1._2, t._2))

    // I think we have to move all non-equi-join conditions as a filter in front of and after the join ?
    val k = studentsAttendingCourses.filter(t => t._1.faculty != t._3.faculty)
      .join(studentsAttendingCourses.filter(t => t._1.faculty == t._3.faculty))
      .where("_3.id").equalTo("_3.id")
      .filter(t => t._1._2.points > t._2._2.points)

    val result = k.map(t => (t._1._1.id, t._1._1.name.last, t._2._1.id))
    result
  }

  def `Example A'`(students: DataSet[Student], attends: DataSet[Attends], courses: DataSet[Course]): DataSet[(Int, String, Int)] = {
    val s = students.map(s => (s.id, s.faculty, s.name.last))
    val a = attends.map(a => (a.sid, a.cid, a.points))
    val c = courses.map(c => (c.id, c.faculty))

    val studentsAttendingCourses = s
      .join(a).where("_1").equalTo("_1")
      .join(c).where("_2._2").equalTo("_1")
      .map(t => (t._1._1._1, t._1._1._2, t._1._1._3, t._2._2, t._2._1, t._1._2._3))

    val k = studentsAttendingCourses.filter(t => t._2 != t._4)
      .join(studentsAttendingCourses.filter(t => t._2 == t._4))
      .where("_5").equalTo("_5")
      .filter(t => t._1._6 > t._2._6)

    val result = k.map(t => (t._1._1, t._1._3, t._2._1))
    result
  }

}
