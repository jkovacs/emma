package de.jkovacs.holopt

object University {

  // NOTE: Cannot use Symbols in Spark
  case class Name(first: String, last: String)

  case class Student(id: Int, gender: String, faculty: String, name: Name)

  case class Course(id: Int, title: String, credits: Short, faculty: String, description: String)

  case class Attends(sid: Int, cid: Int, points: Int, term: String)


  val students: Seq[Student] = Seq(
    Student(1, "M", "CS", Name("John", "Doe")),
    Student(2, "F", "Math", Name("Jane", "Doe")),
    Student(3, "M", "CS", Name("Jim", "Doe"))
  )

  val courses: Seq[Course] = Seq(
    Course(1, "CS 101", 6, "CS", "..."),
    Course(2, "LinAlg I", 6, "Math", "...")
  )

  val attends: Seq[Attends] = Seq(
    Attends(1, 1, 90, "Summer 16"),
    Attends(1, 2, 75, "Summer 16"),
    Attends(2, 1, 95, "Summer 16"),
    Attends(2, 2, 70, "Winter 15/16"),
    Attends(3, 2, 75, "Summer 16")
  )
}
