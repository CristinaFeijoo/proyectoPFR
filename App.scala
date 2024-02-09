package ec.edu.utpl.computacion.pfr.pi

import com.github.tototoshi.csv.*
import org.nspl.awtrenderer.*
import org.nspl.data.HistogramData
import org.nspl.*
import org.nspl.par.ylab

import java.io.File
import doobie.*
import doobie.implicits.*
import cats.*
import cats.effect.*
import cats.effect.unsafe.implicits.global
import cats.implicits.*
import doobie.util.transactor.Transactor.Aux

import java.io.File



implicit object CustomFormat extends DefaultCSVFormat {
  override val delimiter: Char = ';'
}

object App {
  @main def Iniciar(): Unit = {
  val xa: Aux[IO, Unit] =Transactor.fromDriverManager[IO](
    driver = "com.mysql.cj.jdbc.Driver",
    url = "jdbc:mysql://localhost:3306/db_tonotos",
    user = "root",
    pass = "CristinaFoxy87"
  )
  // Consulta para obtener los detalles de todos los partidos jugados en un torneo específico
  val tournamentId = "ID_DEL_TORNEO"

  val query1: Query0[(Int, Int, Int)] = sql"""
  SELECT matches_match_id, matches_home_team_id, matches_away_team_id
  FROM matches
  WHERE matches_tournament_id = $tournamentId
  """.query[(Int, Int, Int)]
  // Consulta para obtener los partidos en los que un equipo específico fue el equipo local

  val matchesInTournament: IO[List[(Int, Int, Int)]] = query1.to[List].transact(xa)

  val teamId = "ID_DEL_EQUIPO"

  val query2: Query0[Int] = sql"""
  * SELECT matches_match_id
  * FROM matches
   WHERE matches_home_team_id = $teamId
   """.query[Int]
  //Consulta para obtener los partidos que resultaron en un empate

  val homeMatches: IO[List[Int]] = query2.to[List].transact(xa)

  val query3: Query0[(Int, Int)] = sql"""
   SELECT matches_match_id, matches_home_team_score
   FROM matches
   WHERE matches_home_team_score = matches_away_team_score
   """.query[(Int, Int)]

  val drawMatches: IO[List[(Int, Int)]] = query3.to[List].transact(xa)
    ejecutar()
    }

  def ejecutar()  = {
    val directory = "C:/Users/crist/OneDrive/Escritorio/ArchivoPIntegrador/"
    val pathDataFile = directory + "dsAlineacionesXTorneo.csv"
    val pathDataFile2 = directory + "dsPartidosYGoles.csv"
    val reader = CSVReader.open(new File(pathDataFile))
    val reader2 = CSVReader.open(new File(pathDataFile2))
    val contentFile: List[Map[String, String]] = reader.allWithHeaders()
    val contentFile2: List[Map[String, String]] = reader2.allWithHeaders()

    def calcularPromedioGoles(data: List[Map[String, String]]): Double = {
      val golesPorPartido = data.flatMap { row =>
        for {
          homeGoals <- row.get("matches_home_team_score").map(_.toInt)
          awayGoals <- row.get("matches_away_team_score").map(_.toInt)
        } yield homeGoals + awayGoals
      }

      if (golesPorPartido.nonEmpty) golesPorPartido.sum.toDouble / golesPorPartido.length.toDouble
      else 0.0
    }

    val promedioGoles = calcularPromedioGoles(contentFile2)
    println(s"El promedio de goles por partido es: $promedioGoles")

    def encontrarMinPenales(data: List[Map[String, String]]): (String, Int) = {
      var minPenales = Int.MaxValue
      var torneoMinPenales = ""

      for (row <- data) {
        val nombreTorneo = row("tournaments_tournament_name")
        val penalesLocal = row.get("matches_home_team_score_penalties").map(_.toInt).getOrElse(0)
        val penalesVisitante = row.get("matches_away_team_score_penalties").map(_.toInt).getOrElse(0)
        val totalPenales = penalesLocal + penalesVisitante

        if (totalPenales < minPenales) {
          minPenales = totalPenales
          torneoMinPenales = nombreTorneo
        }
      }

      (torneoMinPenales, minPenales)
    }

    val (torneo, cantidadPenales) = encontrarMinPenales(contentFile2)
    println(s"El torneo con el menor número de penaltis es: $torneo, con $cantidadPenales penales hechos.")

    def encontrarMundialConMasAutogoles(data: List[Map[String, String]]): (String, Int) = {
      val autogolLocales: List[(String, String)] = data
        .filter(row => row("goals_own_goal") == "1" && row("home_team_name").nonEmpty)
        .map(row => (row("tournaments_tournament_name"), row("goals_own_goal")))

      val autogolesPorMundial = autogolLocales.groupBy(_._1).mapValues(_.size)

      val (mundialConMasAutogoles: String, cantidadAutogoles: Int) = autogolesPorMundial.maxBy(_._2)

      (mundialConMasAutogoles, cantidadAutogoles)
    }

    val (mundialAutogoles, cantidadAutogoles) = encontrarMundialConMasAutogoles(contentFile2)
    println(s"El Mundial con más autogoles fue: $mundialAutogoles, con $cantidadAutogoles autogoles.")

    def charting(data: List[Map[String, String]]): Unit = {
      val womensTournaments = data.filter(row => row("tournaments_tournament_name").contains("Women"))

      val mundialMujeresCounts = womensTournaments.groupBy(_("tournaments_tournament_name")).mapValues(_.size).toList

      val counts = mundialMujeresCounts.map(_._2.toDouble)

      val histMundialesMujeres = xyplot(HistogramData(counts, 10) -> bar())(
        par
          .xlab("Número de mundiales de mujeres")
        .ylab("Frecuencia")
        .main("Histograma de número de mundiales de mujeres")
      )

      // Directorio donde se guardará la imagen
      val imagePath = "C://Users//crist//OneDrive//Escritorio//ArchivoPIntegrador//histograma_mundiales_mujeres.png"

      // Guardar el histograma com una imagen PNG
      pngToFile(new File(imagePath), histMundialesMujeres.build, 1000)
    }

    charting(contentFile2)

    def charting2(data: List[Map[String, String]]): Unit = {
      val tournamentsData = data.filter(_.contains("tournaments_tournament_name"))

      val tournamentMatchesCounts = tournamentsData.groupBy(_("tournaments_tournament_name")).mapValues(_.size).toList

      val counts = tournamentMatchesCounts.map(_._2.toDouble)

      val histTournamentMatches = xyplot(HistogramData(counts, 10) -> bar())(
        par
          .xlab("Número de partidos por torneo")
          .ylab("Frecuencia")
          .main("Histograma de número de partidos por torneo")
      )

      val imagePath = "C://Users//crist//OneDrive//Escritorio//ArchivoPIntegrador//histograma_partidos_por_torneo.png"

      pngToFile(new File(imagePath), histTournamentMatches.build, 1000)
    }

    charting2(contentFile2)

    def charting3(data: List[Map[String, String]]): Unit = {
      val scores = data.flatMap(_("matches_home_team_score").toIntOption)
      val hist = xyplot(
        HistogramData(scores.map(_.toDouble), 10) -> bar()
      )(
        par
          .xlab("Goles")
          .ylab("Frecuencia")
          .main("Histograma de Goles")
      )

      val imagePath = "C://Users//crist//OneDrive//Escritorio//ArchivoPIntegrador//histograma_goles.png"
      pngToFile(new File(imagePath), hist.build, 1000)
    }

    charting3(contentFile2)


    reader.close()
    reader2.close()

    println(contentFile.take(2))
    println(contentFile2.take(2))

    println(s"Filas: ${contentFile.length} y columnas: ${contentFile.headOption.map(_.keys.size).getOrElse(0)}")
    println(s"Filas: ${contentFile2.length} y columnas: ${contentFile2.headOption.map(_.keys.size).getOrElse(0)}")
  }
  }





