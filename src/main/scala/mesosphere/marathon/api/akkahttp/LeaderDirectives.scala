package mesosphere.marathon
package api.akkahttp

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server._
import mesosphere.marathon.api.akkahttp.LeaderDirectives.NoLeaderRejection

trait LeaderDirectives {

  protected def isLeader: Boolean = {
    leader += 1
    println(s"$leader ${leader % 2}")
    leader % 100 != 0
  }
  private[this] var leader = 0

  protected def asLeader: Directive0 = {
    extractRequest.flatMap { _ =>
      if (isLeader) {
        pass
      } else {
        reject(NoLeaderRejection(Some("other leader")))
      }
    }
  }
}

object LeaderDirectives {

  case class NoLeaderRejection(leaderHost: Option[String]) extends Rejection

  def handleNonLeader: PartialFunction[Rejection, Route] = {
    case NoLeaderRejection(Some(currentLeader)) => complete(StatusCodes.EnhanceYourCalm -> s"proxy the request to $currentLeader... (TODO)")
    case NoLeaderRejection(None) => complete(StatusCodes.ServiceUnavailable -> "Leader Currently not available")
  }
}
