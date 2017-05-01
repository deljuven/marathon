package mesosphere.marathon
package api.akkahttp

import akka.http.scaladsl.model.StatusCodes._
import akka.http.scaladsl.model.headers.CustomHeader
import akka.http.scaladsl.server._
import akka.http.scaladsl.server.Directives._
import mesosphere.marathon.core.deployment.DeploymentPlan
import mesosphere.marathon.state.{ PathId, Timestamp }
import play.api.libs.json.Json

trait BaseHandler {
  type Message = BaseHandler.Message
  val Message = BaseHandler.Message
  val Deployment = BaseHandler.Deployment
  implicit def rejectionHandler =
    RejectionHandler.newBuilder()
      .handle(LeaderDirectives.handleNonLeader)
      .handle(EntityMarshallers.handleNonValid)
      .handle {
        case ValidationRejection(msg, _) =>
          complete((InternalServerError, "That wasn't valid! " + msg))
      }
      .handleAll[MethodRejection] { methodRejections =>
        val names = methodRejections.map(_.supported.name)
        complete((MethodNotAllowed, s"Can't do that! Supported: ${names mkString " or "}!"))
      }
      .handleNotFound { complete((NotFound, "Not here!")) }
      .result()
}

object BaseHandler {
  case class Deployment(plan: DeploymentPlan) extends CustomHeader {
    override def name(): String = "Marathon-Deployment-Id"
    override def value(): String = plan.id
    override def renderInResponses(): Boolean = true
    override def renderInRequests(): Boolean = false
  }

  case class Message(message: String)
  object Message {
    implicit val messageFormat = Json.format[Message]
    def appNotFound(id: PathId, version: Option[Timestamp] = None): Message = {
      Message(s"App '$id' does not exist" + version.fold("")(v => s" in version $v"))
    }
  }
}
