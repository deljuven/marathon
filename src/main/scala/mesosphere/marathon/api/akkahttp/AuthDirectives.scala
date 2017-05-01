package mesosphere.marathon
package api.akkahttp

import akka.http.scaladsl.model.HttpRequest
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server._
import mesosphere.marathon.plugin.auth.{ Authenticator, AuthorizedAction, Authorizer, Identity }
import mesosphere.marathon.plugin.http.{ HttpRequest => PluginRequest, HttpResponse => PluginResponse }

import scala.util.{ Failure, Success }

trait AuthDirectives {
  implicit val authenticator: Authenticator
  implicit val authorizer: Authorizer

  import AuthDirectives._

  protected def authenticated: Directive1[Identity] = extractRequest.flatMap { request =>
    val pluginRequest = toPluginRequest(request)
    onComplete(authenticator.authenticate(pluginRequest)).flatMap {
      case Success(Some(identity)) => provide(identity)
      case Success(None) => reject(NotAuthenticated(toResponse(authenticator.handleNotAuthenticated(pluginRequest, _))))
      case Failure(_) => reject(AuthServiceUnavailable)
    }
  }

  protected def authorized[Resource](action: AuthorizedAction[Resource], resource: Resource, identity: Identity): Directive0 = extractRequest.flatMap { request =>
    if (authorizer.isAuthorized(identity, action, resource)) pass
    else reject(NotAuthorized(toResponse(authorizer.handleNotAuthorized(identity, _))))
  }
}

object AuthDirectives {

  trait ToResponse {

  }
  case object AuthServiceUnavailable extends Rejection
  case class NotAuthorized(toResponse: ToResponse) extends Rejection
  case class NotAuthenticated(toResponse: ToResponse) extends Rejection

  def toPluginRequest(request: HttpRequest): PluginRequest = new PluginRequest {
    override def method: String = request.method.value

    override def cookie(name: String): Option[String] = request.cookies.find(_.name == name).map(_.value)

    override def localPort: Int = 123

    override def remotePort: Int = 123

    override def header(name: String): Seq[String] = request.headers.filter(_.name == name).map(_.value())

    override def queryParam(name: String): Seq[String] = request.uri.query().filter(_._1 == name).map(_._2)

    override def localAddr: String = ""

    override def requestPath: String = request.uri.path.toString()

    override def remoteAddr: String = ""
  }

  def toResponse(action: (PluginResponse) => Unit): ToResponse = {
    val builder = new PluginResponse with ToResponse {

      override def cookie(name: String, value: String, maxAge: Int, secure: Boolean): Unit = ???

      override def body(mediaType: String, bytes: Array[Byte]): Unit = ???

      override def sendRedirect(url: String): Unit = ???

      override def header(header: String, value: String): Unit = ???

      override def status(code: Int): Unit = ???
    }
    action(builder)
    builder
  }
}
