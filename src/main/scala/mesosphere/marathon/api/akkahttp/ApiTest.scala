package mesosphere.marathon
package api.akkahttp

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import akka.stream.ActorMaterializer
import mesosphere.marathon.api.akkahttp.v2.AppsHandler
import mesosphere.marathon.plugin.auth._
import play.api.libs.json.Json

case class Foo(name: String, child: Option[Foo])
object Foo {
  implicit val format = Json.format[Foo]
}

trait Service extends LeaderDirectives with AuthDirectives {
  import EntityMarshallers._

  val routes: Route = {
    asLeader {
      authenticated { implicit identity =>
        authorized(ViewResource, AuthorizedResource.SystemConfig, identity) {
          path("ping") {
            get { complete(s"pong as $identity") }
          } ~ path("foo") {
            entity(as[Foo]) { foo =>
              complete(StatusCodes.Created -> foo)
            }
          } ~ path("param") {
            get {
              parameters('search.as[Int], 'cmd.?) { (search, cmd) =>
                complete(s"I got: search: $search cmd: $cmd")
              }
            }
          }
        }
      }
    }
  }
}

object ApiTest extends App with Service with AppsHandler with BaseHandler with ServiceMocks {
  implicit val system = ActorSystem()
  implicit val executor = system.dispatcher
  implicit val materializer = ActorMaterializer()

  val allRoutes = decodeRequest(encodeResponse(routes ~ apps))

  // encode response will gzip if needed
  Http().bindAndHandle(allRoutes, "localhost", 8080)
}
