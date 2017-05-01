package mesosphere.marathon
package api.akkahttp

import akka.http.scaladsl.server.Route

trait Controller {
  val route: Route
}
