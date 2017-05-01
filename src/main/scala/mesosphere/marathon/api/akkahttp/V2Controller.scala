package mesosphere.marathon
package api.akkahttp

import akka.http.scaladsl.server.Route
import akka.http.scaladsl.server.Directives._
import v2.AppsController

class V2Controller(appsController: AppsController) extends Controller {
  override val route: Route = {
    pathPrefix("apps") {
      appsController.route
    }
  }
}
