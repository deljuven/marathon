package mesosphere.marathon
package api.akkahttp

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.stream.ActorMaterializer
import com.google.common.util.concurrent.AbstractIdleService
import com.google.inject.Inject
import com.typesafe.scalalogging.StrictLogging
import mesosphere.chaos.http.HttpConf
import mesosphere.marathon.api.MarathonHttpService
import mesosphere.marathon.api.akkahttp.v2.AppsHandler
import scala.concurrent.Future
import v2.AppsHandler
import scala.async.Async._

class AkkaHttpMarathonService @Inject() (
    config: MarathonConf with HttpConf,
    appsHandler: AppsHandler
)(
    implicit
    actorSystem: ActorSystem) extends AbstractIdleService with MarathonHttpService with StrictLogging {
  import actorSystem.dispatcher
  implicit val materializer = ActorMaterializer()
  private var handler: Option[Future[Http.ServerBinding]] = None

  override def startUp(): Unit = synchronized {
    if (handler.isEmpty) {
      logger.info(s"Listening via Akka HTTP on ${config.httpPort()}")
      handler = Some(Http().bindAndHandle(appsHandler.apps, "localhost", config.httpPort()))
    } else {
      logger.error("Service already started")
    }
  }

  override def shutDown(): Unit = {
    val unset = synchronized {
      if (handler.isEmpty)
        None
      else {
        val oldHandler = handler
        handler = None
        oldHandler
      }
    }

    unset.foreach { oldHandlerF =>
      async {
        val oldHandler = await(oldHandlerF)
        logger.info(s"Shutting down Akka HTTP service on ${config.httpPort()}")
        val unbound = await(oldHandler.unbind())
        logger.info(s"Akka HTTP service on ${config.httpPort()} is stopped")
      }
    }
  }
}
