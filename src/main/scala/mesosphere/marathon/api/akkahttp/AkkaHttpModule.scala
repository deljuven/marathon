package mesosphere.marathon
package api.akkahttp

import akka.actor.ActorSystem
import com.google.inject.AbstractModule
import com.google.inject.Scopes
import mesosphere.marathon.MarathonConf
import mesosphere.marathon.api.MarathonHttpService
import v2.AppsHandler
import com.google.inject.{ Provides, Scopes, Singleton }

class AkkaHttpModule(conf: MarathonConf) extends AbstractModule {
  override def configure(): Unit = {
    bind(classOf[MarathonHttpService]).to(classOf[AkkaHttpMarathonService]).in(Scopes.SINGLETON)
  }

  @Provides
  @Singleton
  def serviceMocks(actorSystem: ActorSystem): ServiceMocks =
    new ServiceMocks(actorSystem)

  @Provides
  @Singleton
  def provideAppsHandler(serviceMocks: ServiceMocks, marathonSchedulerService: MarathonSchedulerService,
    appTasksRes: mesosphere.marathon.api.v2.AppTasksResource): AppsHandler = {
    implicit val as = serviceMocks.system
    import as.dispatcher
    import serviceMocks.authenticator
    import serviceMocks.authorizer
    new AppsHandler(
      clock = serviceMocks.clock,
      eventBus = serviceMocks.eventBus,
      appTasksRes = appTasksRes,
      service = marathonSchedulerService,
      appInfoService = serviceMocks.appInfoService,
      config = serviceMocks.config,
      groupManager = serviceMocks.groupManager,
      pluginManager = serviceMocks.pluginManager)
  }
}
