package mesosphere.marathon
package api.akkahttp

import java.time.OffsetDateTime

import akka.NotUsed
import akka.actor.ActorSystem
import akka.event.EventStream
import akka.stream.scaladsl.Source
import mesosphere.marathon
import mesosphere.marathon.core.appinfo.{ AppInfo, AppInfoService, AppSelector }
import mesosphere.marathon.core.base.Clock
import mesosphere.marathon.core.deployment.DeploymentPlan
import mesosphere.marathon.core.group.GroupManager
import mesosphere.marathon.core.instance.Instance
import mesosphere.marathon.core.plugin.PluginManager
import mesosphere.marathon.core.pod.PodDefinition
import mesosphere.marathon.plugin.auth.{ Authenticator, AuthorizedAction, Authorizer, Identity }
import mesosphere.marathon.plugin.http.{ HttpRequest, HttpResponse }
import mesosphere.marathon.state._

import scala.concurrent.Future

trait ServiceMocks {

  // required
  val system: ActorSystem

  // provided

  object AuthAllowEverything extends Authorizer with Authenticator {

    private[this] val defaultIdentity = Future.successful(Some(new Identity {}))

    override def authenticate(request: HttpRequest): Future[Option[Identity]] = defaultIdentity

    override def handleNotAuthenticated(request: HttpRequest, response: HttpResponse): Unit = {}

    override def handleNotAuthorized(principal: Identity, response: HttpResponse): Unit = {}

    override def isAuthorized[Resource](
      principal: Identity,
      action: AuthorizedAction[Resource],
      resource: Resource): Boolean = true
  }

  implicit val authenticator: Authenticator = AuthAllowEverything

  implicit val authorizer: Authorizer = AuthAllowEverything

  lazy val appInfoService = new AppInfoService {

    def info(id: PathId = PathId("/foo")): AppInfo = AppInfo(AppDefinition(id = id))

    override def selectAppsBy(selector: AppSelector, embed: Set[AppInfo.Embed]): Future[marathon.Seq[AppInfo]] = {
      Future.successful(Seq(info()))
    }

    override def selectApp(appId: PathId, selector: AppSelector, embed: Set[AppInfo.Embed]): Future[Option[AppInfo]] = {
      Future.successful(Some(info(appId)))
    }

    override def selectAppsInGroup(groupId: PathId, selector: AppSelector, embed: Set[AppInfo.Embed]): Future[marathon.Seq[AppInfo]] = {
      Future.successful(Seq(info()))
    }
  }

  def appTasksRes: mesosphere.marathon.api.v2.AppTasksResource = ???

  def clock: Clock = Clock()

  def config: MarathonConf = AllConf.withTestConfig()

  def eventBus: EventStream = system.eventStream

  def groupManager: GroupManager = new GroupManager {
    override def app(id: PathId): Option[AppDefinition] = None
    override def appVersion(id: PathId, version: OffsetDateTime): Future[Option[AppDefinition]] = Future.successful(None)
    override def rootGroup(): RootGroup = RootGroup.empty
    override def podVersion(id: PathId, version: OffsetDateTime): Future[Option[PodDefinition]] = Future.successful(None)
    override def pod(id: PathId): Option[PodDefinition] = None
    override def runSpec(id: PathId): Option[RunSpec] = None
    override def podVersions(id: PathId): Source[OffsetDateTime, NotUsed] = Source.empty
    override def updateRoot(id: PathId, fn: (RootGroup) => RootGroup, version: Timestamp, force: Boolean, toKill: Map[PathId, Seq[Instance]]): Future[DeploymentPlan] = Future.successful(DeploymentPlan.empty)
    override def versions(id: PathId): Source[Timestamp, NotUsed] = Source.empty
    override def appVersions(id: PathId): Source[OffsetDateTime, NotUsed] = Source.empty
    override def group(id: PathId): Option[Group] = None
    override def group(id: PathId, version: Timestamp): Future[Option[Group]] = Future.successful(None)
  }

  def pluginManager: PluginManager = PluginManager.None

  def service: MarathonSchedulerService = ???
}
