package mesosphere.marathon
package api.akkahttp.v2

import akka.event.EventStream
import akka.actor.ActorSystem
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import mesosphere.marathon.api.v2.{ AppNormalization, AppTasksResource, LabelSelectorParsers, InfoEmbedResolver }
import mesosphere.marathon.api.akkahttp.{ Controller, BaseHandler, LeaderDirectives, AuthDirectives, EntityMarshallers }
import mesosphere.marathon.api.v2.AppsResource.{ NormalizationConfig, authzSelector }
import mesosphere.marathon.api.v2.Validation.validateOrThrow
import mesosphere.marathon.api.v2.validation.AppValidation
import mesosphere.marathon.core.appinfo._
import mesosphere.marathon.core.base.Clock
import mesosphere.marathon.core.deployment.DeploymentPlan
import mesosphere.marathon.core.group.GroupManager
import mesosphere.marathon.core.plugin.PluginManager
import mesosphere.marathon.plugin.auth.{ CreateRunSpec, Identity, ViewResource, Authenticator, Authorizer }
import mesosphere.marathon.raml.AppConversion
import mesosphere.marathon.state.{ AppDefinition, Identifiable, PathId }
import play.api.libs.json.Json
import PathId._

import scala.concurrent.{ ExecutionContext, Future }

class AppsController(
  val clock: Clock,
  val eventBus: EventStream,
  val appTasksRes: AppTasksResource,
  val service: MarathonSchedulerService,
  val appInfoService: AppInfoService,
  val config: MarathonConf,
  val groupManager: GroupManager,
  val pluginManager: PluginManager)(
  implicit
  val actorSystem: ActorSystem,
  val executionContext: ExecutionContext,
  val authenticator: Authenticator,
  val authorizer: Authorizer
) extends BaseHandler with LeaderDirectives
    with AuthDirectives with AppConversion with Controller {

  private implicit lazy val validateApp = AppDefinition.validAppDefinition(config.availableFeatures)(pluginManager)
  private implicit lazy val updateValidator = AppValidation.validateCanonicalAppUpdateAPI(config.availableFeatures)

  import AppsController._
  import EntityMarshallers._

  import mesosphere.marathon.api.v2.json.Formats._

  private def listApps(cmd: Option[String], id: Option[String], label: Option[String], embed: Set[String])(implicit identity: Identity): Route = {
    def index: Future[Seq[AppInfo]] = {
      def containCaseInsensitive(a: String, b: String): Boolean = b.toLowerCase contains a.toLowerCase

      val selectors = Seq[Option[Selector[AppDefinition]]](
        cmd.map(c => Selector(_.cmd.exists(containCaseInsensitive(c, _)))),
        id.map(s => Selector(app => containCaseInsensitive(s, app.id.toString))),
        label.map(new LabelSelectorParsers().parsed),
        Some(authzSelector)
      ).flatten
      val resolvedEmbed = InfoEmbedResolver.resolveApp(embed) + AppInfo.Embed.Counts + AppInfo.Embed.Deployments
      appInfoService.selectAppsBy(Selector.forall(selectors), resolvedEmbed)
    }
    onSuccess(index)(apps => complete(Json.obj("apps" -> apps)))
  }

  private def createApp(app: AppDefinition, force: Boolean)(implicit identity: Identity): Route = {
    def create: Future[(DeploymentPlan, AppInfo)] = {

      def createOrThrow(opt: Option[AppDefinition]) = opt
        .map(_ => throw ConflictingChangeException(s"An app with id [${app.id}] already exists."))
        .getOrElse(app)

      groupManager.updateApp(app.id, createOrThrow, app.version, force).map { plan =>
        val appWithDeployments = AppInfo(
          app,
          maybeCounts = Some(TaskCounts.zero),
          maybeTasks = Some(Seq.empty),
          maybeDeployments = Some(Seq(Identifiable(plan.id)))
        )
        plan -> appWithDeployments
      }
    }
    authorized(CreateRunSpec, app, identity) {
      onSuccess(create) { (plan, app) =>
        //TODO: post ApiPostEvent
        complete((StatusCodes.Created, Seq(Deployment(plan)), app))
      }
    }
  }

  private def showApp(appId: PathId, embed: Set[String])(implicit identity: Identity): Route = {
    val resolvedEmbed = InfoEmbedResolver.resolveApp(embed) ++ Set(
      // deprecated. For compatibility.
      AppInfo.Embed.Counts, AppInfo.Embed.Tasks, AppInfo.Embed.LastTaskFailure, AppInfo.Embed.Deployments
    )
    onSuccess(appInfoService.selectApp(appId, authzSelector, resolvedEmbed)) {
      case Some(info) =>
        authorized(ViewResource, info.app, identity) {
          complete(Json.obj("app" -> info))
        }
      case None =>
        (StatusCodes.NotFound -> Message.appNotFound(appId))
        complete(StatusCodes.NotFound -> Message.appNotFound(appId))
    }
  }

  val RemainingPathId = RemainingPath.map(_.toString.toRootPath)

  val route: Route = {
    asLeader {
      authenticated { implicit identity =>
        pathEnd {
          post {
            (entity(as[AppDefinition]) & parameters('force.as[Boolean].?(false))) { (app, force) =>
              createApp(app, force)
            }
          } ~
            get {
              parameters('cmd.?, 'id.?, 'label.?, 'embed.*) { (cmd, id, label, embed) =>
                listApps(cmd, id, label, embed.toSet)
              }
            }
        } ~
          get {
            (path(RemainingPathId) & parameters('embed.*)) { (appId, embed) =>
              showApp(appId, embed.toSet)
            }
          }
      }
    }
  }

  private val normalizationConfig = AppNormalization.Configure(config.defaultNetworkName.get, config.mesosBridgeName())
  private implicit val normalizeApp: Normalization[raml.App] =
    appNormalization(NormalizationConfig(config.availableFeatures, normalizationConfig))(AppNormalization.withCanonizedIds())
}

object AppsController {

  def appNormalization(config: NormalizationConfig): Normalization[raml.App] = Normalization { app =>
    validateOrThrow(app)(AppValidation.validateOldAppAPI)
    val migrated = AppNormalization.forDeprecated(config.config).normalized(app)
    validateOrThrow(migrated)(AppValidation.validateCanonicalAppAPI(config.enabledFeatures))
    AppNormalization(config.config).normalized(migrated)
  }
}