package mesosphere.marathon
package api.akkahttp.v2

import akka.event.EventStream
import akka.actor.ActorSystem
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import mesosphere.marathon.api.v2.{ AppNormalization, AppTasksResource, LabelSelectorParsers, InfoEmbedResolver }
import mesosphere.marathon.api.akkahttp.{ BaseHandler, LeaderDirectives, AuthDirectives, EntityMarshallers }
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

class AppsHandler(
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
    with AuthDirectives with AppConversion {

  private implicit lazy val validateApp = AppDefinition.validAppDefinition(config.availableFeatures)(pluginManager)
  private implicit lazy val updateValidator = AppValidation.validateCanonicalAppUpdateAPI(config.availableFeatures)

  import AppsHandler._
  import EntityMarshallers._

  import mesosphere.marathon.api.v2.json.Formats._

  val listApps: Route = {
    def index(cmd: Option[String], id: Option[String], label: Option[String], embed: Set[String])(implicit identity: Identity): Future[Seq[AppInfo]] = {
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
    authenticated { implicit identity =>
      parameters('cmd.?, 'id.?, 'label.?, 'embed.*) { (cmd, id, label, embed) =>
        onSuccess(index(cmd, id, label, embed.toSet))(apps => complete(Json.obj("apps" -> apps)))
      }
    }
  }

  private val createApp: Route = {
    def create(app: AppDefinition, force: Boolean)(implicit identity: Identity): Future[(DeploymentPlan, AppInfo)] = {

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
    authenticated { implicit identity =>
      entity(as[AppDefinition]) { app =>
        authorized(CreateRunSpec, app, identity) {
          parameters('force.as[Boolean].?(false)) { force =>
            onSuccess(create(app, force)) { (plan, app) =>
              //TODO: post ApiPostEvent
              complete((StatusCodes.Created, Seq(Deployment(plan)), app))
            }
          }
        }
      }
    }
  }

  private val showApp: Route = {
    authenticated { implicit identity =>
      path(RemainingPath) { id =>
        parameters('embed.*) { embed =>
          val appId = id.toString().toRootPath
          val resolvedEmbed = InfoEmbedResolver.resolveApp(embed.toSet) ++ Set(
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
              // complete(StatusCodes.NotFound -> Message.appNotFound(appId))
              complete("hi")
          }
        }
      }
    }
  }

  val apps: Route = {
    asLeader {
      pathPrefix("v2" / "apps") {
        pathEnd {
          post(createApp) ~ get(listApps)
        } ~
          get(showApp)
      }
    }
  }

  private val normalizationConfig = AppNormalization.Configure(config.defaultNetworkName.get, config.mesosBridgeName())
  private implicit val normalizeApp: Normalization[raml.App] =
    appNormalization(NormalizationConfig(config.availableFeatures, normalizationConfig))(AppNormalization.withCanonizedIds())
}

object AppsHandler {

  def appNormalization(config: NormalizationConfig): Normalization[raml.App] = Normalization { app =>
    validateOrThrow(app)(AppValidation.validateOldAppAPI)
    val migrated = AppNormalization.forDeprecated(config.config).normalized(app)
    validateOrThrow(migrated)(AppValidation.validateCanonicalAppAPI(config.enabledFeatures))
    AppNormalization(config.config).normalized(migrated)
  }
}