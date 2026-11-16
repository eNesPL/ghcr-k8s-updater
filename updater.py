#!/usr/bin/env python3
"""
GHCR K8s Updater - Lekka aplikacja dla Kubernetes / k3s + ArgoCD monitorująca
prywatne i publiczne obrazy `:latest` (lub inne tagi) na GitHub Container Registry (ghcr.io)
i wykonująca automatyczny rolling restart (odpowiednik `kubectl rollout restart`),
gdy pojawi się nowy digest obrazu.
"""

from __future__ import annotations

import base64
import datetime as dt
import json
import logging
import os
import ssl
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import asdict, dataclass, field
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Optional

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger("ghcr-updater")

MANIFEST_ACCEPT_HEADER = ", ".join(
    [
        "application/vnd.oci.image.index.v1+json",
        "application/vnd.oci.image.manifest.v1+json",
        "application/vnd.docker.distribution.manifest.list.v2+json",
        "application/vnd.docker.distribution.manifest.v2+json",
    ]
)


@dataclass
class Config:
    # Autoryzacja GHCR (GitHub Personal Access Token z read:packages)
    ghcr_username: str = field(default_factory=lambda: os.environ.get("GHCR_USERNAME", "eNesPL"))
    ghcr_token: str = field(default_factory=lambda: os.environ.get("GHCR_TOKEN", ""))

    # Opcjonalny filtr właściciela na ghcr.io (np. "enespl" - jeśli pusty, sprawdza wszystkie obrazy ghcr.io)
    ghcr_owner_filter: str = field(
        default_factory=lambda: os.environ.get("GHCR_OWNER_FILTER", "").strip().lower()
    )

    # Konfiguracja skanowania klastra K8s / k3s ("*" oznacza wszystkie przestrzenie nazw)
    namespaces: list[str] = field(
        default_factory=lambda: [
            ns.strip()
            for ns in os.environ.get("WATCH_NAMESPACES", "*").split(",")
            if ns.strip()
        ]
    )
    exclude_namespaces: list[str] = field(
        default_factory=lambda: [
            ns.strip()
            for ns in os.environ.get(
                "EXCLUDE_NAMESPACES", "kube-system,kube-public,kube-node-lease,argocd"
            ).split(",")
            if ns.strip()
        ]
    )
    watch_kinds: list[str] = field(
        default_factory=lambda: [
            k.strip().lower()
            for k in os.environ.get("WATCH_KINDS", "deployments,statefulsets,daemonsets").split(",")
            if k.strip()
        ]
    )
    # Jeśli REQUIRE_ANNOTATION=true, aktualizowane są tylko zasoby z adnotacją ghcr-updater.io/enabled: "true"
    # Jeśli false (domyślnie), monitorowane są wszystkie zasoby używające obrazów z ghcr.io (chyba że mają ghcr-updater.io/enabled: "false")
    require_annotation: bool = field(
        default_factory=lambda: os.environ.get("REQUIRE_ANNOTATION", "false").lower() == "true"
    )
    # Czy sprawdzać tylko tag :latest, czy dowolny tag na ghcr.io
    only_latest_tag: bool = field(
        default_factory=lambda: os.environ.get("ONLY_LATEST_TAG", "true").lower() == "true"
    )
    # Czy odczytywać też tokeny z imagePullSecrets przypiętych do PodSpec
    use_image_pull_secrets: bool = field(
        default_factory=lambda: os.environ.get("USE_IMAGE_PULL_SECRETS", "true").lower() == "true"
    )
    poll_interval_seconds: int = field(
        default_factory=lambda: int(os.environ.get("POLL_INTERVAL_SECONDS", "60"))
    )
    # Cooldown po restarcie (w sekundach), aby dać czas k3s na pobranie nowego obrazu i start Poda
    restart_cooldown_seconds: int = field(
        default_factory=lambda: int(os.environ.get("RESTART_COOLDOWN_SECONDS", "180"))
    )
    webhook_url: str = field(default_factory=lambda: os.environ.get("WEBHOOK_URL", ""))
    http_port: int = field(default_factory=lambda: int(os.environ.get("HTTP_PORT", "8080")))
    dry_run: bool = field(
        default_factory=lambda: os.environ.get("DRY_RUN", "false").lower() == "true"
    )


@dataclass
class ImageRef:
    registry: str
    repository: str  # np. "enespl/mtgspot-scraper"
    tag: str         # np. "latest"
    raw: str

    @property
    def owner(self) -> str:
        return self.repository.split("/", 1)[0].lower() if "/" in self.repository else ""

    @classmethod
    def parse(cls, image_str: str) -> Optional["ImageRef"]:
        """Parsuje referencję obrazu np. ghcr.io/enespl/repo:latest."""
        image_str = image_str.strip()
        if not image_str:
            return None
        # Pomijamy obrazy przypięte wyłącznie po sztywnym digeście bez tagu (image@sha256:...)
        if "@sha256:" in image_str and ":" not in image_str.split("@sha256:", 1)[0]:
            return None
        if "@sha256:" in image_str:
            image_str = image_str.split("@sha256:", 1)[0]

        parts = image_str.split("/")
        if len(parts) < 2 or "." not in parts[0]:
            return None

        registry = parts[0].lower()
        repo_and_tag = "/".join(parts[1:])
        if ":" in repo_and_tag:
            repository, tag = repo_and_tag.rsplit(":", 1)
        else:
            repository, tag = repo_and_tag, "latest"

        return cls(registry=registry, repository=repository, tag=tag, raw=image_str)


@dataclass
class WorkloadStatus:
    kind: str
    namespace: str
    name: str
    container_name: str
    image: str
    running_digest: Optional[str] = None
    remote_digest: Optional[str] = None
    last_checked: Optional[str] = None
    last_restarted: Optional[str] = None
    status: str = "unknown"  # up_to_date | updated | rollout_in_progress | error
    error_message: Optional[str] = None


class GHCRClient:
    """Klient do komunikacji z GitHub Container Registry (Docker Registry HTTP API V2)."""

    def __init__(self, default_username: str = "token", default_token: str = "", timeout: int = 15):
        self.default_username = default_username
        self.default_token = default_token
        self.timeout = timeout
        self._token_cache: dict[tuple[str, str], tuple[str, float]] = {}

    def _get_bearer_token(
        self, repository: str, username: Optional[str] = None, pat_token: Optional[str] = None
    ) -> str:
        user = username or self.default_username
        pat = pat_token if pat_token is not None else self.default_token
        repo_lower = repository.lower()
        cache_key = (repo_lower, pat[:8] if pat else "anon")
        now = time.time()

        if cache_key in self._token_cache:
            cached_token, expires_at = self._token_cache[cache_key]
            if now < expires_at:
                return cached_token

        params = urllib.parse.urlencode(
            {
                "service": "ghcr.io",
                "scope": f"repository:{repo_lower}:pull",
            }
        )
        url = f"https://ghcr.io/token?{params}"
        req = urllib.request.Request(url, method="GET")
        if pat:
            basic_auth = base64.b64encode(f"{user}:{pat}".encode("utf-8")).decode("ascii")
            req.add_header("Authorization", f"Basic {basic_auth}")

        with urllib.request.urlopen(req, timeout=self.timeout) as resp:
            payload = json.loads(resp.read().decode("utf-8"))
            token = payload.get("token") or payload.get("access_token")
            if not token:
                raise RuntimeError(f"Brak tokenu w odpowiedzi z ghcr.io/token dla {repo_lower}")
            # Tokeny GHCR są ważne zwykle 5-15 minut, buforujemy na 4 minuty
            self._token_cache[cache_key] = (token, now + 240)
            return token

    def get_remote_digest(
        self,
        image_ref: ImageRef,
        username: Optional[str] = None,
        pat_token: Optional[str] = None,
    ) -> str:
        """Pobiera aktualny Docker-Content-Digest dla obrazu z ghcr.io."""
        repo_lower = image_ref.repository.lower()
        bearer = self._get_bearer_token(repo_lower, username=username, pat_token=pat_token)
        url = f"https://ghcr.io/v2/{repo_lower}/manifests/{image_ref.tag}"

        req = urllib.request.Request(url, method="HEAD")
        req.add_header("Authorization", f"Bearer {bearer}")
        req.add_header("Accept", MANIFEST_ACCEPT_HEADER)

        try:
            with urllib.request.urlopen(req, timeout=self.timeout) as resp:
                digest = resp.headers.get("Docker-Content-Digest") or resp.headers.get(
                    "docker-content-digest"
                )
                if digest:
                    return digest.strip()
        except urllib.error.HTTPError as err:
            if err.code != 405:
                raise

        # Fallback na GET, jeśli serwer nie zwrócił nagłówka w HEAD
        req_get = urllib.request.Request(url, method="GET")
        req_get.add_header("Authorization", f"Bearer {bearer}")
        req_get.add_header("Accept", MANIFEST_ACCEPT_HEADER)
        with urllib.request.urlopen(req_get, timeout=self.timeout) as resp:
            digest = resp.headers.get("Docker-Content-Digest") or resp.headers.get(
                "docker-content-digest"
            )
            if not digest:
                raise RuntimeError(
                    f"Brak nagłówka Docker-Content-Digest dla {image_ref.raw}"
                )
            return digest.strip()


class K8sClient:
    """Lekki klient Kubernetes API działający wewnątrz klastra (In-Cluster) lub przez K8S_API_URL."""

    SA_TOKEN_PATH = "/var/run/secrets/kubernetes.io/serviceaccount/token"
    SA_CA_PATH = "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt"

    def __init__(self, api_url: Optional[str] = None, token: Optional[str] = None):
        host = os.environ.get("KUBERNETES_SERVICE_HOST", "kubernetes.default.svc")
        port = os.environ.get("KUBERNETES_SERVICE_PORT", "443")
        self.api_url = (
            api_url
            or os.environ.get("K8S_API_URL")
            or f"https://{host}:{port}"
        ).rstrip("/")
        self._explicit_token = token or os.environ.get("K8S_BEARER_TOKEN")
        self.ssl_context = self._build_ssl_context()

    def _build_ssl_context(self) -> ssl.SSLContext:
        if os.environ.get("K8S_INSECURE_SKIP_VERIFY", "false").lower() == "true":
            ctx = ssl.create_default_context()
            ctx.check_hostname = False
            ctx.verify_mode = ssl.CERT_NONE
            return ctx
        if os.path.exists(self.SA_CA_PATH):
            return ssl.create_default_context(cafile=self.SA_CA_PATH)
        return ssl.create_default_context()

    def _get_token(self) -> str:
        if self._explicit_token:
            return self._explicit_token
        if os.path.exists(self.SA_TOKEN_PATH):
            with open(self.SA_TOKEN_PATH, "r", encoding="utf-8") as f:
                return f.read().strip()
        return ""

    def _request(
        self,
        method: str,
        path: str,
        body: Optional[dict[str, Any]] = None,
        content_type: str = "application/json",
    ) -> dict[str, Any]:
        url = f"{self.api_url}{path}"
        data = json.dumps(body).encode("utf-8") if body is not None else None
        req = urllib.request.Request(url, data=data, method=method)
        token = self._get_token()
        if token:
            req.add_header("Authorization", f"Bearer {token}")
        req.add_header("Accept", "application/json")
        if data is not None:
            req.add_header("Content-Type", content_type)

        with urllib.request.urlopen(req, context=self.ssl_context, timeout=15) as resp:
            return json.loads(resp.read().decode("utf-8"))

    def list_namespaces(self) -> list[str]:
        resp = self._request("GET", "/api/v1/namespaces")
        return [item["metadata"]["name"] for item in resp.get("items", [])]

    def list_workloads(self, namespace: str, kind_plural: str) -> list[dict[str, Any]]:
        path = f"/apis/apps/v1/namespaces/{namespace}/{kind_plural}"
        resp = self._request("GET", path)
        return resp.get("items", [])

    def list_pods(self, namespace: str) -> list[dict[str, Any]]:
        path = f"/api/v1/namespaces/{namespace}/pods"
        resp = self._request("GET", path)
        return resp.get("items", [])

    def get_secret(self, namespace: str, name: str) -> Optional[dict[str, Any]]:
        path = f"/api/v1/namespaces/{namespace}/secrets/{name}"
        try:
            return self._request("GET", path)
        except urllib.error.HTTPError as err:
            if err.code == 404:
                return None
            raise

    def extract_ghcr_creds_from_secret(
        self, namespace: str, secret_name: str
    ) -> Optional[tuple[str, str]]:
        """Odczytuje poświadczenia do ghcr.io z Secretu typu kubernetes.io/dockerconfigjson."""
        secret = self.get_secret(namespace, secret_name)
        if not secret:
            return None
        data = secret.get("data", {})
        raw_b64 = data.get(".dockerconfigjson")
        if not raw_b64:
            return None
        try:
            decoded = json.loads(base64.b64decode(raw_b64).decode("utf-8"))
            auths = decoded.get("auths", {})
            for registry_key, auth_entry in auths.items():
                if "ghcr.io" in registry_key:
                    if "username" in auth_entry and "password" in auth_entry:
                        return auth_entry["username"], auth_entry["password"]
                    if "auth" in auth_entry:
                        user_pass = base64.b64decode(auth_entry["auth"]).decode("utf-8")
                        if ":" in user_pass:
                            u, p = user_pass.split(":", 1)
                            return u, p
        except Exception as exc:
            logger.warning("Nie udało się zdekodować secretu %s/%s: %s", namespace, secret_name, exc)
        return None

    def rollout_restart(
        self,
        namespace: str,
        kind_plural: str,
        name: str,
    ) -> None:
        """
        Wykonuje strategic merge patch na zasobie (dokładny odpowiednik `kubectl rollout restart`).
        Aktualizuje wyłącznie `spec.template.metadata.annotations['kubectl.kubernetes.io/restartedAt']`,
        dzięki czemu ArgoCD nie oznacza aplikacji jako OutOfSync.
        """
        now_iso = dt.datetime.now(dt.timezone.utc).isoformat()
        patch_body: dict[str, Any] = {
            "spec": {
                "template": {
                    "metadata": {
                        "annotations": {
                            "kubectl.kubernetes.io/restartedAt": now_iso,
                        }
                    }
                }
            },
        }

        path = f"/apis/apps/v1/namespaces/{namespace}/{kind_plural}/{name}"
        self._request(
            "PATCH",
            path,
            body=patch_body,
            content_type="application/strategic-merge-patch+json",
        )


def extract_digest_from_image_id(image_id: str) -> Optional[str]:
    """Wyciąga `sha256:...` z pola containerStatuses[].imageID w Podzie."""
    if not image_id:
        return None
    if "sha256:" in image_id:
        idx = image_id.rfind("sha256:")
        digest = image_id[idx:].strip()
        parts = digest.split()
        return parts[0] if parts else None
    return None


class UpdaterService:
    """Główny serwis sprawdzający obrazy w GHCR i restartujący zasoby w Kubernetes / k3s."""

    def __init__(
        self,
        config: Config,
        ghcr_client: Optional[GHCRClient] = None,
        k8s_client: Optional[K8sClient] = None,
    ):
        self.config = config
        self.ghcr = ghcr_client or GHCRClient(
            default_username=config.ghcr_username,
            default_token=config.ghcr_token,
        )
        self.k8s = k8s_client or K8sClient()
        self.lock = threading.Lock()
        self.statuses: dict[str, WorkloadStatus] = {}
        # Pamięć ostatnio zrestartowanego digestu oraz czasu restartu dla klucza zasobu: (digest, timestamp)
        self._restarted_digests: dict[str, tuple[str, float]] = {}
        self.last_run_at: Optional[str] = None
        self.total_restarts: int = 0

    def _should_watch_workload(self, workload: dict[str, Any]) -> bool:
        annotations = workload.get("metadata", {}).get("annotations", {}) or {}
        enabled_val = annotations.get("ghcr-updater.io/enabled", "").lower()
        if enabled_val == "false":
            return False
        if self.config.require_annotation:
            return enabled_val == "true"
        return True

    def _find_running_digests_for_workload(
        self,
        workload: dict[str, Any],
        pods: list[dict[str, Any]],
        container_name: str,
    ) -> list[str]:
        """Zwraca listę digestów obrazów uruchomionych w aktywnych Podach należących do danego zasobu."""
        match_labels = (
            workload.get("spec", {})
            .get("selector", {})
            .get("matchLabels", {})
        )
        if not match_labels:
            return []

        digests: list[str] = []
        for pod in pods:
            pod_labels = pod.get("metadata", {}).get("labels", {}) or {}
            if all(pod_labels.get(k) == v for k, v in match_labels.items()):
                if pod.get("metadata", {}).get("deletionTimestamp"):
                    continue
                statuses = pod.get("status", {}).get("containerStatuses", []) or []
                for st in statuses:
                    if st.get("name") == container_name:
                        digest = extract_digest_from_image_id(st.get("imageID", ""))
                        if digest and digest not in digests:
                            digests.append(digest)
        return digests

    def _resolve_credentials(
        self, namespace: str, pod_spec: dict[str, Any]
    ) -> tuple[Optional[str], Optional[str]]:
        if self.config.ghcr_token:
            return self.config.ghcr_username, self.config.ghcr_token

        if self.config.use_image_pull_secrets:
            for sec_ref in pod_spec.get("imagePullSecrets", []) or []:
                sec_name = sec_ref.get("name")
                if sec_name:
                    creds = self.k8s.extract_ghcr_creds_from_secret(namespace, sec_name)
                    if creds:
                        return creds
        return None, None

    def _send_webhook(self, message: str) -> None:
        if not self.config.webhook_url:
            return
        try:
            body = json.dumps({"content": message, "text": message}).encode("utf-8")
            req = urllib.request.Request(
                self.config.webhook_url,
                data=body,
                method="POST",
                headers={"Content-Type": "application/json"},
            )
            urllib.request.urlopen(req, timeout=10).close()
        except Exception as exc:
            logger.warning("Błąd wysyłania powiadomienia webhook: %s", exc)

    def check_once(self) -> list[WorkloadStatus]:
        """Wykonuje pojedynczy cykl sprawdzenia wszystkich skonfigurowanych przestrzeni nazw i zasobów."""
        with self.lock:
            now_ts = time.time()
            now_iso = dt.datetime.now(dt.timezone.utc).isoformat()
            self.last_run_at = now_iso
            results: list[WorkloadStatus] = []

            namespaces = self.config.namespaces
            if "*" in namespaces or "all" in [n.lower() for n in namespaces]:
                namespaces = [
                    ns
                    for ns in self.k8s.list_namespaces()
                    if ns not in self.config.exclude_namespaces
                ]

            for namespace in namespaces:
                try:
                    pods = self.k8s.list_pods(namespace)
                except Exception as exc:
                    logger.error("Nie udało się pobrać listy Podów w namespace %s: %s", namespace, exc)
                    continue

                for kind_plural in self.config.watch_kinds:
                    try:
                        workloads = self.k8s.list_workloads(namespace, kind_plural)
                    except Exception as exc:
                        logger.error(
                            "Nie udało się pobrać %s w namespace %s: %s",
                            kind_plural,
                            namespace,
                            exc,
                        )
                        continue

                    for wl in workloads:
                        if not self._should_watch_workload(wl):
                            continue

                        wl_name = wl.get("metadata", {}).get("name", "unknown")
                        # Pomijamy samego siebie
                        if wl_name == "ghcr-k8s-updater" and annotations_skip_self(wl):
                            continue

                        annotations = wl.get("metadata", {}).get("annotations", {}) or {}
                        pod_spec = (
                            wl.get("spec", {})
                            .get("template", {})
                            .get("spec", {})
                        )
                        containers = pod_spec.get("containers", []) or []

                        for container in containers:
                            c_name = container.get("name", "")
                            image_str = container.get("image", "")
                            img_ref = ImageRef.parse(image_str)

                            if not img_ref or img_ref.registry != "ghcr.io":
                                continue
                            if (
                                self.config.ghcr_owner_filter
                                and img_ref.owner != self.config.ghcr_owner_filter
                            ):
                                continue
                            if self.config.only_latest_tag and img_ref.tag != "latest":
                                if annotations.get("ghcr-updater.io/allow-any-tag", "").lower() != "true":
                                    continue

                            key = f"{namespace}/{kind_plural}/{wl_name}/{c_name}"
                            prev_status = self.statuses.get(key)
                            status_entry = WorkloadStatus(
                                kind=kind_plural,
                                namespace=namespace,
                                name=wl_name,
                                container_name=c_name,
                                image=image_str,
                                last_checked=now_iso,
                                last_restarted=prev_status.last_restarted if prev_status else None,
                            )

                            try:
                                username, pat_token = self._resolve_credentials(namespace, pod_spec)
                                remote_digest = self.ghcr.get_remote_digest(
                                    img_ref, username=username, pat_token=pat_token
                                )
                                running_digests = self._find_running_digests_for_workload(
                                    wl, pods, c_name
                                )
                                running_digest = running_digests[0] if running_digests else None

                                status_entry.remote_digest = remote_digest
                                status_entry.running_digest = running_digest

                                if not running_digests:
                                    logger.info(
                                        "[%s] Brak aktywnego Poda z imageID (np. trwa start lub skalowanie do 0), pomijam.",
                                        key,
                                    )
                                    status_entry.status = "up_to_date"
                                elif all(d == remote_digest for d in running_digests):
                                    status_entry.running_digest = remote_digest
                                    status_entry.status = "up_to_date"
                                    logger.debug("[%s] Obraz aktualny (%s)", key, remote_digest)
                                else:
                                    # Sprawdzamy czy nie zrestartowaliśmy już tego zasobu pod ten konkretny remote_digest
                                    last_restart_info = self._restarted_digests.get(key)
                                    if (
                                        last_restart_info
                                        and last_restart_info[0] == remote_digest
                                        and (now_ts - last_restart_info[1]) < self.config.restart_cooldown_seconds
                                    ):
                                        logger.info(
                                            "[%s] Rollout dla digestu %s już trwa (cooldown %ds), czekam na start nowego Poda.",
                                            key,
                                            remote_digest[:19],
                                            self.config.restart_cooldown_seconds,
                                        )
                                        status_entry.status = "rollout_in_progress"
                                    else:
                                        logger.info(
                                            "[%s] Wykryto nowy obraz %s! Uruchomiony digest: %s -> Nowy digest GHCR: %s. Restartuję %s/%s...",
                                            key,
                                            image_str,
                                            running_digest,
                                            remote_digest,
                                            kind_plural,
                                            wl_name,
                                        )
                                        if not self.config.dry_run:
                                            self.k8s.rollout_restart(
                                                namespace=namespace,
                                                kind_plural=kind_plural,
                                                name=wl_name,
                                            )
                                        self._restarted_digests[key] = (remote_digest, now_ts)
                                        status_entry.last_restarted = now_iso
                                        status_entry.status = "updated"
                                        self.total_restarts += 1
                                        self._send_webhook(
                                            f"🔄 **GHCR Updater**: Zrestartowano `{namespace}/{kind_plural}/{wl_name}` "
                                            f"(nowy obraz `{image_str}`: `{running_digest[:19] if running_digest else 'brak'}` -> `{remote_digest[:19]}`)"
                                        )

                            except Exception as exc:
                                logger.error("[%s] Błąd podczas sprawdzania obrazu %s: %s", key, image_str, exc)
                                status_entry.status = "error"
                                status_entry.error_message = str(exc)

                            self.statuses[key] = status_entry
                            results.append(status_entry)

            return results


def annotations_skip_self(workload: dict[str, Any]) -> bool:
    annotations = workload.get("metadata", {}).get("annotations", {}) or {}
    return annotations.get("ghcr-updater.io/self-update", "false").lower() != "true"


DASHBOARD_HTML = """<!DOCTYPE html>
<html lang="pl">
<head>
  <meta charset="UTF-8" />
  <title>GHCR K8s Updater – Status</title>
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <style>
    :root {
      --bg: #0f172a;
      --card: #1e293b;
      --border: #334155;
      --text: #f8fafc;
      --muted: #94a3b8;
      --accent: #38bdf8;
      --green: #22c55e;
      --amber: #f59e0b;
      --red: #ef4444;
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      padding: 24px;
      font-family: ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
      background: var(--bg);
      color: var(--text);
    }
    .container { max-width: 1100px; margin: 0 auto; }
    header {
      display: flex;
      justify-content: space-between;
      align-items: center;
      flex-wrap: wrap;
      gap: 16px;
      margin-bottom: 24px;
      padding-bottom: 16px;
      border-bottom: 1px solid var(--border);
    }
    h1 { margin: 0; font-size: 1.4rem; }
    .meta { color: var(--muted); font-size: 0.875rem; margin-top: 4px; }
    button {
      background: var(--accent);
      color: #0f172a;
      border: none;
      border-radius: 6px;
      padding: 10px 16px;
      font-weight: 600;
      cursor: pointer;
      transition: opacity 0.15s;
    }
    button:hover { opacity: 0.9; }
    table {
      width: 100%;
      border-collapse: collapse;
      background: var(--card);
      border-radius: 8px;
      overflow: hidden;
      border: 1px solid var(--border);
    }
    th, td {
      text-align: left;
      padding: 12px 14px;
      border-bottom: 1px solid var(--border);
      font-size: 0.875rem;
    }
    th { color: var(--muted); font-weight: 600; background: rgba(15, 23, 42, 0.4); }
    code {
      font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace;
      font-size: 0.8rem;
      background: rgba(15, 23, 42, 0.6);
      padding: 2px 6px;
      border-radius: 4px;
    }
    .badge {
      display: inline-block;
      padding: 3px 8px;
      border-radius: 999px;
      font-size: 0.75rem;
      font-weight: 600;
    }
    .badge.up_to_date { background: rgba(34, 197, 94, 0.15); color: var(--green); }
    .badge.updated, .badge.rollout_in_progress { background: rgba(245, 158, 11, 0.15); color: var(--amber); }
    .badge.error { background: rgba(239, 68, 68, 0.15); color: var(--red); }
  </style>
</head>
<body>
  <div class="container">
    <header>
      <div>
        <h1>GHCR K8s Updater</h1>
        <div class="meta" id="summary">Ładowanie statusu...</div>
      </div>
      <button id="checkBtn" onclick="triggerCheck()">Sprawdź teraz (GHCR)</button>
    </header>
    <table>
      <thead>
        <tr>
          <th>Zasób</th>
          <th>Obraz</th>
          <th>Uruchomiony Digest (Pod)</th>
          <th>Zdalny Digest (GHCR)</th>
          <th>Status</th>
          <th>Ostatni restart</th>
        </tr>
      </thead>
      <tbody id="rows">
        <tr><td colspan="6" style="color:var(--muted)">Brak danych...</td></tr>
      </tbody>
    </table>
  </div>
  <script>
    function shortDigest(d) {
      if (!d) return "-";
      return d.length > 23 ? d.slice(0, 23) + "…" : d;
    }
    async function loadStatus() {
      const res = await fetch("/api/status");
      const data = await res.json();
      document.getElementById("summary").textContent =
        `Ostatnie sprawdzenie: ${data.last_run_at || "nigdy"} | Interwał: ${data.poll_interval_seconds}s | Wykonane restarty: ${data.total_restarts}`;
      const tbody = document.getElementById("rows");
      if (!data.workloads || data.workloads.length === 0) {
        tbody.innerHTML = '<tr><td colspan="6" style="color:var(--muted)">Nie znaleziono monitorowanych zasobów z obrazami ghcr.io</td></tr>';
        return;
      }
      tbody.innerHTML = data.workloads.map(w => `
        <tr>
          <td><strong>${w.namespace}</strong> / ${w.kind} / <code>${w.name}</code> <span style="color:var(--muted)">(${w.container_name})</span></td>
          <td><code>${w.image}</code></td>
          <td title="${w.running_digest || ''}"><code>${shortDigest(w.running_digest)}</code></td>
          <td title="${w.remote_digest || ''}"><code>${shortDigest(w.remote_digest)}</code></td>
          <td>
            <span class="badge ${w.status}">${w.status}</span>
            ${w.error_message ? `<div style="color:var(--red);font-size:0.75rem;margin-top:4px">${w.error_message}</div>` : ""}
          </td>
          <td>${w.last_restarted || "-"}</td>
        </tr>
      `).join("");
    }
    async function triggerCheck() {
      const btn = document.getElementById("checkBtn");
      btn.disabled = true;
      btn.textContent = "Sprawdzanie...";
      try {
        await fetch("/api/check", { method: "POST" });
        await loadStatus();
      } finally {
        btn.disabled = false;
        btn.textContent = "Sprawdź teraz (GHCR)";
      }
    }
    loadStatus();
    setInterval(loadStatus, 15000);
  </script>
</body>
</html>
"""


def create_http_handler(service: UpdaterService):
    class StatusHandler(BaseHTTPRequestHandler):
        def do_GET(self):
            if self.path == "/healthz":
                self._send_json(200, {"status": "ok", "last_run_at": service.last_run_at})
            elif self.path == "/api/status":
                self._send_json(
                    200,
                    {
                        "last_run_at": service.last_run_at,
                        "poll_interval_seconds": service.config.poll_interval_seconds,
                        "total_restarts": service.total_restarts,
                        "workloads": [asdict(v) for v in service.statuses.values()],
                    },
                )
            elif self.path in ("/", "/index.html"):
                body = DASHBOARD_HTML.encode("utf-8")
                self.send_response(200)
                self.send_header("Content-Type", "text/html; charset=utf-8")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
            else:
                self._send_json(404, {"error": "Not found"})

        def do_POST(self):
            if self.path == "/api/check":
                results = service.check_once()
                self._send_json(
                    200,
                    {
                        "checked": len(results),
                        "workloads": [asdict(r) for r in results],
                    },
                )
            else:
                self._send_json(404, {"error": "Not found"})

        def _send_json(self, code: int, payload: dict[str, Any]):
            body = json.dumps(payload).encode("utf-8")
            self.send_response(code)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, format: str, *args: Any) -> None:
            if self.path != "/healthz":
                logger.debug("HTTP %s - %s", self.address_string(), format % args)

    return StatusHandler


def main() -> None:
    config = Config()
    service = UpdaterService(config)

    server = ThreadingHTTPServer(("0.0.0.0", config.http_port), create_http_handler(service))
    http_thread = threading.Thread(target=server.serve_forever, daemon=True)
    http_thread.start()
    logger.info(
        "Uruchomiono GHCR K8s Updater (HTTP port=%d, interwał=%ds, namespaces=%s)",
        config.http_port,
        config.poll_interval_seconds,
        ",".join(config.namespaces),
    )

    while True:
        try:
            service.check_once()
        except Exception as exc:
            logger.exception("Nieoczekiwany błąd w pętli głównej: %s", exc)
        time.sleep(config.poll_interval_seconds)


if __name__ == "__main__":
    main()
