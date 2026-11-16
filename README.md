# GHCR K8s Updater (`ghcr-k8s-updater`)

Lekka aplikacja dla **Kubernetes / k3s** oraz **ArgoCD**, która cyklicznie sprawdza na **GitHub Container Registry (`ghcr.io`)** czy pojawił się nowy digest (`sha256:...`) dla obrazów `:latest` z Twojego konta GitHub (`eNesPL`) i automatycznie wykonuje bezprzestojowy rolling restart (`kubectl rollout restart`).

## Najważniejsze cechy
- **W 100% kompatybilna z ArgoCD**: podczas aktualizacji aplikacja modyfikuje wyłącznie `spec.template.metadata.annotations["kubectl.kubernetes.io/restartedAt"]` (identycznie jak `kubectl rollout restart`), dzięki czemu ArgoCD nie oznacza Twoich aplikacji jako `OutOfSync`.
- **Obsługa prywatnych i publicznych obrazów GHCR**:
  - autoryzacja przez token PAT (`GHCR_TOKEN` z uprawnieniem `read:packages`) **lub**
  - automatyczne odczytywanie poświadczeń z `imagePullSecrets` (`kubernetes.io/dockerconfigjson`) przypiętych do Podów w klastrze.
- **Ochrona przed pętlą restartów (Cooldown)**: po zleceniu restartu aplikacja czeka aż nowy Pod pobierze obraz i wystartuje z nowym digestem (`RESTART_COOLDOWN_SECONDS=180`).
- **Wbudowany panel webowy i API (`:8080`)**:
  - `/` – czytelny dashboard ze statusem wszystkich monitorowanych aplikacji, aktualnym digestem w Podzie vs GHCR oraz przyciskiem **„Sprawdź teraz”**
  - `/api/status` – pełny status JSON
  - `/api/check` (`POST`) – natychmiastowe wymuszenie sprawdzenia GHCR
  - `/healthz` – endpoint dla `livenessProbe` / `readinessProbe`
- **Zero zewnętrznych zależności**: czysty Python 3.12 (biblioteka standardowa), obraz Docker o minimalnym zużyciu RAM (~20 MB).

---

## Struktura projektu

- `updater.py` – główny kod aplikacji (klient GHCR API v2, klient Kubernetes In-Cluster API, serwer HTTP + dashboard)
- `Dockerfile` – lekki obraz kontenera (`python:3.12-slim`, uruchamiany jako non-root `10001`)
- `.github/workflows/docker-build-push.yml` – workflow GitHub Actions budujący i wypychający obraz `ghcr.io/enespl/ghcr-k8s-updater:latest`
- `k8s/` – manifesty Kustomize dla klastra k3s / Kubernetes (`namespace.yaml`, `rbac.yaml`, `deployment.yaml`, `secret.yaml`, `kustomization.yaml`)
- `argocd/application.yaml` – gotowa definicja `Application` do dodania w repozytorium `eNesPL/ArgoCD`

---

## Konfiguracja (zmienne środowiskowe)

| Zmienna | Domyślna wartość | Opis |
|---|---|---|
| `GHCR_USERNAME` | `eNesPL` | Nazwa użytkownika GitHub |
| `GHCR_TOKEN` | *(puste)* | Token GitHub PAT (`read:packages`) do prywatnych obrazów. Jeśli puste, updater użyje `imagePullSecrets` z Podów |
| `GHCR_OWNER_FILTER` | `enespl` | Filtr właściciela obrazów na `ghcr.io` (puste = wszystkie obrazy z `ghcr.io`) |
| `WATCH_NAMESPACES` | `*` | Lista przestrzeni nazw po przecinku lub `*` dla wszystkich |
| `EXCLUDE_NAMESPACES` | `kube-system,kube-public,kube-node-lease,argocd` | Przestrzenie nazw pomijane przy `WATCH_NAMESPACES=*` |
| `WATCH_KINDS` | `deployments,statefulsets,daemonsets` | Typy zasobów do monitorowania |
| `ONLY_LATEST_TAG` | `true` | Sprawdza tylko obrazy z tagiem `:latest` (lub zasoby z adnotacją `ghcr-updater.io/allow-any-tag: "true"`) |
| `REQUIRE_ANNOTATION` | `false` | Jeśli `true`, aktualizuje tylko zasoby z adnotacją `ghcr-updater.io/enabled: "true"` |
| `POLL_INTERVAL_SECONDS` | `60` | Częstotliwość odpytywania GHCR (w sekundach) |
| `RESTART_COOLDOWN_SECONDS` | `180` | Czas oczekiwania na start nowego Poda po zleceniu restartu |
| `WEBHOOK_URL` | *(puste)* | Opcjonalny URL webhooka (Discord / Slack) do powiadomień o restartach |

### Sterowanie per-Deployment przez adnotacje (opcjonalne)
- Wyłączenie aktualizacji dla wybranego Deploymentu:
  ```yaml
  metadata:
    annotations:
      ghcr-updater.io/enabled: "false"
  ```
- Wymuszenie sprawdzania tagu innego niż `:latest` (np. `:main` lub `:prod`):
  ```yaml
  metadata:
    annotations:
      ghcr-updater.io/allow-any-tag: "true"
  ```

---

## Wdrożenie przez ArgoCD

1. Utwórz w klastrze Secret z tokenem PAT (`read:packages`), jeśli nie chcesz trzymać go w Git:
   ```bash
   kubectl create namespace ghcr-updater
   kubectl -n ghcr-updater create secret generic ghcr-updater-secret \
     --from-literal=GHCR_USERNAME=eNesPL \
     --from-literal=GHCR_TOKEN=ghp_TWOJ_TOKEN_PAT
   ```
   *(Uwaga: jeśli Twoje Deploymenty w k3s mają już skonfigurowane `imagePullSecrets` do `ghcr.io`, updater automatycznie odczyta z nich token — wtedy `ghcr-updater-secret` nie jest nawet wymagany!)*

2. Dodaj plik `argocd/application.yaml` do swojego repozytorium `eNesPL/ArgoCD` (lub zaaplikuj go bezpośrednio przez `kubectl apply -f argocd/application.yaml`).
