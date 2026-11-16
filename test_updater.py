import unittest
from unittest.mock import MagicMock

from updater import (
    Config,
    ImageRef,
    UpdaterService,
    extract_digest_from_image_id,
)


class TestImageRef(unittest.TestCase):
    def test_parse_ghcr_latest(self):
        ref = ImageRef.parse("ghcr.io/eNesPL/mtgspot-scraper:latest")
        self.assertIsNotNone(ref)
        assert ref is not None
        self.assertEqual(ref.registry, "ghcr.io")
        self.assertEqual(ref.repository, "eNesPL/mtgspot-scraper")
        self.assertEqual(ref.owner, "enespl")
        self.assertEqual(ref.tag, "latest")

    def test_parse_implicit_latest(self):
        ref = ImageRef.parse("ghcr.io/enespl/bramaapi")
        self.assertIsNotNone(ref)
        assert ref is not None
        self.assertEqual(ref.tag, "latest")

    def test_extract_digest(self):
        img_id = "ghcr.io/enespl/app@sha256:1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef"
        self.assertEqual(
            extract_digest_from_image_id(img_id),
            "sha256:1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef",
        )


class TestUpdaterService(unittest.TestCase):
    def test_restarts_when_digest_changes_and_respects_cooldown(self):
        cfg = Config(
            ghcr_username="eNesPL",
            ghcr_token="ghp_test",
            ghcr_owner_filter="enespl",
            namespaces=["default"],
            watch_kinds=["deployments"],
            restart_cooldown_seconds=180,
        )
        mock_ghcr = MagicMock()
        mock_k8s = MagicMock()

        old_digest = "sha256:aaaa1111"
        new_digest = "sha256:bbbb2222"
        mock_ghcr.get_remote_digest.return_value = new_digest

        mock_k8s.list_workloads.return_value = [
            {
                "metadata": {"name": "my-app", "annotations": {}},
                "spec": {
                    "selector": {"matchLabels": {"app": "my-app"}},
                    "template": {
                        "spec": {
                            "containers": [
                                {
                                    "name": "app",
                                    "image": "ghcr.io/eNesPL/my-app:latest",
                                }
                            ]
                        }
                    },
                },
            }
        ]
        mock_k8s.list_pods.return_value = [
            {
                "metadata": {"name": "my-app-xyz", "labels": {"app": "my-app"}},
                "status": {
                    "containerStatuses": [
                        {
                            "name": "app",
                            "imageID": f"ghcr.io/enespl/my-app@{old_digest}",
                        }
                    ]
                },
            }
        ]

        service = UpdaterService(cfg, ghcr_client=mock_ghcr, k8s_client=mock_k8s)

        # Pierwsze sprawdzenie -> wykrywa nowy digest i wykonuje rollout_restart
        results = service.check_once()
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0].status, "updated")
        mock_k8s.rollout_restart.assert_called_once_with(
            namespace="default",
            kind_plural="deployments",
            name="my-app",
        )

        # Drugie sprawdzenie chwilę później (zanim Pod się przeładuje) -> cooldown zapobiega pętli restartów
        results_2 = service.check_once()
        self.assertEqual(results_2[0].status, "rollout_in_progress")
        self.assertEqual(mock_k8s.rollout_restart.call_count, 1)

        # Gdy nowy Pod już wystartuje z nowym digestem -> status zmienia się na up_to_date
        mock_k8s.list_pods.return_value[0]["status"]["containerStatuses"][0][
            "imageID"
        ] = f"ghcr.io/enespl/my-app@{new_digest}"
        results_3 = service.check_once()
        self.assertEqual(results_3[0].status, "up_to_date")


if __name__ == "__main__":
    unittest.main()
