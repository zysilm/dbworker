"""Send rendered notifications through the upstream SMTP implementation."""


def initialize():
    from examples.posthog_dbworker.bootstrap import initialize as bootstrap
    bootstrap()


def execute(payload: dict) -> dict:
    from posthog.email import _send_email_now
    from posthog.models.messaging import MessagingRecord, get_email_hashes

    if payload.get("use_http"):
        raise ValueError("This suite requires the configured local SMTP sink")
    _send_email_now(**payload)
    records = MessagingRecord.objects.filter(campaign_key=payload["campaign_key"],
                                             email_hash__in=[value for item in payload["to"]
                                                             for value in get_email_hashes(item["raw_email"])])
    if records.count() != len(payload["to"]) or records.filter(sent_at__isnull=True).exists():
        raise RuntimeError("PostHog did not record every intended delivery")
    return {"accepted_recipients": sorted(item["raw_email"] for item in payload["to"])}
